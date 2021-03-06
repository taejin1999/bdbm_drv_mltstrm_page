/*
The MIT License (MIT)

Copyright (c) 2014-2015 CSAIL, MIT

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

#if defined (KERNEL_MODE)
#include <linux/module.h>
#include <linux/slab.h>
#include <linux/log2.h>
//tjkim
#include  <linux/workqueue.h>
#include  <linux/slab.h>

#elif defined (USER_MODE)
#include <stdio.h>
#include <stdint.h>
#include "uilog.h"
#include "upage.h"

#else
#error Invalid Platform (KERNEL_MODE or USER_MODE)
#endif

#include "bdbm_drv.h"
#include "params.h"
#include "debug.h"
#include "utime.h"
#include "ufile.h"
#include "umemory.h"
#include "hlm_reqs_pool.h"

#include "algo/abm.h"
#include "algo/page_ftl.h"
#include "pmu.h"
#include "autostream.h"

bdbm_drv_info_t* this;

/* FTL interface */
bdbm_ftl_inf_t _ftl_page_ftl = {
	.ptr_private = NULL,
	.create = bdbm_page_ftl_create,
	.destroy = bdbm_page_ftl_destroy,
	.get_free_ppa = bdbm_page_ftl_get_free_ppa,
	.get_ppa = bdbm_page_ftl_get_ppa,
	.map_lpa_to_ppa = bdbm_page_ftl_map_lpa_to_ppa,
	.invalidate_lpa = bdbm_page_ftl_invalidate_lpa,
	.do_gc = bdbm_page_ftl_do_gc,
	.is_gc_needed = bdbm_page_ftl_is_gc_needed,
	.scan_badblocks = bdbm_page_badblock_scan,
	/*.load = bdbm_page_ftl_load,*/
	/*.store = bdbm_page_ftl_store,*/
	/*.get_segno = NULL,*/
};


/* data structures for block-level FTL */
enum BDBM_PFTL_PAGE_STATUS {
	PFTL_PAGE_NOT_ALLOCATED = 0,
	PFTL_PAGE_VALID,
	PFTL_PAGE_INVALID,
	PFTL_PAGE_INVALID_ADDR = -1ULL,
};

typedef struct {
	uint8_t status; /* BDBM_PFTL_PAGE_STATUS */
	bdbm_phyaddr_t phyaddr; /* physical location */
	uint8_t sp_off;
	int64_t writtentime;
	uint8_t sID;
	uint8_t type;
	uint8_t pcid;
	uint8_t asid;
} bdbm_page_mapping_entry_t;

typedef struct {
	bdbm_abm_info_t* bai;
	bdbm_page_mapping_entry_t* ptr_mapping_table;
	bdbm_spinlock_t ftl_lock;
	uint64_t nr_punits;
	uint64_t nr_punits_pages;

	/* for the management of active blocks */
	uint64_t curr_puid[BDBM_DEV_NR_STREAM];
	uint64_t curr_page_ofs[BDBM_DEV_NR_STREAM];
	bdbm_abm_block_t** ac_bab;

	/* reserved for gc (reused whenever gc is invoked) */
	bdbm_abm_block_t** gc_bab;
	int8_t* sID_for_llm_reqs;
	bdbm_hlm_req_gc_t gc_hlm;
	bdbm_hlm_req_gc_t gc_hlm_w;

	/* for bad-block scanning */
	bdbm_sema_t badblk;
} bdbm_page_ftl_private_t;


bdbm_page_mapping_entry_t* __bdbm_page_ftl_create_mapping_table (
	bdbm_device_params_t* np)
{
	bdbm_page_mapping_entry_t* me;
	uint64_t loop;

	/* create a page-level mapping table */
	if ((me = (bdbm_page_mapping_entry_t*)bdbm_zmalloc 
			(sizeof (bdbm_page_mapping_entry_t) * np->nr_subpages_per_ssd)) == NULL) {
		return NULL;
	}

	/* initialize a page-level mapping table */
	for (loop = 0; loop < np->nr_subpages_per_ssd; loop++) {
		me[loop].status = PFTL_PAGE_NOT_ALLOCATED;
		me[loop].phyaddr.channel_no = PFTL_PAGE_INVALID_ADDR;
		me[loop].phyaddr.chip_no = PFTL_PAGE_INVALID_ADDR;
		me[loop].phyaddr.block_no = PFTL_PAGE_INVALID_ADDR;
		me[loop].phyaddr.page_no = PFTL_PAGE_INVALID_ADDR;
		me[loop].sp_off = -1;
		me[loop].sID = -1;
		me[loop].type = -1;
		me[loop].pcid = -1;
		me[loop].asid = -1;
		me[loop].writtentime = -1;
	}

	/* return a set of mapping entries */
	return me;
}


void __bdbm_page_ftl_destroy_mapping_table (
	bdbm_page_mapping_entry_t* me)
{
	if (me == NULL)
		return;
	bdbm_free (me);
}

// re-allocate a free block for all the parallel units
uint32_t __bdbm_page_ftl_get_active_blocks (
	bdbm_device_params_t* np,
	bdbm_abm_info_t* bai,
	bdbm_abm_block_t** bab,
	int8_t streamID)
{
	uint64_t i, j;

	bdbm_bug_on(streamID < 0 || streamID >= BDBM_DEV_NR_STREAM);

	bab += streamID;
	/* get a set of free blocks for active blocks */
	for (i = 0; i < np->nr_channels; i++) {
		for (j = 0; j < np->nr_chips_per_channel; j++) {
			/* prepare & commit free blocks */
			//bdbm_msg("get_active_blk: %lld, %lld, %d", i, j, streamID);
			if ((*bab = bdbm_abm_get_free_block_prepare (bai, i, j))) {
				bdbm_abm_get_free_block_commit (bai, *bab);
				(*bab)->streamID = streamID;
				//bdbm_msg("active_blk (%lld, %lld): %lld", i, j, (*bab)->block_no);
				/*bdbm_msg ("active blk = %p", *bab);*/
				bab += BDBM_DEV_NR_STREAM;
			} else {
				bdbm_error ("bdbm_abm_get_free_block_prepare failed");
				return 1;
			}
		}
	}

	return 0;
}

uint64_t g_nr_punits;
bdbm_abm_block_t** __bdbm_page_ftl_create_active_blocks (
	bdbm_device_params_t* np,
	bdbm_abm_info_t* bai)
{
	uint64_t nr_punits;
	bdbm_abm_block_t** bab = NULL;
	uint8_t i;

	nr_punits = np->nr_chips_per_channel * np->nr_channels * BDBM_DEV_NR_STREAM;
	g_nr_punits = nr_punits;

	/* create a set of active blocks */
	if ((bab = (bdbm_abm_block_t**)bdbm_zmalloc 
			(sizeof (bdbm_abm_block_t*) * nr_punits)) == NULL) {
		bdbm_error ("bdbm_zmalloc failed");
		goto fail;
	}

	/* get a set of free blocks for active blocks */
	for(i = 0; i < BDBM_DEV_NR_STREAM; i++) {
		if (__bdbm_page_ftl_get_active_blocks (np, bai, bab, i) != 0) {
			bdbm_error ("__bdbm_page_ftl_get_active_blocks failed");
			goto fail;
		}
	}

	return bab;

fail:
	if (bab)
		bdbm_free (bab);
	return NULL;
}

void __bdbm_page_ftl_destroy_active_blocks (
	bdbm_abm_block_t** bab)
{
	if (bab == NULL)
		return;

	/* TODO: it might be required to save the status of active blocks 
	 * in order to support rebooting */
	bdbm_free (bab);

}

bdbm_file_t fp_asID[BDBM_DEV_NR_STREAM];
uint64_t fp_asID_pos[BDBM_DEV_NR_STREAM] = {0};
bdbm_file_t fp_pcID[BDBM_DEV_NR_STREAM];
uint64_t fp_pcID_pos[BDBM_DEV_NR_STREAM] = {0};
bdbm_file_t fp_type[BDBM_DEV_NR_STREAM];
uint64_t fp_type_pos[BDBM_DEV_NR_STREAM] = {0};
bdbm_file_t fp_all;
uint64_t fp_all_pos = 0;
bdbm_file_t fp_gc;
uint64_t fp_gc_pos = 0;
bdbm_file_t fp_waf;

static void pc_work_handler(struct work_struct *w);
static struct workqueue_struct *wq = 0;
static struct workqueue_struct *wq_gc = 0;

typedef struct {
	struct work_struct my_work;
	uint64_t lifetime;
	uint64_t lba;
	uint64_t writtentime;
	uint64_t invalidtime;
	uint8_t sID;
	uint8_t type;
	uint8_t pcid;
	uint8_t asid;
	uint8_t inv_type;
}my_work_t;

my_work_t *work;

typedef struct {
	struct work_struct my_work;
	uint64_t read_pages;
	uint64_t written_pages;
	uint64_t erased_blocks;
	uint8_t   num_victim_blocks[BDBM_DEV_NR_STREAM];
	uint32_t  num_valid_page_read[BDBM_DEV_NR_STREAM];
	uint32_t  num_valid_page_write[BDBM_DEV_NR_STREAM];
}my_gc_t;
my_gc_t *work_gc;

#define LIFETIME_BUFFER_SIZE 256
char buffer[LIFETIME_BUFFER_SIZE] = {0};
static void pc_work_handler(struct work_struct *w) {
	my_work_t *temp;
	uint64_t lifetime;
	uint64_t lba;
	uint64_t writtentime;
	uint64_t invalidtime;
	uint8_t sID;
	uint8_t type;
	uint8_t pcid;
	uint8_t asid;
	uint8_t inv_type;

	temp = container_of(w, my_work_t, my_work);
	lifetime = temp->lifetime;
	sID = temp->sID;
	type = temp->type;
	lba = temp->lba;
	writtentime = temp->writtentime;
	invalidtime = temp->invalidtime;
	inv_type = temp->inv_type;
	pcid = temp->pcid;
	asid = temp->asid;

	if(type != 0 && inv_type == 3) {
		sprintf(buffer, "%lld %lld %lld %lld %d %d\n", lba, lifetime, writtentime, invalidtime, inv_type, type);
		fp_type_pos[type] += bdbm_fwrite(fp_type[type], fp_type_pos[type], buffer, strlen(buffer));
		fp_asID_pos[asid] += bdbm_fwrite(fp_asID[asid], fp_asID_pos[asid], buffer, strlen(buffer));
		fp_pcID_pos[pcid] += bdbm_fwrite(fp_pcID[pcid], fp_pcID_pos[pcid], buffer, strlen(buffer));
		sprintf(buffer, "%lld %lld %lld %lld %d %d\n", lba, lifetime, writtentime, invalidtime, inv_type, type);
		fp_all_pos += bdbm_fwrite(fp_all, fp_all_pos, buffer, strlen(buffer));
		memset(buffer, 0x00, LIFETIME_BUFFER_SIZE);
	}
}

#define LIFETIME_BUFFER_SIZE 256
char buffer_gc[LIFETIME_BUFFER_SIZE] = {0};
char buffer_gc_format[LIFETIME_BUFFER_SIZE] = {0};
static void gc_work_handler(struct work_struct *w) {
	my_gc_t *temp;
	uint64_t read_pages;
	uint64_t written_pages;
	uint64_t erased_blocks;
	uint8_t *num_victim_blocks;
	uint32_t *num_valid_page_read;
	uint32_t *num_valid_page_write;
	int32_t i;

	temp = container_of(w, my_gc_t, my_work);
	read_pages = temp->read_pages;
	written_pages = temp->written_pages;
	erased_blocks = temp->erased_blocks;
	num_victim_blocks = temp->num_victim_blocks;
	num_valid_page_read = temp->num_valid_page_read;
	num_valid_page_write = temp->num_valid_page_write;

	if(erased_blocks < BDBM_DEV_NR_STREAM+1) {// hot line for writing waf
		//erased_blocks: tech_type, written_pages: total_writes, read_pages: waf
		sprintf(buffer_gc, "%lld %lld %lld\n", erased_blocks, written_pages, read_pages);
		bdbm_fwrite(fp_waf, 0, buffer_gc, strlen(buffer_gc));
		return;
	}
	
	sprintf(buffer_gc, "%lld %lld %lld ", read_pages, written_pages, erased_blocks);
	for(i = 0; i < BDBM_DEV_NR_STREAM; i++) {
		sprintf(buffer_gc_format, "%d:%d:%d:%d, ", i, num_victim_blocks[i], num_valid_page_read[i], num_valid_page_write[i]);
		strcat(buffer_gc, buffer_gc_format);
	}
	strcat(buffer_gc, "\n");
	fp_gc_pos += bdbm_fwrite(fp_gc, fp_gc_pos, buffer_gc, strlen(buffer_gc));
	memset(buffer_gc, 0x00, LIFETIME_BUFFER_SIZE);
}

uint32_t bdbm_page_ftl_create (bdbm_drv_info_t* bdi)
{
	uint32_t i;
	bdbm_page_ftl_private_t* p = NULL;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);

	/* create a private data structure */
	if ((p = (bdbm_page_ftl_private_t*)bdbm_zmalloc 
			(sizeof (bdbm_page_ftl_private_t))) == NULL) {
		bdbm_error ("bdbm_malloc failed");
		return 1;
	}
	for( i = 0; i < BDBM_DEV_NR_STREAM; i++){
		p->curr_puid[i] = 0;
		p->curr_page_ofs[i] = 0;
	}
	p->nr_punits = np->nr_chips_per_channel * np->nr_channels;
	p->nr_punits_pages = p->nr_punits * np->nr_pages_per_block;
	bdbm_spin_lock_init (&p->ftl_lock);
	_ftl_page_ftl.ptr_private = (void*)p;

	/* create 'bdbm_abm_info' with pst */
	if ((p->bai = bdbm_abm_create (np, 1)) == NULL) {
		bdbm_error ("bdbm_abm_create failed");
		bdbm_page_ftl_destroy (bdi);
		return 1;
	}

	/* create a mapping table */
	if ((p->ptr_mapping_table = __bdbm_page_ftl_create_mapping_table (np)) == NULL) {
		bdbm_error ("__bdbm_page_ftl_create_mapping_table failed");
		bdbm_page_ftl_destroy (bdi);
		return 1;
	}

	/* allocate active blocks */
	if ((p->ac_bab = __bdbm_page_ftl_create_active_blocks (np, p->bai)) == NULL) {
		bdbm_error ("__bdbm_page_ftl_create_active_blocks failed");
		bdbm_page_ftl_destroy (bdi);
		return 1;
	}

	/* allocate gc stuff */
	if ((p->gc_bab = (bdbm_abm_block_t**)bdbm_zmalloc 
			(sizeof (bdbm_abm_block_t*) * p->nr_punits)) == NULL) {
		bdbm_error ("bdbm_zmalloc failed");
		bdbm_page_ftl_destroy (bdi);
		return 1;
	}
	if ((p->sID_for_llm_reqs = (int8_t*)bdbm_zmalloc 
			(sizeof (int8_t) * p->nr_punits * np->nr_pages_per_block)) == NULL) {
		bdbm_error ("bdbm_zmalloc failed");
		bdbm_page_ftl_destroy (bdi);
		return 1;
	}

	if ((p->gc_hlm.llm_reqs = (bdbm_llm_req_t*)bdbm_zmalloc
			(sizeof (bdbm_llm_req_t) * p->nr_punits_pages)) == NULL) {
		bdbm_error ("bdbm_zmalloc failed");
		bdbm_page_ftl_destroy (bdi);
		return 1;
	}
	bdbm_sema_init (&p->gc_hlm.done);
	hlm_reqs_pool_allocate_llm_reqs (p->gc_hlm.llm_reqs, p->nr_punits_pages, RP_MEM_PHY);

	if ((p->gc_hlm_w.llm_reqs = (bdbm_llm_req_t*)bdbm_zmalloc
			(sizeof (bdbm_llm_req_t) * p->nr_punits_pages)) == NULL) {
		bdbm_error ("bdbm_zmalloc failed");
		bdbm_page_ftl_destroy (bdi);
		return 1;
	}
	bdbm_sema_init (&p->gc_hlm_w.done);
	hlm_reqs_pool_allocate_llm_reqs (p->gc_hlm_w.llm_reqs, p->nr_punits_pages, RP_MEM_PHY);

	//tjkim
	for (i = 0; i < BDBM_DEV_NR_STREAM; i++) {
		sprintf(buffer, "/tmp/lifetime_asID%d.dat", i);
		if((fp_asID[i] = bdbm_fopen(buffer, O_CREAT | O_WRONLY, 0777)) == 0) {
			bdbm_error ("bdbm_fopen failed");
			return 1;
		}
		sprintf(buffer, "/tmp/lifetime_pcID%d.dat", i);
		if((fp_pcID[i] = bdbm_fopen(buffer, O_CREAT | O_WRONLY, 0777)) == 0) {
			bdbm_error ("bdbm_fopen failed");
			return 1;
		}
		sprintf(buffer, "/tmp/lifetime_type%d.dat", i);
		if((fp_type[i] = bdbm_fopen(buffer, O_CREAT | O_WRONLY, 0777)) == 0) {
			bdbm_error ("bdbm_fopen failed");
			return 1;
		}
	}
	if((fp_all = bdbm_fopen("/tmp/lifetime.dat", O_CREAT | O_WRONLY, 0777)) == 0) {
		bdbm_error ("bdbm_fopen failed");
		return 1;
	}

	if((fp_gc = bdbm_fopen("/tmp/gc.dat", O_CREAT | O_WRONLY, 0777)) == 0) {
		bdbm_error ("bdbm_fopen failed");
		return 1;
	}
	if((fp_waf = bdbm_fopen("/tmp/waf.log", O_CREAT | O_WRONLY | O_APPEND, 0777)) == 0) {
		bdbm_error ("bdbm_fopen failed");
		return 1;
	}

	memset(buffer, 0x00, 128);

	work = (my_work_t*)kmalloc(sizeof(my_work_t), GFP_KERNEL);
	memset(work, 0, sizeof(my_work_t));
	INIT_WORK( &(work->my_work), pc_work_handler);
	wq = create_singlethread_workqueue("pc_wq");

	work_gc = (my_gc_t*)kmalloc(sizeof(my_gc_t), GFP_KERNEL);
	memset(work_gc, 0, sizeof(my_gc_t));
	INIT_WORK( &(work_gc->my_work), gc_work_handler);
	wq_gc = create_singlethread_workqueue("pc_wq_gc");

	autostream_init(bdi);
	return 0;
}

int8_t sub_stream[BDBM_DEV_NR_STREAM] = {0};
extern char format[1024];
extern char str[1024];
void bdbm_page_ftl_destroy (bdbm_drv_info_t* bdi)
{
	uint32_t i;
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;

	if (!p)
		return;

	if (p->gc_hlm_w.llm_reqs) {
		hlm_reqs_pool_release_llm_reqs (p->gc_hlm_w.llm_reqs, p->nr_punits_pages, RP_MEM_PHY);
		bdbm_sema_free (&p->gc_hlm_w.done);
		bdbm_free (p->gc_hlm_w.llm_reqs);
	}
	if (p->gc_hlm.llm_reqs) {
		hlm_reqs_pool_release_llm_reqs (p->gc_hlm.llm_reqs, p->nr_punits_pages, RP_MEM_PHY);
		bdbm_sema_free (&p->gc_hlm.done);
		bdbm_free (p->gc_hlm.llm_reqs);
	}
	if (p->gc_bab)
		bdbm_free (p->gc_bab);
	if (p->sID_for_llm_reqs)
		bdbm_free (p->sID_for_llm_reqs);

	if (p->ac_bab)
		__bdbm_page_ftl_destroy_active_blocks (p->ac_bab);
	if (p->ptr_mapping_table)
		__bdbm_page_ftl_destroy_mapping_table (p->ptr_mapping_table);
	if (p->bai)
		bdbm_abm_destroy (p->bai);
	bdbm_free (p);
	//tjkim
	sprintf(format, "substreams:");
	for(i = 0; i < BDBM_DEV_NR_STREAM; i++){
		bdbm_fclose(fp_asID[i]);
		bdbm_fclose(fp_pcID[i]);
		bdbm_fclose(fp_type[i]);
		if(sub_stream[i] != 0) {
			sprintf(str, "m%d:s%d, ", i, sub_stream[i]);
			strcat(format, str);
			bdbm_memset (str, 0x00, sizeof (str));
		}
	}
	bdbm_msg("%s", format);
	bdbm_memset (format, 0x00, sizeof (format));

	bdbm_fclose(fp_all);
	bdbm_fclose(fp_gc);
	bdbm_fclose(fp_waf);
	if(wq)
		destroy_workqueue(wq);
	if(wq_gc)
		destroy_workqueue(wq_gc);

	kfree(work);
	kfree(work_gc);

	autostream_destroy();
}

uint64_t g_logical_wtime;
uint64_t g_physical_wtime;
uint32_t bdbm_page_ftl_get_free_ppa (
	bdbm_drv_info_t* bdi, 
	int8_t streamID,
	bdbm_phyaddr_t* ppa)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_abm_block_t* b = NULL;
	uint64_t curr_channel;
	uint64_t curr_chip;
	uint64_t bid;

	bdbm_bug_on(streamID < 0 || streamID >= BDBM_DEV_NR_STREAM);

	/* get the channel & chip numbers */
	curr_channel = p->curr_puid[streamID] % np->nr_channels;
	curr_chip = p->curr_puid[streamID] / np->nr_channels;

	/* get the physical offset of the active blocks */
	bid = curr_channel * np->nr_chips_per_channel * BDBM_DEV_NR_STREAM + curr_chip * BDBM_DEV_NR_STREAM + streamID;
#ifdef DEBUG
	bdbm_bug_on(bid < 0 || bid >= np->nr_chips_per_channel*np->nr_channels*BDBM_DEV_NR_STREAM);
#endif
	b = p->ac_bab[bid];
#ifdef DEBUG
	if(b->streamID != streamID) printk("[get_free_ppa] target streamID %d, streamID %d, %lld, %lld, %d\n", b->streamID, streamID, curr_channel, curr_chip, streamID); //shane part
#endif
	//bdbm_msg("free_ppa s:%d (%lld, %lld) b: %lld", streamID, curr_channel, curr_chip, b->block_no);
	ppa->channel_no =  b->channel_no;
	ppa->chip_no = b->chip_no;
	ppa->block_no = b->block_no;
	ppa->page_no = p->curr_page_ofs[streamID];
	ppa->punit_id = BDBM_GET_PUNIT_ID (bdi, ppa);

	bdbm_bug_on(streamID < 0 || streamID >= BDBM_DEV_NR_STREAM);
#ifdef DEBUG

	/* check some error cases before returning the physical address */
	bdbm_bug_on (ppa->channel_no != curr_channel);
	bdbm_bug_on (ppa->chip_no != curr_chip);
	bdbm_bug_on (ppa->page_no >= np->nr_pages_per_block);
#endif

	/* go to the next parallel unit */
	if ((p->curr_puid[streamID] + 1) == p->nr_punits) {
		p->curr_puid[streamID] = 0;
		p->curr_page_ofs[streamID]++;	/* go to the next page */

		/* see if there are sufficient free pages or not */
		if (p->curr_page_ofs[streamID] == np->nr_pages_per_block) {
			/* get active blocks */
			if (__bdbm_page_ftl_get_active_blocks (np, p->bai, p->ac_bab, streamID) != 0) {
				bdbm_error ("__bdbm_page_ftl_get_active_blocks failed");
				return 1;
			}
			/* ok; go ahead with 0 offset */
			/*bdbm_msg ("curr_puid = %llu", p->curr_puid);*/
			p->curr_page_ofs[streamID] = 0;
		}
	} else {
		/*bdbm_msg ("curr_puid = %llu", p->curr_puid);*/
		p->curr_puid[streamID]++;
	}

	return 0;
}

uint64_t g_num_write_req = 0;
extern int32_t tech_type;
extern int _param_display_num;

void do_log(bdbm_drv_info_t* bdi) {
	uint64_t total_write = pmu_get_write_req(bdi);
	bdbm_msg("WAF at %lld: %lld", g_num_write_req, (total_write*100)/g_num_write_req);
	work_gc->erased_blocks = tech_type; 					//tech_type
	work_gc->read_pages = (total_write*100)/g_num_write_req;// waf
	work_gc->written_pages = g_num_write_req;				// total_writes
	queue_work(wq_gc, &(work_gc->my_work));
}

uint32_t bdbm_page_ftl_map_lpa_to_ppa (
	bdbm_drv_info_t* bdi, 
	bdbm_logaddr_t* logaddr,
	bdbm_phyaddr_t* phyaddr)
{
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_page_mapping_entry_t* me = NULL;
	int k;

	/* is it a valid logical address */
	for (k = 0; k < np->nr_subpages_per_page; k++) {
		if (logaddr->lpa[k] == -1) {
			/* the correpsonding subpage must be set to invalid for gc */
			bdbm_abm_invalidate_page (
				p->bai, 
				phyaddr->channel_no, 
				phyaddr->chip_no,
				phyaddr->block_no,
				phyaddr->page_no,
				k
			);
			continue;
		}

		if (logaddr->lpa[k] >= np->nr_subpages_per_ssd) {
			bdbm_error ("LPA is beyond logical space (%llX)", logaddr->lpa[k]);
			return 1;
		}

		/* get the mapping entry for lpa */
		me = &p->ptr_mapping_table[logaddr->lpa[k]];
		bdbm_bug_on (me == NULL);

		/* update the mapping table */
		if (me->status == PFTL_PAGE_VALID) {
			int64_t lifetime;
			//bdbm_msg(" update done, %lld %d", logaddr->lpa[k], b->wtime[phyaddr->page_no*np->nr_subpages_per_page+k]);
			if(logaddr->ofs == 0) {    // skip invalidation for gc req (ofs==wtime), since it will be erased soon
				bdbm_abm_invalidate_page (
						p->bai, 
						me->phyaddr.channel_no, 
						me->phyaddr.chip_no,
						me->phyaddr.block_no,
						me->phyaddr.page_no,
						me->sp_off
						);

				/*
				lifetime = ktime_to_us(ktime_get())- me->writtentime;
				bdbm_bug_on(lifetime < 0);
				*/
				if(me->writtentime > 0) {
#ifdef DEBUG
					bdbm_bug_on(me->writtentime < 0 || me->writtentime > g_logical_wtime);
					bdbm_bug_on(me->sID < 0 || me->sID > BDBM_DEV_NR_STREAM);
#endif
					lifetime = g_logical_wtime - me->writtentime;
#ifdef GET_AVG_LIFETIME
					lifetimesum_sID[me->sID] += lifetime;
					discarded_ID_cnt[me->sID]++;
#endif
					work->lifetime = lifetime;
					work->sID = me->sID;
					work->type = me->type;
					work->pcid = me->pcid;
					work->asid = me->asid;
					work->lba = logaddr->lpa[k];
					work->writtentime = me->writtentime;
					work->invalidtime = g_logical_wtime;
					work->inv_type = 1;
					queue_work(wq, &(work->my_work));

					me->sID = -1;
					me->type = -1;
					me->pcid = -1;
					me->asid = -1;
					me->writtentime = -1;
				}
			}
			else if(logaddr->ofs == 1) {
				if(logaddr->streamID >= 0 || logaddr->streamID < BDBM_DEV_NR_STREAM) {
					me->sID = logaddr->streamID;
					me->pcid = logaddr->streamID;
				}
			}
		}
		me->status = PFTL_PAGE_VALID;
		me->phyaddr.channel_no = phyaddr->channel_no;
		me->phyaddr.chip_no = phyaddr->chip_no;
		me->phyaddr.block_no = phyaddr->block_no;
		me->phyaddr.page_no = phyaddr->page_no;
		me->sp_off = k;

		//tjkim
		if(logaddr->ofs == 0) { // count if it's user request (ofs==0), skip for gc req (ofs==wtime).
			int8_t sID;
			if(me->writtentime != -1) {
				int64_t lifetime;
#ifdef DEBUG
				bdbm_bug_on(me->writtentime < 0 || me->writtentime > g_logical_wtime);
#endif
				lifetime = g_logical_wtime - me->writtentime;

				work->lifetime = lifetime;
				work->sID = me->sID;
				work->type = me->type;
				work->pcid = me->pcid;
				work->asid = me->asid;
				work->lba = logaddr->lpa[k];
				work->writtentime = me->writtentime;
				work->invalidtime = g_logical_wtime;
				work->inv_type = 2;

				queue_work(wq, &(work->my_work));
			}

			sID = logaddr->streamID;
			//if(ID != 0) ID -= 10;
#ifdef DEBUG
			bdbm_bug_on(sID < 0 || sID > BDBM_DEV_NR_STREAM);
#endif
			me->sID = sID;
			me->type = logaddr->type;
			me->pcid = logaddr->pcid;
			me->asid = logaddr->asid;
			me->writtentime = g_logical_wtime++;
			//me->writtentime = ktime_to_us(ktime_get());

			//pmu_inc_ID_cnt(bdi, sID); 
			if(g_num_write_req >= _param_display_num) {
				if(g_num_write_req % 500000 == 0) {
					do_log(bdi);
				}
				else if(_param_display_num < 5000000 && g_num_write_req % 100000 == 0) {
					do_log(bdi);
				}
			}
			g_num_write_req++;
		}
	}

	return 0;
}

uint32_t bdbm_page_ftl_get_ppa (
	bdbm_drv_info_t* bdi, 
	int64_t lpa,
	bdbm_phyaddr_t* phyaddr,
	uint64_t* sp_off)
{
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_page_mapping_entry_t* me = NULL;
	uint32_t ret;

	/* is it a valid logical address */
	if (lpa >= np->nr_subpages_per_ssd) {
		bdbm_error ("A given lpa is beyond logical space (%llu)", lpa);
		return 1;
	}

	/* get the mapping entry for lpa */
	me = &p->ptr_mapping_table[lpa];

	/* NOTE: sometimes a file system attempts to read 
	 * a logical address that was not written before.
	 * in that case, we return 'address 0' */
	if (me->status != PFTL_PAGE_VALID) {
		phyaddr->channel_no = 0;
		phyaddr->chip_no = 0;
		phyaddr->block_no = 0;
		phyaddr->page_no = 0;
		phyaddr->punit_id = 0;
		*sp_off = 0;
		ret = 1;
	} else {
		phyaddr->channel_no = me->phyaddr.channel_no;
		phyaddr->chip_no = me->phyaddr.chip_no;
		phyaddr->block_no = me->phyaddr.block_no;
		phyaddr->page_no = me->phyaddr.page_no;
		phyaddr->punit_id = BDBM_GET_PUNIT_ID (bdi, phyaddr);
		*sp_off = me->sp_off;
		ret = 0;
	}

	return ret;
}

uint32_t bdbm_page_ftl_invalidate_lpa (
	bdbm_drv_info_t* bdi, 
	int64_t lpa, 
	uint64_t len)
{	
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_page_mapping_entry_t* me = NULL;
	uint64_t loop;

	/* check the range of input addresses */
	if ((lpa + len) > np->nr_subpages_per_ssd) {
		bdbm_warning ("LPA is beyond logical space (%llu = %llu+%llu) %llu", 
			lpa+len, lpa, len, np->nr_subpages_per_ssd);
		return 1;
	}

	/* make them invalid */
	for (loop = lpa; loop < (lpa + len); loop++) {
		me = &p->ptr_mapping_table[loop];
		if (me->status == PFTL_PAGE_VALID) {
			int64_t lifetime;
			bdbm_abm_invalidate_page (
				p->bai, 
				me->phyaddr.channel_no, 
				me->phyaddr.chip_no,
				me->phyaddr.block_no,
				me->phyaddr.page_no,
				me->sp_off
			);
			me->status = PFTL_PAGE_INVALID;

			if(me->writtentime >= 0) {
#ifdef DEBUG
				bdbm_bug_on(me->writtentime > g_logical_wtime);
				bdbm_bug_on(me->sID < 0 || me->sID > BDBM_DEV_NR_STREAM);
#endif
				lifetime = g_logical_wtime - me->writtentime;

				work->lifetime = lifetime;
				work->sID = me->sID;
				work->type = me->type;
				work->pcid = me->pcid;
				work->asid = me->asid;
				work->lba = loop;
				work->writtentime = me->writtentime;
				work->invalidtime = g_logical_wtime;
				work->inv_type = 3;
				queue_work(wq, &(work->my_work));

				me->sID = -1;
				me->type = -1;
				me->pcid = -1;
				me->asid = -1;
				me->writtentime = -1;
			}
		}
	}

	return 0;
}

uint8_t bdbm_page_ftl_is_gc_needed (bdbm_drv_info_t* bdi, int64_t lpa)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	uint64_t nr_total_blks = bdbm_abm_get_nr_total_blocks (p->bai);
	uint64_t nr_free_blks = bdbm_abm_get_nr_free_blocks (p->bai);

	/* invoke gc when remaining free blocks are less than 1% of total blocks */
	if ((nr_free_blks * 100 / nr_total_blks) <= (2*BDBM_DEV_NR_STREAM)) {
		return 1;
	}

	/* invoke gc when there is only one dirty block (for debugging) */
	/*
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	if (bdbm_abm_get_nr_dirty_blocks (p->bai) > 1) {
		return 1;
	}
	*/

	return 0;
}

/* VICTIM SELECTION - First Selection:
 * select the first dirty block in a list */
bdbm_abm_block_t* __bdbm_page_ftl_victim_selection (
	bdbm_drv_info_t* bdi,
	uint64_t channel_no,
	uint64_t chip_no)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_abm_block_t* a = NULL;
	bdbm_abm_block_t* b = NULL;
	struct list_head* pos = NULL;

	a = p->ac_bab[channel_no*np->nr_chips_per_channel + chip_no];
	bdbm_abm_list_for_each_dirty_block (pos, p->bai, channel_no, chip_no) {
		b = bdbm_abm_fetch_dirty_block (pos);
		if (a != b)
			break;
		b = NULL;
	}

	return b;
}

/* VICTIM SELECTION - Greedy:
 * select a dirty block with a small number of valid pages */
bdbm_abm_block_t* __bdbm_page_ftl_victim_selection_greedy (
	bdbm_drv_info_t* bdi,
	uint64_t channel_no,
	uint64_t chip_no)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_abm_block_t* a[BDBM_DEV_NR_STREAM];
	bdbm_abm_block_t* b = NULL;
	bdbm_abm_block_t* v = NULL;
	struct list_head* pos = NULL;
	uint32_t i;

	for(i = 0; i < BDBM_DEV_NR_STREAM; i++) {
		a[i] = p->ac_bab[channel_no * np->nr_chips_per_channel * BDBM_DEV_NR_STREAM + chip_no * BDBM_DEV_NR_STREAM + i];
	}

	bdbm_abm_list_for_each_dirty_block (pos, p->bai, channel_no, chip_no) {
		b = bdbm_abm_fetch_dirty_block (pos);
		for(i = 0; i < BDBM_DEV_NR_STREAM; i++){
			if (a[i] == b)
				goto cont;
		}
		if (b->nr_invalid_subpages == np->nr_subpages_per_block) {
			v = b;
			break;
		}
		if (v == NULL) {
			v = b;
			goto cont;
		}
		if (b->nr_invalid_subpages > v->nr_invalid_subpages)
			v = b;
cont:
		continue;
	}

	return v;
}

/* TODO: need to improve it for background gc */
#if 0
uint32_t bdbm_page_ftl_do_gc (bdbm_drv_info_t* bdi)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_hlm_req_gc_t* hlm_gc = &p->gc_hlm;
	uint64_t nr_gc_blks = 0;
	uint64_t nr_llm_reqs = 0;
	uint64_t nr_punits = 0;
	uint64_t i, j, k;
	bdbm_stopwatch_t sw;

	nr_punits = np->nr_channels * np->nr_chips_per_channel;

	/* choose victim blocks for individual parallel units */
	bdbm_memset (p->gc_bab, 0x00, sizeof (bdbm_abm_block_t*) * nr_punits);
	bdbm_stopwatch_start (&sw);
	for (i = 0, nr_gc_blks = 0; i < np->nr_channels; i++) {
		for (j = 0; j < np->nr_chips_per_channel; j++) {
			bdbm_abm_block_t* b; 
			if ((b = __bdbm_page_ftl_victim_selection_greedy (bdi, i, j))) {
				p->gc_bab[nr_gc_blks] = b;
				nr_gc_blks++;
			}
		}
	}
	if (nr_gc_blks < nr_punits) {
		/* TODO: we need to implement a load balancing feature to avoid this */
		/*bdbm_warning ("TODO: this warning will be removed with load-balancing");*/
		return 0;
	}

	/* build hlm_req_gc for reads */
	for (i = 0, nr_llm_reqs = 0; i < nr_gc_blks; i++) {
		bdbm_abm_block_t* b = p->gc_bab[i];
		if (b == NULL)
			break;
		for (j = 0; j < np->nr_pages_per_block; j++) {
			bdbm_llm_req_t* r = &hlm_gc->llm_reqs[nr_llm_reqs];
			int has_valid = 0;
			/* are there any valid subpages in a block */
			hlm_reqs_pool_reset_fmain (&r->fmain);
			hlm_reqs_pool_reset_logaddr (&r->logaddr);
			for (k = 0; k < np->nr_subpages_per_page; k++) {
				if (b->pst[j*np->nr_subpages_per_page+k] != BDBM_ABM_SUBPAGE_INVALID) {
					has_valid = 1;
					r->logaddr.lpa[k] = -1; /* the subpage contains new data */
					r->fmain.kp_stt[k] = KP_STT_DATA;
				} else {
					r->logaddr.lpa[k] = -1;	/* the subpage contains obsolate data */
					r->fmain.kp_stt[k] = KP_STT_HOLE;
				}
			}
			/* if it is, selects it as the gc candidates */
			if (has_valid) {
				r->req_type = REQTYPE_GC_READ;
				r->phyaddr.channel_no = b->channel_no;
				r->phyaddr.chip_no = b->chip_no;
				r->phyaddr.block_no = b->block_no;
				r->phyaddr.page_no = j;
				r->phyaddr.punit_id = BDBM_GET_PUNIT_ID (bdi, (&r->phyaddr));
				r->ptr_hlm_req = (void*)hlm_gc;
				r->ret = 0;
				nr_llm_reqs++;
			}
		}
	}

	/*
	bdbm_msg ("----------------------------------------------");
	bdbm_msg ("gc-victim: %llu pages, %llu blocks, %llu us", 
		nr_llm_reqs, nr_gc_blks, bdbm_stopwatch_get_elapsed_time_us (&sw));
	*/

	/* wait until Q in llm becomes empty 
	 * TODO: it might be possible to further optimize this */
	bdi->ptr_llm_inf->flush (bdi);

	if (nr_llm_reqs == 0) 
		goto erase_blks;

	/* send read reqs to llm */
	hlm_gc->req_type = REQTYPE_GC_READ;
	hlm_gc->nr_llm_reqs = nr_llm_reqs;
	atomic64_set (&hlm_gc->nr_llm_reqs_done, 0);
	bdbm_sema_lock (&hlm_gc->done);
	for (i = 0; i < nr_llm_reqs; i++) {
		if ((bdi->ptr_llm_inf->make_req (bdi, &hlm_gc->llm_reqs[i])) != 0) {
			bdbm_error ("llm_make_req failed");
			bdbm_bug_on (1);
		}
	}
	bdbm_sema_lock (&hlm_gc->done);
	bdbm_sema_unlock (&hlm_gc->done);

	/* build hlm_req_gc for writes */
	for (i = 0; i < nr_llm_reqs; i++) {
		bdbm_llm_req_t* r = &hlm_gc->llm_reqs[i];
		r->req_type = REQTYPE_GC_WRITE;	/* change to write */
		for (k = 0; k < np->nr_subpages_per_page; k++) {
			/* move subpages that contain new data */
			if (r->fmain.kp_stt[k] == KP_STT_DATA) {
				r->logaddr.lpa[k] = ((uint64_t*)r->foob.data)[k];
			} else if (r->fmain.kp_stt[k] == KP_STT_HOLE) {
				((uint64_t*)r->foob.data)[k] = -1;
				r->logaddr.lpa[k] = -1;
			} else {
				bdbm_bug_on (1);
			}
		}
		if (bdbm_page_ftl_get_free_ppa (bdi, &r->phyaddr) != 0) {
			bdbm_error ("bdbm_page_ftl_get_free_ppa failed");
			bdbm_bug_on (1);
		}
		if (bdbm_page_ftl_map_lpa_to_ppa (bdi, &r->logaddr, &r->phyaddr) != 0) {
			bdbm_error ("bdbm_page_ftl_map_lpa_to_ppa failed");
			bdbm_bug_on (1);
		}
	}

	/* send write reqs to llm */
	hlm_gc->req_type = REQTYPE_GC_WRITE;
	hlm_gc->nr_llm_reqs = nr_llm_reqs;
	atomic64_set (&hlm_gc->nr_llm_reqs_done, 0);
	bdbm_sema_lock (&hlm_gc->done);
	for (i = 0; i < nr_llm_reqs; i++) {
		if ((bdi->ptr_llm_inf->make_req (bdi, &hlm_gc->llm_reqs[i])) != 0) {
			bdbm_error ("llm_make_req failed");
			bdbm_bug_on (1);
		}
	}
	bdbm_sema_lock (&hlm_gc->done);
	bdbm_sema_unlock (&hlm_gc->done);

	/* erase blocks */
erase_blks:
	for (i = 0; i < nr_gc_blks; i++) {
		bdbm_abm_block_t* b = p->gc_bab[i];
		bdbm_llm_req_t* r = &hlm_gc->llm_reqs[i];
		r->req_type = REQTYPE_GC_ERASE;
		r->logaddr.lpa[0] = -1ULL; /* lpa is not available now */
		r->phyaddr.channel_no = b->channel_no;
		r->phyaddr.chip_no = b->chip_no;
		r->phyaddr.block_no = b->block_no;
		r->phyaddr.page_no = 0;
		r->phyaddr.punit_id = BDBM_GET_PUNIT_ID (bdi, (&r->phyaddr));
		r->ptr_hlm_req = (void*)hlm_gc;
		r->ret = 0;
	}

	/* send erase reqs to llm */
	hlm_gc->req_type = REQTYPE_GC_ERASE;
	hlm_gc->nr_llm_reqs = p->nr_punits;
	atomic64_set (&hlm_gc->nr_llm_reqs_done, 0);
	bdbm_sema_lock (&hlm_gc->done);
	for (i = 0; i < nr_gc_blks; i++) {
		if ((bdi->ptr_llm_inf->make_req (bdi, &hlm_gc->llm_reqs[i])) != 0) {
			bdbm_error ("llm_make_req failed");
			bdbm_bug_on (1);
		}
	}
	bdbm_sema_lock (&hlm_gc->done);
	bdbm_sema_unlock (&hlm_gc->done);

	/* FIXME: what happens if block erasure fails */
	for (i = 0; i < nr_gc_blks; i++) {
		uint8_t ret = 0;
		bdbm_abm_block_t* b = p->gc_bab[i];
		if (hlm_gc->llm_reqs[i].ret != 0) 
			ret = 1;	/* bad block */
		bdbm_abm_erase_block (p->bai, b->channel_no, b->chip_no, b->block_no, ret);
	}

	return 0;
}
#endif

extern int8_t g_id;
#define SUBSTREAM_THERSHOLD 384
int8_t get_sID_for_gc(int8_t cur_sID, uint32_t* num_valid_pages_arr) {
	if(cur_sID == 0) return 0;
	if(sub_stream[cur_sID] != 0)
		return sub_stream[cur_sID];
	if(num_valid_pages_arr[cur_sID] > SUBSTREAM_THERSHOLD) {
		int8_t i;
		for(i = 0; i < BDBM_DEV_NR_STREAM; i++){
			if(cur_sID == sub_stream[i])
				return cur_sID;
		}
		if(g_id < BDBM_DEV_NR_STREAM) {
			sub_stream[cur_sID] = g_id++;
			return sub_stream[cur_sID];
		}
		//else bdbm_msg("cannot use substream, g_id: %d, sID: %d, validpages: %d",	g_id, cur_sID, num_valid_pages_arr[cur_sID]);
	}
	return cur_sID;
}

int8_t get_sID_for_gc_all(int8_t cur_sID, uint32_t* num_valid_pages_arr) {
	if(sub_stream[cur_sID] != 0)
		return sub_stream[cur_sID];
	if(g_id < BDBM_DEV_NR_STREAM) {
		sub_stream[cur_sID] = g_id++;
		return sub_stream[cur_sID];
	}
	//else bdbm_msg("cannot use substream, g_id: %d, sID: %d, validpages: %d",	g_id, cur_sID, num_valid_pages_arr[cur_sID]);
	return cur_sID;
}

extern int _param_tech_type;

uint32_t bdbm_page_ftl_do_gc (bdbm_drv_info_t* bdi)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_hlm_req_gc_t* hlm_gc = &p->gc_hlm;
	bdbm_hlm_req_gc_t* hlm_gc_w = &p->gc_hlm_w;
	uint64_t nr_gc_blks = 0;
	uint64_t nr_llm_reqs = 0;
	uint64_t nr_punits = 0;
	uint64_t i, j, k;
	bdbm_stopwatch_t sw;
	uint8_t    num_victim_blocks[BDBM_DEV_NR_STREAM] = {0};
	uint32_t   num_valid_page_read[BDBM_DEV_NR_STREAM] = {0};
	uint32_t   num_valid_page_write[BDBM_DEV_NR_STREAM] = {0};

	nr_punits = np->nr_channels * np->nr_chips_per_channel;

	for(i = 0; i < BDBM_DEV_NR_STREAM; i++){
		work_gc->num_victim_blocks[i] = 0;
		work_gc->num_valid_page_read[i] = 0;
		work_gc->num_valid_page_write[i] = 0;
	}
	work_gc->written_pages = 0;
	work_gc->read_pages = 0;

	/* choose victim blocks for individual parallel units */
	bdbm_memset (p->gc_bab, 0x00, sizeof (bdbm_abm_block_t*) * nr_punits);
	bdbm_memset (p->sID_for_llm_reqs, 0x00, sizeof (uint8_t) * nr_punits * np->nr_pages_per_block);
	bdbm_stopwatch_start (&sw);
	for (i = 0, nr_gc_blks = 0; i < np->nr_channels; i++) {
		for (j = 0; j < np->nr_chips_per_channel; j++) {
			bdbm_abm_block_t* b; 
			if ((b = __bdbm_page_ftl_victim_selection_greedy (bdi, i, j))) {
				p->gc_bab[nr_gc_blks] = b;
				nr_gc_blks++;
				num_victim_blocks[b->streamID]++;
#ifdef DEBUG
				if(b->streamID < 0 || b->streamID >= BDBM_DEV_NR_STREAM)
					bdbm_msg("b->streamID is abnormal, %d", b->streamID);
#endif
			}
		}
	}
	if (nr_gc_blks < nr_punits) {
		/* TODO: we need to implement a load balancing feature to avoid this */
		/*bdbm_warning ("TODO: this warning will be removed with load-balancing");*/
		return 0;
    }

	//pmu_inc_gc(bdi);

    /* TEMP */
    for (i = 0; i < nr_punits * np->nr_pages_per_block; i++) {
        hlm_reqs_pool_reset_fmain (&hlm_gc->llm_reqs[i].fmain);
    }
    /* TEMP */

    /* build hlm_req_gc for reads */
    for (i = 0, nr_llm_reqs = 0; i < nr_gc_blks; i++) {
        bdbm_abm_block_t* b = p->gc_bab[i];
        if (b == NULL)
            break;
        for (j = 0; j < np->nr_pages_per_block; j++) {
            bdbm_llm_req_t* r = &hlm_gc->llm_reqs[nr_llm_reqs];
            int has_valid = 0;
            /* are there any valid subpages in a block */
            hlm_reqs_pool_reset_fmain (&r->fmain);
            hlm_reqs_pool_reset_logaddr (&r->logaddr);
            for (k = 0; k < np->nr_subpages_per_page; k++) {
                if (b->pst[j*np->nr_subpages_per_page+k] != BDBM_ABM_SUBPAGE_INVALID) {
                    has_valid = 1;
                    r->logaddr.lpa[k] = -1; /* the subpage contains new data */
                    r->fmain.kp_stt[k] = KP_STT_DATA;
                } else {
                    r->logaddr.lpa[k] = -1;	/* the subpage contains obsolate data */
                    r->fmain.kp_stt[k] = KP_STT_HOLE;
                }
            }
            /* if it is, selects it as the gc candidates */
            if (has_valid) {
                r->req_type = REQTYPE_GC_READ;
                r->phyaddr.channel_no = b->channel_no;
                r->phyaddr.chip_no = b->chip_no;
                r->phyaddr.block_no = b->block_no;
                r->phyaddr.page_no = j;
                r->phyaddr.punit_id = BDBM_GET_PUNIT_ID (bdi, (&r->phyaddr));
                r->ptr_hlm_req = (void*)hlm_gc;
                r->ret = 0;
				r->sID = b->streamID;
				p->sID_for_llm_reqs[nr_llm_reqs] = b->streamID;
				num_valid_page_read[b->streamID]++;
                nr_llm_reqs++;
            }
        }
    }


    /* wait until Q in llm becomes empty 
     * TODO: it might be possible to further optimize this */
    bdi->ptr_llm_inf->flush (bdi);

    if (nr_llm_reqs == 0) 
        goto erase_blks;

    /* send read reqs to llm */
    hlm_gc->req_type = REQTYPE_GC_READ;
    hlm_gc->nr_llm_reqs = nr_llm_reqs;
    atomic64_set (&hlm_gc->nr_llm_reqs_done, 0);
    bdbm_sema_lock (&hlm_gc->done);
    for (i = 0; i < nr_llm_reqs; i++) {
        if ((bdi->ptr_llm_inf->make_req (bdi, &hlm_gc->llm_reqs[i])) != 0) {
            bdbm_error ("llm_make_req failed");
            bdbm_bug_on (1);
        }
    }
    bdbm_sema_lock (&hlm_gc->done);
    bdbm_sema_unlock (&hlm_gc->done);

#ifdef DEBUG
	if(nr_llm_reqs > nr_punits * np->nr_pages_per_block)
		bdbm_msg("do_gc: abnormal read page: %lld", nr_llm_reqs);
#endif

	work_gc->read_pages = nr_llm_reqs;

    hlm_reqs_pool_write_compaction (hlm_gc_w, hlm_gc, np);

#ifdef DEBUG
	if(hlm_gc->nr_llm_reqs != hlm_gc_w->nr_llm_reqs)
		bdbm_msg("nr_llm_reqs is not equal %lld, %lld", hlm_gc->nr_llm_reqs, hlm_gc_w->nr_llm_reqs);
#endif

    nr_llm_reqs = hlm_gc_w->nr_llm_reqs;

    /* build hlm_req_gc for writes */
    for (i = 0; i < nr_llm_reqs; i++) {
		int32_t old_ofs;
		int8_t streamID = 0;
        bdbm_llm_req_t* r = &hlm_gc_w->llm_reqs[i];
        r->req_type = REQTYPE_GC_WRITE;	/* change to write */
        for (k = 0; k < np->nr_subpages_per_page; k++) {
            /* move subpages that contain new data */
            if (r->fmain.kp_stt[k] == KP_STT_DATA) {
                r->logaddr.lpa[k] = ((uint64_t*)r->foob.data)[k];
            } else if (r->fmain.kp_stt[k] == KP_STT_HOLE) {
                ((uint64_t*)r->foob.data)[k] = -1;
                r->logaddr.lpa[k] = -1;
                bdbm_bug_on (1);
            } else {
                bdbm_bug_on (1);
            }
        }
		r->ptr_hlm_req = (void*)hlm_gc_w;

		switch(_param_tech_type) {
			case 0:
			case 2:
			case 4:
			case 6:
				streamID = r->sID;
				break;
			case 1:
			case 3:
				streamID = get_sID_for_gc_all(r->sID, num_valid_page_read);
				break;
			case 5:
				streamID = get_sID_for_gc(r->sID, num_valid_page_read);
				break;
			case 7:
				streamID = (r->sID == 6) ? 7 : r->sID;
			default:
				streamID = r->sID;
				break;
		}

        if (bdbm_page_ftl_get_free_ppa (bdi, streamID, &r->phyaddr) != 0) {
            bdbm_error ("bdbm_page_ftl_get_free_ppa failed");
            bdbm_bug_on (1);
        }
#ifdef DEBUG
		if(streamID < 0 || streamID >= BDBM_DEV_NR_STREAM)
			bdbm_msg("streamID from llmreq is abnormal, %d", streamID);
#endif
		num_valid_page_write[streamID]++;

		old_ofs = r->logaddr.ofs;
		r->logaddr.ofs = 1;	
		r->logaddr.streamID = streamID;
        if (bdbm_page_ftl_map_lpa_to_ppa (bdi, &r->logaddr, &r->phyaddr) != 0) {
            bdbm_error ("bdbm_page_ftl_map_lpa_to_ppa failed");
            bdbm_bug_on (1);
        }
		r->logaddr.ofs = old_ofs;	
    }

    /* send write reqs to llm */
    hlm_gc_w->req_type = REQTYPE_GC_WRITE;
    hlm_gc_w->nr_llm_reqs = nr_llm_reqs;
    atomic64_set (&hlm_gc_w->nr_llm_reqs_done, 0);
    bdbm_sema_lock (&hlm_gc_w->done);
    for (i = 0; i < nr_llm_reqs; i++) {
        if ((bdi->ptr_llm_inf->make_req (bdi, &hlm_gc_w->llm_reqs[i])) != 0) {
            bdbm_error ("llm_make_req failed");
            bdbm_bug_on (1);
        }
    }
    bdbm_sema_lock (&hlm_gc_w->done);
    bdbm_sema_unlock (&hlm_gc_w->done);
	
#ifdef DEBUG
	if(hlm_gc_w->nr_llm_reqs > nr_punits * np->nr_pages_per_block)
		bdbm_msg("do_gc: abnormal written page: %lld", nr_llm_reqs);
#endif
	work_gc->written_pages = hlm_gc_w->nr_llm_reqs;

    /* erase blocks */
erase_blks:
    for (i = 0; i < nr_gc_blks; i++) {
        bdbm_abm_block_t* b = p->gc_bab[i];
        bdbm_llm_req_t* r = &hlm_gc->llm_reqs[i];
        r->req_type = REQTYPE_GC_ERASE;
        r->logaddr.lpa[0] = -1ULL; /* lpa is not available now */
        r->phyaddr.channel_no = b->channel_no;
        r->phyaddr.chip_no = b->chip_no;
        r->phyaddr.block_no = b->block_no;
        r->phyaddr.page_no = 0;
        r->phyaddr.punit_id = BDBM_GET_PUNIT_ID (bdi, (&r->phyaddr));
        r->ptr_hlm_req = (void*)hlm_gc;
        r->ret = 0;
    }

    /* send erase reqs to llm */
    hlm_gc->req_type = REQTYPE_GC_ERASE;
    hlm_gc->nr_llm_reqs = p->nr_punits;
    atomic64_set (&hlm_gc->nr_llm_reqs_done, 0);
    bdbm_sema_lock (&hlm_gc->done);
    for (i = 0; i < nr_gc_blks; i++) {
        if ((bdi->ptr_llm_inf->make_req (bdi, &hlm_gc->llm_reqs[i])) != 0) {
            bdbm_error ("llm_make_req failed");
            bdbm_bug_on (1);
        }
    }
    bdbm_sema_lock (&hlm_gc->done);
    bdbm_sema_unlock (&hlm_gc->done);
	
	work_gc->erased_blocks = nr_gc_blks;

	for(i = 0; i < BDBM_DEV_NR_STREAM; i++) {
		work_gc->num_victim_blocks[i] = num_victim_blocks[i];
		work_gc->num_valid_page_read[i] = num_valid_page_read[i];
		work_gc->num_valid_page_write[i] = num_valid_page_write[i];
	}

	queue_work(wq_gc, &(work_gc->my_work));

    /* FIXME: what happens if block erasure fails */
    for (i = 0; i < nr_gc_blks; i++) {
        uint8_t ret = 0;
        bdbm_abm_block_t* b = p->gc_bab[i];
        if (hlm_gc->llm_reqs[i].ret != 0) 
            ret = 1;	/* bad block */
        bdbm_abm_erase_block (p->bai, b->channel_no, b->chip_no, b->block_no, ret);
    }

	/*
    bdbm_msg ("----------------------------------------------");
    bdbm_msg ("gc-victim: %llu pages, %llu blocks, %llu us", 
            nr_llm_reqs, nr_gc_blks, bdbm_stopwatch_get_elapsed_time_us (&sw));
	*/

	pmu_inc_gc(bdi);

    return 0;
}

#if 0
/* for snapshot */
uint32_t bdbm_page_ftl_load (bdbm_drv_info_t* bdi, const char* fn)
{
    bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
    bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
    bdbm_page_mapping_entry_t* me;
    bdbm_file_t fp = 0;
    uint64_t i, pos = 0;

    /* step1: load abm */
    if (bdbm_abm_load (p->bai, "/usr/share/bdbm_drv/abm.dat") != 0) {
        bdbm_error ("bdbm_abm_load failed");
        return 1;
    }

    /* step2: load mapping table */
    if ((fp = bdbm_fopen (fn, O_RDWR, 0777)) == 0) {
        bdbm_error ("bdbm_fopen failed");
        return 1;
    }

    me = p->ptr_mapping_table;
    for (i = 0; i < np->nr_subpages_per_ssd; i++) {
        pos += bdbm_fread (fp, pos, (uint8_t*)&me[i], sizeof (bdbm_page_mapping_entry_t));
        if (me[i].status != PFTL_PAGE_NOT_ALLOCATED &&
                me[i].status != PFTL_PAGE_VALID &&
                me[i].status != PFTL_PAGE_INVALID &&
                me[i].status != PFTL_PAGE_INVALID_ADDR) {
            bdbm_msg ("snapshot: invalid status = %u", me[i].status);
        }
    }

    /* step3: get active blocks */
    if (__bdbm_page_ftl_get_active_blocks (np, p->bai, p->ac_bab) != 0) {
        bdbm_error ("__bdbm_page_ftl_get_active_blocks failed");
        bdbm_fclose (fp);
        return 1;
    }
    p->curr_puid = 0;
    p->curr_page_ofs = 0;

    bdbm_fclose (fp);

    return 0;
}

uint32_t bdbm_page_ftl_store (bdbm_drv_info_t* bdi, const char* fn)
{
    bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
    bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
    bdbm_page_mapping_entry_t* me;
    bdbm_abm_block_t* b = NULL;
    bdbm_file_t fp = 0;
    uint64_t pos = 0;
    uint64_t i, j, k;
    uint32_t ret;

    /* step1: make active blocks invalid (it's ugly!!!) */
    if ((fp = bdbm_fopen (fn, O_CREAT | O_WRONLY, 0777)) == 0) {
        bdbm_error ("bdbm_fopen failed");
        return 1;
    }

    while (1) {
        /* get the channel & chip numbers */
        i = p->curr_puid % np->nr_channels;
        j = p->curr_puid / np->nr_channels;

        /* get the physical offset of the active blocks */
        b = p->ac_bab[i*np->nr_chips_per_channel + j];

        /* invalidate remaining pages */
        for (k = 0; k < np->nr_subpages_per_page; k++) {
            bdbm_abm_invalidate_page (
                    p->bai, 
                    b->channel_no, 
                    b->chip_no, 
                    b->block_no, 
                    p->curr_page_ofs, 
                    k);
        }
        bdbm_bug_on (b->channel_no != i);
        bdbm_bug_on (b->chip_no != j);

        /* go to the next parallel unit */
        if ((p->curr_puid + 1) == p->nr_punits) {
            p->curr_puid = 0;
            p->curr_page_ofs++;	/* go to the next page */

            /* see if there are sufficient free pages or not */
            if (p->curr_page_ofs == np->nr_pages_per_block) {
                p->curr_page_ofs = 0;
                break;
            }
        } else {
            p->curr_puid++;
        }
    }

    /* step2: store mapping table */
    me = p->ptr_mapping_table;
    for (i = 0; i < np->nr_subpages_per_ssd; i++) {
        pos += bdbm_fwrite (fp, pos, (uint8_t*)&me[i], sizeof (bdbm_page_mapping_entry_t));
    }
    bdbm_fsync (fp);
    bdbm_fclose (fp);

    /* step3: store abm */
    ret = bdbm_abm_store (p->bai, "/usr/share/bdbm_drv/abm.dat");

    return ret;
}
#endif
void __bdbm_page_badblock_scan_eraseblks (
        bdbm_drv_info_t* bdi,
        uint64_t block_no)
{
    bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
    bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
    bdbm_hlm_req_gc_t* hlm_gc = &p->gc_hlm;
    uint64_t i, j;

    /* setup blocks to erase */
    bdbm_memset (p->gc_bab, 0x00, sizeof (bdbm_abm_block_t*) * p->nr_punits);
    for (i = 0; i < np->nr_channels; i++) {
        for (j = 0; j < np->nr_chips_per_channel; j++) {
            bdbm_abm_block_t* b = NULL;
            bdbm_llm_req_t* r = NULL;
            uint64_t punit_id = i*np->nr_chips_per_channel+j;

            if ((b = bdbm_abm_get_block (p->bai, i, j, block_no)) == NULL) {
                bdbm_error ("oops! bdbm_abm_get_block failed");
                bdbm_bug_on (1);
            }
            p->gc_bab[punit_id] = b;

            r = &hlm_gc->llm_reqs[punit_id];
            r->req_type = REQTYPE_GC_ERASE;
            r->logaddr.lpa[0] = -1ULL; /* lpa is not available now */
            r->phyaddr.channel_no = b->channel_no;
            r->phyaddr.chip_no = b->chip_no;
            r->phyaddr.block_no = b->block_no;
            r->phyaddr.page_no = 0;
            r->phyaddr.punit_id = BDBM_GET_PUNIT_ID (bdi, (&r->phyaddr));
            r->ptr_hlm_req = (void*)hlm_gc;
            r->ret = 0;
        }
    }

    /* send erase reqs to llm */
    hlm_gc->req_type = REQTYPE_GC_ERASE;
    hlm_gc->nr_llm_reqs = p->nr_punits;
    atomic64_set (&hlm_gc->nr_llm_reqs_done, 0);
    bdbm_sema_lock (&hlm_gc->done);
    for (i = 0; i < p->nr_punits; i++) {
        if ((bdi->ptr_llm_inf->make_req (bdi, &hlm_gc->llm_reqs[i])) != 0) {
            bdbm_error ("llm_make_req failed");
            bdbm_bug_on (1);
        }
    }
    bdbm_sema_lock (&hlm_gc->done);
    bdbm_sema_unlock (&hlm_gc->done);

    for (i = 0; i < p->nr_punits; i++) {
        uint8_t ret = 0;
        bdbm_abm_block_t* b = p->gc_bab[i];

        if (hlm_gc->llm_reqs[i].ret != 0) {
            ret = 1; /* bad block */
        }

        bdbm_abm_erase_block (p->bai, b->channel_no, b->chip_no, b->block_no, ret);
    }

    /* measure gc elapsed time */
}

#if 0
static void __bdbm_page_mark_it_dead (
        bdbm_drv_info_t* bdi,
        uint64_t block_no)
{
    bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
    bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
    int i, j;

    for (i = 0; i < np->nr_channels; i++) {
        for (j = 0; j < np->nr_chips_per_channel; j++) {
            bdbm_abm_block_t* b = NULL;

            if ((b = bdbm_abm_get_block (p->bai, i, j, block_no)) == NULL) {
                bdbm_error ("oops! bdbm_abm_get_block failed");
                bdbm_bug_on (1);
            }

            bdbm_abm_set_to_dirty_block (p->bai, i, j, block_no);
        }
    }
}
#endif

uint32_t bdbm_page_badblock_scan (bdbm_drv_info_t* bdi)
{
    bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
    bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
    bdbm_page_mapping_entry_t* me = NULL;
    uint64_t i = 0;
    uint32_t ret = 0;

    bdbm_msg ("[WARNING] 'bdbm_page_badblock_scan' is called! All of the flash blocks will be erased!!!");

    /* step1: reset the page-level mapping table */
    bdbm_msg ("step1: reset the page-level mapping table");
    me = p->ptr_mapping_table;
    for (i = 0; i < np->nr_subpages_per_ssd; i++) {
        me[i].status = PFTL_PAGE_NOT_ALLOCATED;
        me[i].phyaddr.channel_no = PFTL_PAGE_INVALID_ADDR;
        me[i].phyaddr.chip_no = PFTL_PAGE_INVALID_ADDR;
        me[i].phyaddr.block_no = PFTL_PAGE_INVALID_ADDR;
        me[i].phyaddr.page_no = PFTL_PAGE_INVALID_ADDR;
        me[i].sp_off = -1;
    }

    /* step2: erase all the blocks */
    bdi->ptr_llm_inf->flush (bdi);
    for (i = 0; i < np->nr_blocks_per_chip; i++) {
        __bdbm_page_badblock_scan_eraseblks (bdi, i);
    }

    /* step3: store abm */
    if ((ret = bdbm_abm_store (p->bai, "/usr/share/bdbm_drv/abm.dat"))) {
        bdbm_error ("bdbm_abm_store failed");
        return 1;
    }

#if 0
    /* step4: get active blocks */
    bdbm_msg ("step2: get active blocks");
    if (__bdbm_page_ftl_get_active_blocks (np, p->bai, p->ac_bab) != 0) {
        bdbm_error ("__bdbm_page_ftl_get_active_blocks failed");
        return 1;
    }
    p->curr_puid = 0;
    p->curr_page_ofs = 0;

#endif

    bdbm_msg ("done");

    return 0;

#if 0
    /* TEMP: on-demand format */
    bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
    bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
    bdbm_page_mapping_entry_t* me = NULL;
    uint64_t i = 0;
    uint32_t ret = 0;
    uint32_t erased_blocks = 0;

    bdbm_msg ("[WARNING] 'bdbm_page_badblock_scan' is called! All of the flash blocks will be dirty!!!");

    /* step1: reset the page-level mapping table */
    bdbm_msg ("step1: reset the page-level mapping table");
    me = p->ptr_mapping_table;
    for (i = 0; i < np->nr_pages_per_ssd; i++) {
        me[i].status = PFTL_PAGE_NOT_ALLOCATED;
        me[i].phyaddr.channel_no = PFTL_PAGE_INVALID_ADDR;
        me[i].phyaddr.chip_no = PFTL_PAGE_INVALID_ADDR;
        me[i].phyaddr.block_no = PFTL_PAGE_INVALID_ADDR;
        me[i].phyaddr.page_no = PFTL_PAGE_INVALID_ADDR;
    }

    /* step2: erase all the blocks */
    bdi->ptr_llm_inf->flush (bdi);
    for (i = 0; i < np->nr_blocks_per_chip; i++) {
        if (erased_blocks <= p->nr_punits)
            __bdbm_page_badblock_scan_eraseblks (bdi, i);
        else 
            __bdbm_page_mark_it_dead (bdi, i);
        erased_blocks += np->nr_channels;
    }

    /* step3: store abm */
    if ((ret = bdbm_abm_store (p->bai, "/usr/share/bdbm_drv/abm.dat"))) {
        bdbm_error ("bdbm_abm_store failed");
        return 1;
    }

    /* step4: get active blocks */
    bdbm_msg ("step2: get active blocks");
    if (__bdbm_page_ftl_get_active_blocks (np, p->bai, p->ac_bab) != 0) {
        bdbm_error ("__bdbm_page_ftl_get_active_blocks failed");
        return 1;
    }
    p->curr_puid = 0;
    p->curr_page_ofs = 0;

    bdbm_msg ("[summary] Total: %llu, Free: %llu, Clean: %llu, Dirty: %llu",
            bdbm_abm_get_nr_total_blocks (p->bai),
            bdbm_abm_get_nr_free_blocks (p->bai),
            bdbm_abm_get_nr_clean_blocks (p->bai),
            bdbm_abm_get_nr_dirty_blocks (p->bai)
            );
#endif
    bdbm_msg ("done");

    return 0;

}
