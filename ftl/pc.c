#if defined(KERNEL_MODE)
#include <linux/module.h>
#include <linux/blkdev.h>

#elif defined(USER_MODE)
#include <stdio.h>
#include <stdint.h>

#else
#error Invalid Platform (KERNEL_MODE or USER_MODE)
#endif

#include "debug.h"
#include "params.h"
#include "bdbm_drv.h"
#include "hlm_nobuf.h"
#include "hlm_reqs_pool.h"
#include "utime.h"
#include "umemory.h"

#include "pc.h"

#define MAX_PCID 128


typedef struct {
	uint64_t pc;
	uint64_t cnt;
	int8_t id;
}pcinfo_t;

pcinfo_t pcids[MAX_PCID] = {0};
int32_t num_pcs = 0;


#ifdef AUTOSTREAM_GC
int8_t g_id = 4;
#else
int8_t g_id = 1;
#endif
int8_t get_pcid(uint64_t pc_signature) {
	int8_t id = 0;
	uint8_t i;

	if(pc_signature == 0) {
		//bdbm_msg("pc: pc_signature is 0");
		return 0;
	}

	for(i = 0; i < MAX_PCID; i++) {
		if(pcids[i].pc == pc_signature) {
			pcids[i].cnt++;
			if(pcids[i].cnt > 500 && pcids[i].id == 0 && g_id < BDBM_DEV_NR_STREAM) {
				pcids[i].id = g_id++;
				if(g_id >= BDBM_DEV_NR_STREAM)
					bdbm_msg("no more available stream id for PC");
			}
			id = pcids[i].id;
			break;
		}
		if(pcids[i].pc == 0) {
			pcids[i].cnt++;
			pcids[i].pc = pc_signature;
			pcids[i].id = 0;
			id = pcids[i].id;
			num_pcs++;
			break;
		}
	}
	if(i == MAX_PCID) {
		//bdbm_msg("pc array is full");
	}

	return id;
}

void print_num_pcs(void) {
	bdbm_msg("num_pcs: %d", num_pcs);
}

uint64_t get_pcid_cnt(int32_t pcid) {
	return pcids[pcid].cnt;
}

extern char format[1024];
extern char str[1024];
void print_pc_cnt(void) {
	uint32_t i;

	sprintf(format, "pc: ");
	for(i = 0; i < MAX_PCID; i++) {
		if(pcids[i].pc == 0) break;
		sprintf(str, "%llx(%d)%lld, ", pcids[i].pc, pcids[i].id, pcids[i].cnt);
		strcat(format, str);
		bdbm_memset (str, 0x00, sizeof (str));
	}
	bdbm_msg("%s", format);
	bdbm_memset (format, 0x00, sizeof (format));
}
