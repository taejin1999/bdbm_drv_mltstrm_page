#include "bdbm_drv.h"
#include "hlm_nobuf.h"
#include "hlm_reqs_pool.h"

uint32_t autostream_init(bdbm_drv_info_t* bdi);
uint32_t autostream_destroy(void);
int8_t autostream_get_streamid(uint64_t sLBA, uint64_t sz);
