#ifndef __ACD_TIERS_H_
#define __ACD_TIERS_H_

#include "acd_tools.h"
#include "acd_state.h"
#include "acd_queue.h"

cc_status_t cc_tier_add(const char *queue_name, const char *agent, const char *state, int level, int position);

cc_status_t cc_tier_update(const char *key, const char *value, const char *queue_name, const char *agent);


cc_status_t cc_tier_del(const char *queue_name, const char *agent);
#endif
