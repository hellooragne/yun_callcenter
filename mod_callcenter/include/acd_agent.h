#ifndef __ACD_AGENT__
#define __ACD_AGENT__

#include "acd_tools.h"
#include "acd_state.h"
#include "acd_common.h"

cc_status_t cc_agent_add(const char *agent, const char *type);


cc_status_t cc_agent_del(const char *agent);


cc_status_t cc_agent_get(const char *key, const char *agent, char *ret_result, size_t ret_result_size);


cc_status_t cc_agent_update(const char *key, const char *value, const char *agent);


switch_status_t load_agent(const char *agent_name);

#endif
