#ifndef __ACD_CONFIG_H_
#define __ACD_CONFIG_H_

#include "acd_tools.h"
#include "acd_queue.h"
#include "acd_tiers.h"
#include "acd_common.h"
#include "acd_agent.h"

#define MASTER_NODE       "masternode"
#define SYSTEM_RECOVERY   "systemrecovery"

struct list_result {
       const char *name;
       const char *format;
       int row_process;
       switch_stream_handle_t *stream;

};

switch_status_t load_config(void);

int acd_config_init(switch_loadable_module_interface_t **module_interface, switch_api_interface_t *api_interface);

//SWITCH_STANDARD_API(cc_config_api_function);

#endif
