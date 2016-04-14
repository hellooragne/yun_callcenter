#ifndef __ACD_QUEUE_H_
#define __ACD_QUEUE_H_

#include "acd_tools.h"
#include "acd_state.h"

#define CC_QUEUE_CONFIGITEM_COUNT 100

struct cc_queue {
	char *name;

	char *strategy;
	char *moh;
	char *record_template;
	char *time_base_score;

	switch_bool_t tier_rules_apply;
	uint32_t tier_rule_wait_second;
	switch_bool_t tier_rule_wait_multiply_level;
	switch_bool_t tier_rule_no_agent_no_wait;

	uint32_t discard_abandoned_after;
	switch_bool_t abandoned_resume_allowed;

	uint32_t max_wait_time;
	uint32_t max_wait_time_with_no_agent;
	uint32_t max_wait_time_with_no_agent_time_reached;

	switch_mutex_t *mutex;

	switch_thread_rwlock_t *rwlock;
	switch_memory_pool_t *pool;
	uint32_t flags;

	switch_time_t last_agent_exist;
	switch_time_t last_agent_exist_check;

	switch_xml_config_item_t config[CC_QUEUE_CONFIGITEM_COUNT];
	switch_xml_config_string_options_t config_str_pool;

};

typedef struct cc_queue cc_queue_t;


void free_queue(cc_queue_t *queue);

void queue_rwunlock(cc_queue_t *queue);


void destroy_queue(const char *queue_name, switch_bool_t block);


cc_queue_t *queue_set_config(cc_queue_t *queue);


cc_queue_t *load_queue(const char *queue_name);


cc_queue_t *get_queue(const char *queue_name);


int cc_queue_count(const char *queue);
#endif
