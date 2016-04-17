#include "acd_queue.h"
#include "acd_common.h"


static switch_xml_config_int_options_t config_int_0_86400 = { SWITCH_TRUE, 0, SWITCH_TRUE, 86400 };

void free_queue(cc_queue_t *queue)
{
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Destroying Profile %s\n", queue->name);
	switch_core_destroy_memory_pool(&queue->pool);
}

void queue_rwunlock(cc_queue_t *queue)
{
	switch_thread_rwlock_unlock(queue->rwlock);
	if (switch_test_flag(queue, PFLAG_DESTROY)) {
		if (switch_thread_rwlock_trywrlock(queue->rwlock) == SWITCH_STATUS_SUCCESS) {
			switch_thread_rwlock_unlock(queue->rwlock);
			free_queue(queue);
		}
	}
}

void destroy_queue(const char *queue_name, switch_bool_t block)
{
	cc_queue_t *queue = NULL;
	switch_mutex_lock(globals.mutex);
	if ((queue = (cc_queue_t *)switch_core_hash_find(globals.queue_hash, queue_name))) {
		switch_core_hash_delete(globals.queue_hash, queue_name);
	}
	switch_mutex_unlock(globals.mutex);

	if (!queue) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "[%s] Invalid queue\n", queue_name);
		return;
	}

	if (block) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "[%s] Waiting for write lock\n", queue->name);
		switch_thread_rwlock_wrlock(queue->rwlock);
	} else {
		if (switch_thread_rwlock_trywrlock(queue->rwlock) != SWITCH_STATUS_SUCCESS) {
			/* Lock failed, set the destroy flag so it'll be destroyed whenever its not in use anymore */
			switch_set_flag(queue, PFLAG_DESTROY);
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "[%s] queue is in use, memory will be freed whenever its no longer in use\n",
					queue->name);
			return;
		}
	}

	free_queue(queue);
}



/*!
 * \brief Sets the queue's configuration instructions 
 */
cc_queue_t *queue_set_config(cc_queue_t *queue)
{
	int i = 0;

	queue->config_str_pool.pool = queue->pool;

	/*
	   SWITCH _CONFIG_SET_ITEM(item, "key", type, flags, 
	   pointer, default, options, help_syntax, help_description)
	 */
	SWITCH_CONFIG_SET_ITEM(queue->config[i++], "strategy", SWITCH_CONFIG_STRING, 0, &queue->strategy, "longest-idle-agent", &queue->config_str_pool, NULL, NULL);
	SWITCH_CONFIG_SET_ITEM(queue->config[i++], "moh-sound", SWITCH_CONFIG_STRING, 0, &queue->moh, NULL, &queue->config_str_pool, NULL, NULL);
	SWITCH_CONFIG_SET_ITEM(queue->config[i++], "record-template", SWITCH_CONFIG_STRING, 0, &queue->record_template, NULL, &queue->config_str_pool, NULL, NULL);
	SWITCH_CONFIG_SET_ITEM(queue->config[i++], "time-base-score", SWITCH_CONFIG_STRING, 0, &queue->time_base_score, "queue", &queue->config_str_pool, NULL, NULL);

	SWITCH_CONFIG_SET_ITEM(queue->config[i++], "tier-rules-apply", SWITCH_CONFIG_BOOL, 0, &queue->tier_rules_apply, SWITCH_FALSE, NULL, NULL, NULL);
	SWITCH_CONFIG_SET_ITEM(queue->config[i++], "tier-rule-wait-second", SWITCH_CONFIG_INT, 0, &queue->tier_rule_wait_second, 0, &config_int_0_86400, NULL, NULL);
	SWITCH_CONFIG_SET_ITEM(queue->config[i++], "tier-rule-wait-multiply-level", SWITCH_CONFIG_BOOL, 0, &queue->tier_rule_wait_multiply_level, SWITCH_FALSE, NULL, NULL, NULL);
	SWITCH_CONFIG_SET_ITEM(queue->config[i++], "tier-rule-no-agent-no-wait", SWITCH_CONFIG_BOOL, 0, &queue->tier_rule_no_agent_no_wait, SWITCH_TRUE, NULL, NULL, NULL);
	SWITCH_CONFIG_SET_ITEM(queue->config[i++], "discard-abandoned-after", SWITCH_CONFIG_INT, 0, &queue->discard_abandoned_after, 60, &config_int_0_86400, NULL, NULL);
	SWITCH_CONFIG_SET_ITEM(queue->config[i++], "abandoned-resume-allowed", SWITCH_CONFIG_BOOL, 0, &queue->abandoned_resume_allowed, SWITCH_FALSE, NULL, NULL, NULL);

	SWITCH_CONFIG_SET_ITEM(queue->config[i++], "max-wait-time", SWITCH_CONFIG_INT, 0, &queue->max_wait_time, 0, &config_int_0_86400, NULL, NULL);
	SWITCH_CONFIG_SET_ITEM(queue->config[i++], "max-wait-time-with-no-agent", SWITCH_CONFIG_INT, 0, &queue->max_wait_time_with_no_agent, 0, &config_int_0_86400, NULL, NULL);
	SWITCH_CONFIG_SET_ITEM(queue->config[i++], "max-wait-time-with-no-agent-time-reached", SWITCH_CONFIG_INT, 0, &queue->max_wait_time_with_no_agent_time_reached, 5, &config_int_0_86400, NULL, NULL);

	switch_assert(i < CC_QUEUE_CONFIGITEM_COUNT);

	return queue;

}

cc_queue_t *load_queue(const char *queue_name)
{
	cc_queue_t *queue = NULL;
	switch_xml_t x_queues, x_queue, cfg, xml;
	switch_event_t *event = NULL;

	if (!(xml = switch_xml_open_cfg(global_cf, &cfg, NULL))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Open of %s failed\n", global_cf);
		return queue;
	}
	if (!(x_queues = switch_xml_child(cfg, "queues"))) {
		goto end;
	}

	if ((x_queue = switch_xml_find_child(x_queues, "queue", "name", queue_name))) {
		switch_memory_pool_t *pool;
		int count;

		if (switch_core_new_memory_pool(&pool) != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CRIT, "Pool Failure\n");
			goto end;
		}

		if (!(queue = (cc_queue_t *)switch_core_alloc(pool, sizeof(cc_queue_t)))) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CRIT, "Alloc Failure\n");
			switch_core_destroy_memory_pool(&pool);
			goto end;
		}

		queue->pool = pool;
		queue_set_config(queue);

		/* Add the params to the event structure */
		count = switch_event_import_xml(switch_xml_child(x_queue, "param"), "name", "value", &event);

		if (switch_xml_config_parse_event(event, count, SWITCH_FALSE, queue->config) != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to process configuration\n");
			switch_core_destroy_memory_pool(&pool);
			goto end;
		}

		switch_thread_rwlock_create(&queue->rwlock, pool);
		queue->name = switch_core_strdup(pool, queue_name);

		queue->last_agent_exist = 0;
		queue->last_agent_exist_check = 0;

		switch_mutex_init(&queue->mutex, SWITCH_MUTEX_NESTED, queue->pool);
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Added queue %s\n", queue->name);
		switch_core_hash_insert(globals.queue_hash, queue->name, queue);

	}

end:

	if (xml) {
		switch_xml_free(xml);
	}
	if (event) {
		switch_event_destroy(&event);
	}
	return queue;
}

cc_queue_t *get_queue(const char *queue_name)
{
	cc_queue_t *queue = NULL;

	switch_mutex_lock(globals.mutex);
	if (!(queue = (cc_queue_t *)switch_core_hash_find(globals.queue_hash, queue_name))) {
		queue = load_queue(queue_name);
	}
	if (queue) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG10, "[%s] rwlock\n", queue->name);

		switch_thread_rwlock_rdlock(queue->rwlock);
	}
	switch_mutex_unlock(globals.mutex);

	return queue;
}


int cc_queue_count(const char *queue)
{
	char *sql;
	int count = 0;
	char res[256] = "0";
	const char *event_name = "Single-Queue";
	switch_event_t *event;

	if (!switch_strlen_zero(queue)) {
		if (queue[0] == '*') {
			event_name = "All-Queues";
			sql = switch_mprintf("SELECT count(*) FROM members WHERE state = '%q' OR state = '%q'",
					cc_member_state2str(CC_MEMBER_STATE_WAITING), cc_member_state2str(CC_MEMBER_STATE_TRYING));
		} else {
			sql = switch_mprintf("SELECT count(*) FROM members WHERE queue = '%q' AND (state = '%q' OR state = '%q')",
					queue, cc_member_state2str(CC_MEMBER_STATE_WAITING), cc_member_state2str(CC_MEMBER_STATE_TRYING));
		}
		cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
		switch_safe_free(sql);
		count = atoi(res);

		if (switch_event_create_subclass(&event, SWITCH_EVENT_CUSTOM, CALLCENTER_EVENT) == SWITCH_STATUS_SUCCESS) {
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Queue", queue);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Action", "members-count");
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Count", res);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Selection", event_name);
			switch_event_fire(&event);
		}
	}	

	return count;
}
