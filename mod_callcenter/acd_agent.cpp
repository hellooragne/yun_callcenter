#include "acd_agent.h"

cc_status_t cc_agent_add(const char *agent, const char *type)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;

	if (!strcasecmp(type, CC_AGENT_TYPE_CALLBACK) || !strcasecmp(type, CC_AGENT_TYPE_UUID_STANDBY)) {
		char res[256] = "";
		/* Check to see if agent already exist */
		sql = switch_mprintf("SELECT count(*) FROM agents WHERE name = '%q'", agent);
		cc_execute_sql2str(NULL, sql, res, sizeof(res));
		switch_safe_free(sql);

		if (atoi(res) != 0) {
			result = CC_STATUS_AGENT_ALREADY_EXIST;
			goto done;
		}
		/* Add Agent */
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Adding Agent %s with type %s with default status %s\n", 
				agent, type, cc_agent_status2str(CC_AGENT_STATUS_LOGGED_OUT));
		sql = switch_mprintf("INSERT INTO agents (name, system, type, status, state) VALUES('%q', 'single_box', '%q', '%q', '%q');", 
				agent, type, cc_agent_status2str(CC_AGENT_STATUS_LOGGED_OUT), cc_agent_state2str(CC_AGENT_STATE_WAITING));
		cc_execute_sql(sql, NULL);
		switch_safe_free(sql);
	} else {
		result = CC_STATUS_AGENT_INVALID_TYPE;
		goto done;
	}
done:		
	return result;
}

cc_status_t cc_agent_del(const char *agent)
{
	cc_status_t result = CC_STATUS_SUCCESS;

	char *sql;

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Deleted Agent %s\n", agent);
	sql = switch_mprintf("DELETE FROM agents WHERE name = '%q';"
			"DELETE FROM tiers WHERE agent = '%q';",
			agent, agent);
	cc_execute_sql(sql, NULL);
	switch_safe_free(sql);
	return result;
}

cc_status_t cc_agent_get(const char *key, const char *agent, char *ret_result, size_t ret_result_size)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
	switch_event_t *event;
	char res[256];

	/* Check to see if agent already exists */
	sql = switch_mprintf("SELECT count(*) FROM agents WHERE name = '%q'", agent);
	cc_execute_sql2str(NULL, sql, res, sizeof(res));
	switch_safe_free(sql);

	if (atoi(res) == 0) {
		result = CC_STATUS_AGENT_NOT_FOUND;
		goto done;
	}

	if (!strcasecmp(key, "status") || !strcasecmp(key, "state") || !strcasecmp(key, "uuid") ) { 
		/* Check to see if agent already exists */
		sql = switch_mprintf("SELECT %q FROM agents WHERE name = '%q'", key, agent);
		cc_execute_sql2str(NULL, sql, res, sizeof(res));
		switch_safe_free(sql);
		switch_snprintf(ret_result, ret_result_size, "%s", res);
		result = CC_STATUS_SUCCESS;

		if (switch_event_create_subclass(&event, SWITCH_EVENT_CUSTOM, CALLCENTER_EVENT) == SWITCH_STATUS_SUCCESS) {
			char tmpname[256];
			if (!strcasecmp(key, "uuid")) {
				switch_snprintf(tmpname, sizeof(tmpname), "CC-Agent-UUID");	
			} else {
				switch_snprintf(tmpname, sizeof(tmpname), "CC-Agent-%c%s", (char) switch_toupper(key[0]), key+1);
			}
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent", agent);
			switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Action", "agent-%s-get", key);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, tmpname, res);
			switch_event_fire(&event);
		}

	} else {
		result = CC_STATUS_INVALID_KEY;
		goto done;

	}

done:   
	if (result == CC_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Get Info Agent %s %s = %s\n", agent, key, res);
	}

	return result;
}

cc_status_t cc_agent_update(const char *key, const char *value, const char *agent)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
	char res[256];
	switch_event_t *event;

	/* Check to see if agent already exist */
	sql = switch_mprintf("SELECT count(*) FROM agents WHERE name = '%q'", agent);
	cc_execute_sql2str(NULL, sql, res, sizeof(res));
	switch_safe_free(sql);

	if (atoi(res) == 0) {
		result = CC_STATUS_AGENT_NOT_FOUND;
		goto done;
	}

	if (!strcasecmp(key, "status")) {
		if (cc_agent_str2status(value) != CC_AGENT_STATUS_UNKNOWN) {
			/* Reset values on available only */
			if (cc_agent_str2status(value) == CC_AGENT_STATUS_AVAILABLE) {
				sql = switch_mprintf("UPDATE agents SET status = '%q', last_status_change = '%" SWITCH_TIME_T_FMT "', talk_time = 0, calls_answered = 0, no_answer_count = 0"
						" WHERE name = '%q' AND NOT status = '%q'",
						value, local_epoch_time_now(NULL),
						agent, value);
			} else {
				sql = switch_mprintf("UPDATE agents SET status = '%q', last_status_change = '%" SWITCH_TIME_T_FMT "' WHERE name = '%q'",
						value, local_epoch_time_now(NULL), agent);
			}
			cc_execute_sql(sql, NULL);
			switch_safe_free(sql);


			/* Used to stop any active callback */
			if (cc_agent_str2status(value) != CC_AGENT_STATUS_AVAILABLE) {
				sql = switch_mprintf("SELECT uuid FROM members WHERE serving_agent = '%q' AND serving_system = 'single_box' AND NOT state = 'Answered'", agent);
				cc_execute_sql2str(NULL, sql, res, sizeof(res));
				switch_safe_free(sql);
				if (!switch_strlen_zero(res)) {

					/*hmeng*/
					switch_core_session_hupall_matching_var_ans("cc_member_pre_answer_uuid", res, SWITCH_CAUSE_ORIGINATOR_CANCEL, (switch_hup_type_t) (SHT_UNANSWERED | SHT_ANSWERED));
				}
			}


			result = CC_STATUS_SUCCESS;

			if (switch_event_create_subclass(&event, SWITCH_EVENT_CUSTOM, CALLCENTER_EVENT) == SWITCH_STATUS_SUCCESS) {
				switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent", agent);
				switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Action", "agent-status-change");
				switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent-Status", value);
				switch_event_fire(&event);
			}

		} else {
			result = CC_STATUS_AGENT_INVALID_STATUS;
			goto done;
		}
	} else if (!strcasecmp(key, "state")) {
		if (cc_agent_str2state(value) != CC_AGENT_STATE_UNKNOWN) {
			if (cc_agent_str2state(value) != CC_AGENT_STATE_RECEIVING) {
				sql = switch_mprintf("UPDATE agents SET state = '%q' WHERE name = '%q'", value, agent);
			} else {
				sql = switch_mprintf("UPDATE agents SET state = '%q', last_offered_call = '%" SWITCH_TIME_T_FMT "' WHERE name = '%q'",
						value, local_epoch_time_now(NULL), agent);
			}
			cc_execute_sql(sql, NULL);
			switch_safe_free(sql);

			result = CC_STATUS_SUCCESS;

			if (switch_event_create_subclass(&event, SWITCH_EVENT_CUSTOM, CALLCENTER_EVENT) == SWITCH_STATUS_SUCCESS) {
				switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent", agent);
				switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Action", "agent-state-change");
				switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent-State", value);
				switch_event_fire(&event);
			}

		} else {
			result = CC_STATUS_AGENT_INVALID_STATE;
			goto done;
		}
	} else if (!strcasecmp(key, "uuid")) {
		sql = switch_mprintf("UPDATE agents SET uuid = '%q', system = 'single_box' WHERE name = '%q'", value, agent);
		cc_execute_sql(sql, NULL);
		switch_safe_free(sql);

		result = CC_STATUS_SUCCESS;
	} else if (!strcasecmp(key, "contact")) {
		sql = switch_mprintf("UPDATE agents SET contact = '%q', system = 'single_box' WHERE name = '%q'", value, agent);
		cc_execute_sql(sql, NULL);
		switch_safe_free(sql);

		result = CC_STATUS_SUCCESS;
	} else if (!strcasecmp(key, "ready_time")) {
		sql = switch_mprintf("UPDATE agents SET ready_time = '%ld', system = 'single_box' WHERE name = '%q'", atol(value), agent);
		cc_execute_sql(sql, NULL);
		switch_safe_free(sql);

		result = CC_STATUS_SUCCESS;
	} else if (!strcasecmp(key, "busy_delay_time")) {
		sql = switch_mprintf("UPDATE agents SET busy_delay_time = '%ld', system = 'single_box' WHERE name = '%q'", atol(value), agent);
		cc_execute_sql(sql, NULL);
		switch_safe_free(sql);

		result = CC_STATUS_SUCCESS;
	} else if (!strcasecmp(key, "reject_delay_time")) {
		sql = switch_mprintf("UPDATE agents SET reject_delay_time = '%ld', system = 'single_box' WHERE name = '%q'", atol(value), agent);
		cc_execute_sql(sql, NULL);
		switch_safe_free(sql);

		result = CC_STATUS_SUCCESS;
	} else if (!strcasecmp(key, "no_answer_delay_time")) {
		sql = switch_mprintf("UPDATE agents SET no_answer_delay_time = '%ld', system = 'single_box' WHERE name = '%q'", atol(value), agent);
		cc_execute_sql(sql, NULL);
		switch_safe_free(sql);

		result = CC_STATUS_SUCCESS;
	} else if (!strcasecmp(key, "type")) {
		if (strcasecmp(value, CC_AGENT_TYPE_CALLBACK) && strcasecmp(value, CC_AGENT_TYPE_UUID_STANDBY)) {
			result = CC_STATUS_AGENT_INVALID_TYPE;
			goto done;
		}

		sql = switch_mprintf("UPDATE agents SET type = '%q' WHERE name = '%q'", value, agent);
		cc_execute_sql(sql, NULL);
		switch_safe_free(sql);

		result = CC_STATUS_SUCCESS;

	} else if (!strcasecmp(key, "max_no_answer")) {
		sql = switch_mprintf("UPDATE agents SET max_no_answer = '%d', system = 'single_box' WHERE name = '%q'", atoi(value), agent);
		cc_execute_sql(sql, NULL);
		switch_safe_free(sql);

		result = CC_STATUS_SUCCESS;

	} else if (!strcasecmp(key, "wrap_up_time")) {
		sql = switch_mprintf("UPDATE agents SET wrap_up_time = '%d', system = 'single_box' WHERE name = '%q'", atoi(value), agent);
		cc_execute_sql(sql, NULL);
		switch_safe_free(sql);

		result = CC_STATUS_SUCCESS;

	} else {
		result = CC_STATUS_INVALID_KEY;
		goto done;

	}

done:
	if (result == CC_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Updated Agent %s set %s = %s\n", agent, key, value);
	}

	return result;
}



switch_status_t load_agent(const char *agent_name)
{
	switch_xml_t x_agents, x_agent, cfg, xml;

	if (!(xml = switch_xml_open_cfg(global_cf, &cfg, NULL))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Open of %s failed\n", global_cf);
		return SWITCH_STATUS_FALSE;
	}
	if (!(x_agents = switch_xml_child(cfg, "agents"))) {
		goto end;
	}

	if ((x_agent = switch_xml_find_child(x_agents, "agent", "name", agent_name))) {
		const char *type = switch_xml_attr(x_agent, "type");
		const char *contact = switch_xml_attr(x_agent, "contact"); 
		const char *status = switch_xml_attr(x_agent, "status");
		const char *max_no_answer = switch_xml_attr(x_agent, "max-no-answer");
		const char *wrap_up_time = switch_xml_attr(x_agent, "wrap-up-time");
		const char *reject_delay_time = switch_xml_attr(x_agent, "reject-delay-time");
		const char *busy_delay_time = switch_xml_attr(x_agent, "busy-delay-time");
		const char *no_answer_delay_time = switch_xml_attr(x_agent, "no-answer-delay-time");

		if (type) {
			cc_status_t res = cc_agent_add(agent_name, type);
			if (res == CC_STATUS_SUCCESS || res == CC_STATUS_AGENT_ALREADY_EXIST) {
				if (contact) {
					cc_agent_update("contact", contact, agent_name);
				}
				if (status) {
					cc_agent_update("status", status, agent_name);
				}
				if (wrap_up_time) {
					cc_agent_update("wrap_up_time", wrap_up_time, agent_name);
				}
				if (max_no_answer) {
					cc_agent_update("max_no_answer", max_no_answer, agent_name);
				}
				if (reject_delay_time) {
					cc_agent_update("reject_delay_time", reject_delay_time, agent_name);
				}
				if (busy_delay_time) {
					cc_agent_update("busy_delay_time", busy_delay_time, agent_name);
				}
				if (no_answer_delay_time) {
					cc_agent_update("no_answer_delay_time", no_answer_delay_time, agent_name);
				}

				if (type && res == CC_STATUS_AGENT_ALREADY_EXIST) {
					cc_agent_update("type", type, agent_name);
				}

			}
		}
	}

end:

	if (xml) {
		switch_xml_free(xml);
	}

	return SWITCH_STATUS_SUCCESS;
}
