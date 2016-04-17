#include <switch.h>

#include "acd_tools.h"
#include "acd_state.h"
#include "acd_config.h"
#include "acd_common.h"


/* Prototypes */
SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_callcenter_shutdown);
SWITCH_MODULE_RUNTIME_FUNCTION(mod_callcenter_runtime);
SWITCH_MODULE_LOAD_FUNCTION(mod_callcenter_load);

/* SWITCH_MODULE_DEFINITION(name, load, shutdown, runtime) 
 * Defines a switch_loadable_module_function_table_t and a static const char[] modname
 */
SWITCH_MODULE_DEFINITION(mod_callcenter, mod_callcenter_load, mod_callcenter_shutdown, NULL);

struct globals_type globals;

struct call_helper {
	const char *member_uuid;
	const char *member_session_uuid;
	const char *queue_name;
	const char *queue_strategy;
	const char *member_joined_epoch;
	const char *member_cid_name;
	const char *member_cid_number;
	const char *agent_name;
	const char *agent_system;
	const char *agent_status;
	const char *agent_type;
	const char *agent_uuid;
	const char *originate_string;
	const char *record_template;
	int no_answer_count;
	int max_no_answer;
	int reject_delay_time;
	int busy_delay_time;
	int no_answer_delay_time;

	switch_memory_pool_t *pool;
};




static void *SWITCH_THREAD_FUNC outbound_agent_thread_run(switch_thread_t *thread, void *obj)
{
	struct call_helper *h = (struct call_helper *) obj;
	switch_core_session_t *agent_session = NULL;
	switch_call_cause_t cause = SWITCH_CAUSE_NONE;
	switch_status_t status = SWITCH_STATUS_FALSE;
	char *sql = NULL;
	char *dialstr = NULL;
	cc_tier_state_t tiers_state = CC_TIER_STATE_READY;
	switch_core_session_t *member_session = switch_core_session_locate(h->member_session_uuid);
	switch_event_t *ovars;
	switch_time_t t_agent_called = 0;
	switch_time_t t_agent_answered = 0;
	switch_time_t t_member_called = atoi(h->member_joined_epoch);
	switch_event_t *event = NULL;

	switch_mutex_lock(globals.mutex);
	globals.threads++;
	switch_mutex_unlock(globals.mutex);

	/* member is gone before we could process it */
	if (!member_session) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Member %s <%s> with uuid %s in queue %s is gone just before we assigned an agent\n", h->member_cid_name, h->member_cid_number, h->member_session_uuid, h->queue_name);
	
	
		 sql = switch_mprintf("UPDATE members SET state = '%q', session_uuid = '', abandoned_epoch = '%" SWITCH_TIME_T_FMT "' WHERE system = 'single_box' AND uuid = '%q' AND state != '%q'",
				cc_member_state2str(CC_MEMBER_STATE_ABANDONED), local_epoch_time_now(NULL), h->member_uuid, cc_member_state2str(CC_MEMBER_STATE_ABANDONED));

		cc_execute_sql(sql, NULL);
		switch_safe_free(sql);
		goto done;
	}

	/* Proceed contact the agent to offer the member */
	if (switch_event_create_subclass(&event, SWITCH_EVENT_CUSTOM, CALLCENTER_EVENT) == SWITCH_STATUS_SUCCESS) {
		switch_channel_t *member_channel = switch_core_session_get_channel(member_session);
		switch_caller_profile_t *member_profile = switch_channel_get_caller_profile(member_channel);
		const char *member_dnis = member_profile->rdnis;

		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Queue", h->queue_name);
		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Action", "agent-offering");
		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent", h->agent_name);
		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent-Type", h->agent_type);
		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent-System", h->agent_system);
		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-UUID", h->member_uuid);
		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-Session-UUID", h->member_session_uuid);
		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Name", h->member_cid_name);
		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Number", h->member_cid_number);
		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-DNIS", member_dnis);
		switch_event_fire(&event);
	}

	/* CallBack Mode */
	if (!strcasecmp(h->agent_type, CC_AGENT_TYPE_CALLBACK)) {
		switch_channel_t *member_channel = switch_core_session_get_channel(member_session);
		char *cid_name = NULL;
		const char *cid_name_prefix = NULL;
		if ((cid_name_prefix = switch_channel_get_variable(member_channel, "cc_outbound_cid_name_prefix"))) {
			cid_name = switch_mprintf("%s%s", cid_name_prefix, h->member_cid_name);
			switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_DEBUG, "Setting outbound caller_id_name to: %s\n", cid_name);
		}

		switch_event_create(&ovars, SWITCH_EVENT_REQUEST_PARAMS);
		switch_event_add_header(ovars, SWITCH_STACK_BOTTOM, "cc_queue", "%s", h->queue_name);
		switch_event_add_header(ovars, SWITCH_STACK_BOTTOM, "cc_member_uuid", "%s", h->member_uuid);
		switch_event_add_header(ovars, SWITCH_STACK_BOTTOM, "cc_member_session_uuid", "%s", h->member_session_uuid);
		switch_event_add_header(ovars, SWITCH_STACK_BOTTOM, "cc_member_pre_answer_uuid", "%s", h->member_uuid);
		switch_event_add_header(ovars, SWITCH_STACK_BOTTOM, "cc_agent", "%s", h->agent_name);
		switch_event_add_header(ovars, SWITCH_STACK_BOTTOM, "cc_agent_type", "%s", h->agent_type);
		switch_event_add_header(ovars, SWITCH_STACK_BOTTOM, "cc_side", "%s", "agent");
		switch_event_add_header(ovars, SWITCH_STACK_BOTTOM, "loopback_bowout", "false");
		switch_event_add_header(ovars, SWITCH_STACK_BOTTOM, "loopback_bowout_on_execute", "false");
		switch_event_add_header(ovars, SWITCH_STACK_BOTTOM, "ignore_early_media", "true");

		switch_channel_process_export(member_channel, NULL, ovars, "cc_export_vars");

		t_agent_called = local_epoch_time_now(NULL);
		dialstr = switch_mprintf("%s", h->originate_string);
		status = switch_ivr_originate(NULL, &agent_session, &cause, dialstr, 60, NULL, cid_name ? cid_name : h->member_cid_name, h->member_cid_number, NULL, ovars, SOF_NONE, NULL);
		switch_safe_free(dialstr);
		switch_safe_free(cid_name);

		switch_event_destroy(&ovars);
	/* UUID Standby Mode */
	} else if (!strcasecmp(h->agent_type, CC_AGENT_TYPE_UUID_STANDBY)) {
		agent_session = switch_core_session_locate(h->agent_uuid);
		if (agent_session) {
			switch_channel_t *agent_channel = switch_core_session_get_channel(agent_session);
			switch_event_t *event;
			const char *cc_warning_tone = switch_channel_get_variable(agent_channel, "cc_warning_tone");

			switch_channel_set_variable(agent_channel, "cc_side", "agent");
			switch_channel_set_variable(agent_channel, "cc_queue", h->queue_name);
			switch_channel_set_variable(agent_channel, "cc_agent", h->agent_name);
			switch_channel_set_variable(agent_channel, "cc_agent_type", h->agent_type);
			switch_channel_set_variable(agent_channel, "cc_member_uuid", h->member_uuid);
			switch_channel_set_variable(agent_channel, "cc_member_session_uuid", h->member_session_uuid);

			/* Playback this to the agent */
			if (cc_warning_tone && switch_event_create(&event, SWITCH_EVENT_COMMAND) == SWITCH_STATUS_SUCCESS) {
				switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "call-command", "execute");
				switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "execute-app-name", "playback");
				switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "execute-app-arg", cc_warning_tone);
				switch_core_session_queue_private_event(agent_session, &event, SWITCH_TRUE);
			}

			status = SWITCH_STATUS_SUCCESS;
		} else {
			cc_agent_update("status", cc_agent_status2str(CC_AGENT_STATUS_LOGGED_OUT), h->agent_name);
			cc_agent_update("uuid", "", h->agent_name);
		}
	} else {
		switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_DEBUG, "Invalid agent type '%s' for agent '%s', aborting member offering", h->agent_type, h->agent_name);
		cause = SWITCH_CAUSE_USER_NOT_REGISTERED;
	}

	/* Originate/Bridge is not finished, processing the return value */
	if (status == SWITCH_STATUS_SUCCESS) {
		/* Agent Answered */
		const char *agent_uuid = switch_core_session_get_uuid(agent_session);
		switch_channel_t *member_channel = switch_core_session_get_channel(member_session);
		switch_channel_t *agent_channel = switch_core_session_get_channel(agent_session);
		const char *other_loopback_leg_uuid = switch_channel_get_variable(agent_channel, "other_loopback_leg_uuid");
		const char *o_announce = NULL;

		switch_channel_set_variable(agent_channel, "cc_member_pre_answer_uuid", NULL);

		/* Our agent channel is a loopback. Try to find if a real channel is bridged to it in order
		   to use it as our new agent channel.
		   - Locate the loopback-b channel using 'other_loopback_leg_uuid' variable
		   - Locate the real agent channel using 'signal_bond' variable from loopback-b
		*/
		if (other_loopback_leg_uuid) {
			switch_core_session_t *other_loopback_session = NULL;

			switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_DEBUG, "Agent '%s' is a loopback channel. Searching for real channel...\n", h->agent_name);

			if ((other_loopback_session = switch_core_session_locate(other_loopback_leg_uuid))) {
				switch_channel_t *other_loopback_channel = switch_core_session_get_channel(other_loopback_session);
				const char *real_uuid = NULL;

				/* Wait for the real channel to be fully bridged */
				switch_channel_wait_for_flag(other_loopback_channel, CF_BRIDGED, SWITCH_TRUE, 5000, member_channel);

				real_uuid = switch_channel_get_partner_uuid(other_loopback_channel);
				switch_channel_set_variable(other_loopback_channel, "cc_member_pre_answer_uuid", NULL);

				/* Switch the agent session */
				if (real_uuid) {
					switch_core_session_rwunlock(agent_session);
					agent_session = switch_core_session_locate(real_uuid);
					agent_uuid = switch_core_session_get_uuid(agent_session);
					agent_channel = switch_core_session_get_channel(agent_session);

					if (!switch_channel_test_flag(agent_channel, CF_BRIDGED)) {
						switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_DEBUG, "Timeout waiting for real channel to be bridged (agent '%s')\n", h->agent_name);
					} else {
						switch_channel_set_variable(agent_channel, "cc_queue", h->queue_name);
						switch_channel_set_variable(agent_channel, "cc_agent", h->agent_name);
						switch_channel_set_variable(agent_channel, "cc_agent_type", h->agent_type);
						switch_channel_set_variable(agent_channel, "cc_member_uuid", h->member_uuid);
						switch_channel_set_variable(agent_channel, "cc_member_session_uuid", h->member_session_uuid);

						switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_DEBUG, "Real channel found behind loopback agent '%s'\n", h->agent_name);
					}
				} else {
					switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_DEBUG, "Failed to find a real channel behind loopback agent '%s'\n", h->agent_name);
				}

				switch_core_session_rwunlock(other_loopback_session);
			} else {
				switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_DEBUG, "Failed to locate loopback-b channel of agent '%s'\n", h->agent_name);
			}
		}


		if (!strcasecmp(h->queue_strategy,"ring-all")) {
			char res[256];
			/* Map the Agent to the member */
			sql = switch_mprintf("UPDATE members SET serving_agent = '%q', serving_system = 'single_box', state = '%q'"
					" WHERE state = '%q' AND uuid = '%q' AND system = 'single_box' AND serving_agent = 'ring-all'",
					h->agent_name, cc_member_state2str(CC_MEMBER_STATE_TRYING),
					cc_member_state2str(CC_MEMBER_STATE_TRYING), h->member_uuid);
			cc_execute_sql(sql, NULL);

			switch_safe_free(sql);

			/* Check if we won the race to get the member to our selected agent (Used for Multi system purposes) */
			sql = switch_mprintf("SELECT count(*) FROM members"
					" WHERE serving_agent = '%q' AND serving_system = 'single_box' AND uuid = '%q' AND system = 'single_box'",
					h->agent_name, h->member_uuid);
			cc_execute_sql2str(NULL, sql, res, sizeof(res));
			switch_safe_free(sql);

			if (atoi(res) == 0) {
				goto done;
			}
			//switch_core_session_hupall_matching_var("cc_member_pre_answer_uuid", h->member_uuid, SWITCH_CAUSE_ORIGINATOR_CANCEL);
			switch_core_session_hupall_matching_var_ans("cc_member_pre_answer_uuid", h->member_uuid, SWITCH_CAUSE_ORIGINATOR_CANCEL, (switch_hup_type_t) (SHT_UNANSWERED | SHT_ANSWERED));  //hmeng

		}
		t_agent_answered = local_epoch_time_now(NULL);

		if (switch_event_create_subclass(&event, SWITCH_EVENT_CUSTOM, CALLCENTER_EVENT) == SWITCH_STATUS_SUCCESS) {
			switch_caller_profile_t *member_profile = switch_channel_get_caller_profile(member_channel);
			const char *member_dnis = member_profile->rdnis;

			switch_channel_event_set_data(agent_channel, event);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Queue", h->queue_name);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Action", "bridge-agent-start");
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent", h->agent_name);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent-System", h->agent_system);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent-UUID", agent_uuid);
			switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Agent-Called-Time", "%" SWITCH_TIME_T_FMT, t_agent_called);
			switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Agent-Answered-Time", "%" SWITCH_TIME_T_FMT, t_agent_answered);
			switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Member-Joined-Time", "%" SWITCH_TIME_T_FMT, t_member_called);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-UUID", h->member_uuid);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-Session-UUID", h->member_session_uuid);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Name", h->member_cid_name);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Number", h->member_cid_number);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-DNIS", member_dnis);
			switch_event_fire(&event);
		}
		/* for xml_cdr needs */
		switch_channel_set_variable(member_channel, "cc_agent", h->agent_name);
		switch_channel_set_variable_printf(member_channel, "cc_queue_answered_epoch", "%" SWITCH_TIME_T_FMT, local_epoch_time_now(NULL)); 

		/* Set UUID of the Agent channel */
		sql = switch_mprintf("UPDATE agents SET uuid = '%q', last_bridge_start = '%" SWITCH_TIME_T_FMT "', calls_answered = calls_answered + 1, no_answer_count = 0"
				" WHERE name = '%q' AND system = '%q'",
				agent_uuid, local_epoch_time_now(NULL),
				h->agent_name, h->agent_system);
		cc_execute_sql(sql, NULL);
		switch_safe_free(sql);

		/* Change the agents Status in the tiers */
		cc_tier_update("state", cc_tier_state2str(CC_TIER_STATE_ACTIVE_INBOUND), h->queue_name, h->agent_name);
		cc_agent_update("state", cc_agent_state2str(CC_AGENT_STATE_IN_A_QUEUE_CALL), h->agent_name);

		/* Record session if record-template is provided */
		if (h->record_template) {
			char *expanded = switch_channel_expand_variables(member_channel, h->record_template);
			switch_channel_set_variable(member_channel, "cc_record_filename", expanded);
			switch_ivr_record_session(member_session, expanded, 0, NULL);
			if (expanded != h->record_template) {
				switch_safe_free(expanded);
			}					
		}

		switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_DEBUG, "Agent %s answered \"%s\" <%s> from queue %s%s\n",
				h->agent_name, h->member_cid_name, h->member_cid_number, h->queue_name, (h->record_template?" (Recorded)":""));

		if ((o_announce = switch_channel_get_variable(member_channel, "cc_outbound_announce"))) {
			playback_array(agent_session, o_announce);
		}

		switch_ivr_uuid_bridge(h->member_session_uuid, switch_core_session_get_uuid(agent_session));

		switch_channel_set_variable(member_channel, "cc_agent_uuid", agent_uuid);

		/* This is used for the waiting caller to quit waiting for a agent */
		switch_channel_set_variable(member_channel, "cc_agent_found", "true");

		/* Wait until the member hangup or the agent hangup.  This will quit also if the agent transfer the call */
		while(switch_channel_up(member_channel) && switch_channel_up(agent_channel) && globals.running) {
			switch_yield(100000);
		}
		tiers_state = CC_TIER_STATE_READY;

		if (switch_event_create_subclass(&event, SWITCH_EVENT_CUSTOM, CALLCENTER_EVENT) == SWITCH_STATUS_SUCCESS) {
			switch_channel_event_set_data(agent_channel, event);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Queue", h->queue_name);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Action", "bridge-agent-end");
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Hangup-Cause", switch_channel_cause2str(cause));
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent", h->agent_name);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent-System", h->agent_system);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent-UUID", agent_uuid);
			switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Agent-Called-Time", "%" SWITCH_TIME_T_FMT, t_agent_called);
			switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Agent-Answered-Time", "%" SWITCH_TIME_T_FMT, t_agent_answered);
			switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Member-Joined-Time", "%" SWITCH_TIME_T_FMT,  t_member_called);
			switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Bridge-Terminated-Time", "%" SWITCH_TIME_T_FMT, local_epoch_time_now(NULL));
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-UUID", h->member_uuid);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-Session-UUID", h->member_session_uuid);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Name", h->member_cid_name);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Number", h->member_cid_number);
			switch_event_fire(&event);
		}
		/* for xml_cdr needs */
		switch_channel_set_variable_printf(member_channel, "cc_queue_terminated_epoch", "%" SWITCH_TIME_T_FMT, local_epoch_time_now(NULL));

		/* Update Agents Items */
		/* Do not remove uuid of the agent if we are a standby agent */
		sql = switch_mprintf("UPDATE agents SET %s last_bridge_end = %" SWITCH_TIME_T_FMT ", talk_time = talk_time + (%" SWITCH_TIME_T_FMT "-last_bridge_start) WHERE name = '%q' AND system = '%q';"
				, (strcasecmp(h->agent_type, CC_AGENT_TYPE_UUID_STANDBY)?"uuid = '',":""), local_epoch_time_now(NULL), local_epoch_time_now(NULL), h->agent_name, h->agent_system);
		cc_execute_sql(sql, NULL);
		switch_safe_free(sql);

		/* Remove the member entry from the db (Could become optional to support latter processing) */
		sql = switch_mprintf("DELETE FROM members WHERE system = 'single_box' AND uuid = '%q'", h->member_uuid);
		cc_execute_sql(sql, NULL);
		switch_safe_free(sql);

		/* Caller off event */
		if (switch_event_create_subclass(&event, SWITCH_EVENT_CUSTOM, CALLCENTER_EVENT) == SWITCH_STATUS_SUCCESS) {
			switch_channel_event_set_data(member_channel, event);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Queue", h->queue_name);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Action", "member-queue-end");
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Hangup-Cause", switch_channel_cause2str(cause));
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Cause", "Terminated");
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent", h->agent_name);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent-System", h->agent_system);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent-UUID", agent_uuid);
			switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Agent-Called-Time", "%" SWITCH_TIME_T_FMT, t_agent_called);
			switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Agent-Answered-Time", "%" SWITCH_TIME_T_FMT, t_agent_answered);
			switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Member-Leaving-Time", "%" SWITCH_TIME_T_FMT,  local_epoch_time_now(NULL));
			switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Member-Joined-Time", "%" SWITCH_TIME_T_FMT, t_member_called);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-UUID", h->member_uuid);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-Session-UUID", h->member_session_uuid);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Name", h->member_cid_name);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Number", h->member_cid_number);
			switch_event_fire(&event);
		}


	} else {
		/* Agent didn't answer or originate failed */
		int delay_next_agent_call = 0;
		sql = switch_mprintf("UPDATE members SET state = '%q', serving_agent = '', serving_system = ''"
				" WHERE serving_agent = '%q' AND serving_system = '%q' AND uuid = '%q' AND system = 'single_box'",
				cc_member_state2str(CC_MEMBER_STATE_WAITING),
				h->agent_name, h->agent_system, h->member_uuid);
		cc_execute_sql(sql, NULL);
		switch_safe_free(sql);

		switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_DEBUG, "Agent %s Origination Canceled : %s\n", h->agent_name, switch_channel_cause2str(cause));

		switch (cause) {
			/* When we hang-up agents that did not answer in ring-all strategy */
			case SWITCH_CAUSE_ORIGINATOR_CANCEL:
				break;
			/* Busy: Do Not Disturb, Circuit congestion */
			case SWITCH_CAUSE_NORMAL_CIRCUIT_CONGESTION:
			case SWITCH_CAUSE_USER_BUSY:
				delay_next_agent_call = (h->busy_delay_time > delay_next_agent_call? h->busy_delay_time : delay_next_agent_call);
				break;
			/* Reject: User rejected the call */
			case SWITCH_CAUSE_CALL_REJECTED:
				delay_next_agent_call = (h->reject_delay_time > delay_next_agent_call? h->reject_delay_time : delay_next_agent_call);
				break;
			/* Protection againts super fast loop due to unregistrer */			
			case SWITCH_CAUSE_USER_NOT_REGISTERED:
				delay_next_agent_call = 5;
				break;
			/* No answer: Destination does not answer for some other reason */
			default:
				delay_next_agent_call = (h->no_answer_delay_time > delay_next_agent_call? h->no_answer_delay_time : delay_next_agent_call);

				tiers_state = CC_TIER_STATE_NO_ANSWER;

				/* Update Agent NO Answer count */
				sql = switch_mprintf("UPDATE agents SET no_answer_count = no_answer_count + 1 WHERE name = '%q' AND system = '%q';",
						h->agent_name, h->agent_system);
				cc_execute_sql(sql, NULL);
				switch_safe_free(sql);

				/* Put Agent on break because he didn't answer often */
				if (h->max_no_answer > 0 && (h->no_answer_count + 1) >= h->max_no_answer) {
					switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_DEBUG, "Agent %s reach maximum no answer of %d, Putting agent on break\n",
							h->agent_name, h->max_no_answer);
					cc_agent_update("status", cc_agent_status2str(CC_AGENT_STATUS_ON_BREAK), h->agent_name);
				}
				break;
		}

		/* Put agent to sleep for some time if necessary */
		if (delay_next_agent_call > 0) {
			char ready_epoch[64];
			switch_snprintf(ready_epoch, sizeof(ready_epoch), "%" SWITCH_TIME_T_FMT, local_epoch_time_now(NULL) + delay_next_agent_call);
			cc_agent_update("ready_time", ready_epoch , h->agent_name);
			switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_DEBUG, "Agent %s sleeping for %d seconds\n", h->agent_name, delay_next_agent_call);
		}

		/* Fire up event when contact agent fails */
		if (switch_event_create_subclass(&event, SWITCH_EVENT_CUSTOM, CALLCENTER_EVENT) == SWITCH_STATUS_SUCCESS) {
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Queue", h->queue_name);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Action", "bridge-agent-fail");
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Hangup-Cause", switch_channel_cause2str(cause));
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent", h->agent_name);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent-System", h->agent_system);
			switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Agent-Called-Time", "%" SWITCH_TIME_T_FMT, t_agent_called);
			switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Agent-Aborted-Time", "%" SWITCH_TIME_T_FMT, local_epoch_time_now(NULL));
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-UUID", h->member_uuid);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-Session-UUID", h->member_session_uuid);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Name", h->member_cid_name);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Number", h->member_cid_number);
			switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Member-Joined-Time", "%" SWITCH_TIME_T_FMT, t_member_called);
			switch_event_fire(&event);
		}

	}

done:
	/* Make Agent Available Again */
	sql = switch_mprintf(
			"UPDATE tiers SET state = '%q' WHERE agent = '%q' AND queue = '%q' AND (state = '%q' OR state = '%q' OR state = '%q');"
			"UPDATE tiers SET state = '%q' WHERE agent = '%q' AND NOT queue = '%q' AND state = '%q'"
			, cc_tier_state2str(tiers_state), h->agent_name, h->queue_name, cc_tier_state2str(CC_TIER_STATE_ACTIVE_INBOUND), cc_tier_state2str(CC_TIER_STATE_STANDBY), cc_tier_state2str(CC_TIER_STATE_OFFERING),
			cc_tier_state2str(CC_TIER_STATE_READY), h->agent_name, h->queue_name, cc_tier_state2str(CC_TIER_STATE_STANDBY));
	cc_execute_sql(sql, NULL);
	switch_safe_free(sql);

	/* If we are in Status Available On Demand, set state to Idle so we do not receive another call until state manually changed to Waiting */
	if (!strcasecmp(cc_agent_status2str(CC_AGENT_STATUS_AVAILABLE_ON_DEMAND), h->agent_status)) {
		cc_agent_update("state", cc_agent_state2str(CC_AGENT_STATE_IDLE), h->agent_name);
	} else {
		cc_agent_update("state", cc_agent_state2str(CC_AGENT_STATE_WAITING), h->agent_name);
	}

	if (agent_session) {
		switch_core_session_rwunlock(agent_session);
	}
	if (member_session) {
		switch_core_session_rwunlock(member_session);
	}

	switch_core_destroy_memory_pool(&h->pool);

	switch_mutex_lock(globals.mutex);
	globals.threads--;
	switch_mutex_unlock(globals.mutex);

	return NULL;
}

struct agent_callback {
	const char *queue_name;
	const char *system;
	const char *member_uuid;
	const char *member_session_uuid;
	const char *member_cid_number;
	const char *member_cid_name;
	const char *member_joined_epoch;
	const char *member_score;
	const char *strategy;
	const char *record_template;
	switch_bool_t tier_rules_apply;
	uint32_t tier_rule_wait_second;
	switch_bool_t tier_rule_wait_multiply_level;
	switch_bool_t tier_rule_no_agent_no_wait;
	switch_bool_t agent_found;

	int tier;
	int tier_agent_available;
};
typedef struct agent_callback agent_callback_t;

static int agents_callback(void *pArg, int argc, char **argv, char **columnNames)
{
	agent_callback_t *cbt = (agent_callback_t *) pArg;
	char *sql = NULL;
	char res[256];
	const char *agent_system = argv[0];
	const char *agent_name = argv[1];
	const char *agent_status = argv[2];
	const char *agent_originate_string = argv[3];
	const char *agent_no_answer_count = argv[4];
	const char *agent_max_no_answer = argv[5];
	const char *agent_reject_delay_time = argv[6];
	const char *agent_busy_delay_time = argv[7];
	const char *agent_no_answer_delay_time = argv[8];
	const char *agent_tier_state = argv[9];
	const char *agent_last_bridge_end = argv[10];
	const char *agent_wrap_up_time = argv[11];
	const char *agent_state = argv[12];
	const char *agent_ready_time = argv[13];
	const char *agent_tier_position = argv[14];
	const char *agent_tier_level = argv[15];
	const char *agent_type = argv[16];
	const char *agent_uuid = argv[17];

	switch_bool_t contact_agent = SWITCH_TRUE;

	cbt->agent_found = SWITCH_TRUE;

	/* Check if we switch to a different tier, if so, check if we should continue further for that member */

	if (cbt->tier_rules_apply == SWITCH_TRUE && atoi(agent_tier_level) > cbt->tier) {
		/* Continue if no agent was logged in in the previous tier and noagent = true */
		if (cbt->tier_rule_no_agent_no_wait == SWITCH_TRUE && cbt->tier_agent_available == 0) {
			cbt->tier = atoi(agent_tier_level);
			/* Multiple the tier level by the tier wait time */
		} else if (cbt->tier_rule_wait_multiply_level == SWITCH_TRUE && (long) local_epoch_time_now(NULL) - atol(cbt->member_joined_epoch) >= atoi(agent_tier_level) * cbt->tier_rule_wait_second) {
			cbt->tier = atoi(agent_tier_level);
			cbt->tier_agent_available = 0;
			/* Just check if joined is bigger than next tier wait time */
		} else if (cbt->tier_rule_wait_multiply_level == SWITCH_FALSE && (long) local_epoch_time_now(NULL) - atol(cbt->member_joined_epoch) >= cbt->tier_rule_wait_second) {
			cbt->tier = atoi(agent_tier_level);
			cbt->tier_agent_available = 0;
		} else {
			/* We are not allowed to continue to the next tier of agent */
			return 1;
		}
	}
	cbt->tier_agent_available++;

	/* If Agent is not in a acceptable tier state, continue */
	if (! (!strcasecmp(agent_tier_state, cc_tier_state2str(CC_TIER_STATE_NO_ANSWER)) || !strcasecmp(agent_tier_state, cc_tier_state2str(CC_TIER_STATE_READY)))) {
		contact_agent = SWITCH_FALSE;
	}
	if (! (!strcasecmp(agent_state, cc_agent_state2str(CC_AGENT_STATE_WAITING)))) {
		contact_agent = SWITCH_FALSE;
	}
	if (! (atol(agent_last_bridge_end) < ((long) local_epoch_time_now(NULL) - atol(agent_wrap_up_time)))) {
		contact_agent = SWITCH_FALSE;
	}
	if (! (atol(agent_ready_time) <= (long) local_epoch_time_now(NULL))) {
		contact_agent = SWITCH_FALSE;
	}
	if (! (strcasecmp(agent_status, cc_agent_status2str(CC_AGENT_STATUS_ON_BREAK)))) {
		contact_agent = SWITCH_FALSE;
	}

	if (contact_agent == SWITCH_FALSE) {
		return 0; /* Continue to next Agent */
	}

	/* If agent isn't on this box */
	if (strcasecmp(agent_system,"single_box" /* SELF */)) {
		if (!strcasecmp(cbt->strategy, "ring-all")) {
			return 1; /* Abort finding agent for member if we found a match but for a different Server */
		} else {
			return 0; /* Skip this Agents only, so we can ring the other one */
		}
	}

	if (!strcasecmp(cbt->strategy,"ring-all")) {
		/* Check if member is a ring-all mode */
		sql = switch_mprintf("SELECT count(*) FROM members WHERE serving_agent = 'ring-all' AND uuid = '%q' AND system = 'single_box'", cbt->member_uuid);
		cc_execute_sql2str(NULL, sql, res, sizeof(res));

		switch_safe_free(sql);
	} else {
		/* Map the Agent to the member */
		sql = switch_mprintf("UPDATE members SET serving_agent = '%q', serving_system = 'single_box', state = '%q'"
				" WHERE state = '%q' AND uuid = '%q' AND system = 'single_box'", 
				agent_name, cc_member_state2str(CC_MEMBER_STATE_TRYING),
				cc_member_state2str(CC_MEMBER_STATE_WAITING), cbt->member_uuid);
		cc_execute_sql(sql, NULL);
		switch_safe_free(sql);

		/* Check if we won the race to get the member to our selected agent (Used for Multi system purposes) */
		sql = switch_mprintf("SELECT count(*) FROM members WHERE serving_agent = '%q' AND serving_system = 'single_box' AND uuid = '%q' AND system = 'single_box'",
				agent_name, cbt->member_uuid);
		cc_execute_sql2str(NULL, sql, res, sizeof(res));
		switch_safe_free(sql);
	}

	switch (atoi(res)) {
		case 0: /* Ok, someone else took it, or user hanged up already */
			return 1;
			/* We default to default even if more entry is returned... Should never happen	anyway */
		default: /* Go ahead, start thread to try to bridge these 2 caller */
			{
				switch_thread_t *thread;
				switch_threadattr_t *thd_attr = NULL;
				switch_memory_pool_t *pool;
				struct call_helper *h;

				switch_core_new_memory_pool(&pool);
				h = (struct call_helper *)switch_core_alloc(pool, sizeof(*h));
				h->pool = pool;
				h->member_uuid = switch_core_strdup(h->pool, cbt->member_uuid);
				h->member_session_uuid = switch_core_strdup(h->pool, cbt->member_session_uuid);
				h->queue_strategy = switch_core_strdup(h->pool, cbt->strategy);
				h->originate_string = switch_core_strdup(h->pool, agent_originate_string);
				h->agent_name = switch_core_strdup(h->pool, agent_name);
				h->agent_system = switch_core_strdup(h->pool, "single_box");
				h->agent_status = switch_core_strdup(h->pool, agent_status);
				h->agent_type = switch_core_strdup(h->pool, agent_type);
				h->agent_uuid = switch_core_strdup(h->pool, agent_uuid);
				h->member_joined_epoch = switch_core_strdup(h->pool, cbt->member_joined_epoch); 
				h->member_cid_name = switch_core_strdup(h->pool, cbt->member_cid_name);
				h->member_cid_number = switch_core_strdup(h->pool, cbt->member_cid_number);
				h->queue_name = switch_core_strdup(h->pool, cbt->queue_name);
				h->record_template = switch_core_strdup(h->pool, cbt->record_template);
				h->no_answer_count = atoi(agent_no_answer_count);
				h->max_no_answer = atoi(agent_max_no_answer);
				h->reject_delay_time = atoi(agent_reject_delay_time);
				h->busy_delay_time = atoi(agent_busy_delay_time);
				h->no_answer_delay_time = atoi(agent_no_answer_delay_time);

				if (!strcasecmp(cbt->strategy, "top-down")) {
					switch_core_session_t *member_session = switch_core_session_locate(cbt->member_session_uuid);
					if (member_session) {
						switch_channel_t *member_channel = switch_core_session_get_channel(member_session);
						switch_channel_set_variable(member_channel, "cc_last_agent_tier_position", agent_tier_position);
						switch_channel_set_variable(member_channel, "cc_last_agent_tier_level", agent_tier_level);
						switch_core_session_rwunlock(member_session);
					}
				}
				cc_agent_update("state", cc_agent_state2str(CC_AGENT_STATE_RECEIVING), h->agent_name);

				sql = switch_mprintf(
						"UPDATE tiers SET state = '%q' WHERE agent = '%q' AND queue = '%q';"
						"UPDATE tiers SET state = '%q' WHERE agent = '%q' AND NOT queue = '%q' AND state = '%q';",
						cc_tier_state2str(CC_TIER_STATE_OFFERING), h->agent_name, h->queue_name,
						cc_tier_state2str(CC_TIER_STATE_STANDBY), h->agent_name, h->queue_name, cc_tier_state2str(CC_TIER_STATE_READY));
				cc_execute_sql(sql, NULL);
				switch_safe_free(sql);

				switch_threadattr_create(&thd_attr, h->pool);
				switch_threadattr_detach_set(thd_attr, 1);
				switch_threadattr_stacksize_set(thd_attr, SWITCH_THREAD_STACKSIZE);
				switch_thread_create(&thread, thd_attr, outbound_agent_thread_run, h, h->pool);
			}

			if (!strcasecmp(cbt->strategy,"ring-all")) {
				return 0;
			} else {
				return 1;
			}
	}

	return 0;
}

static int members_callback(void *pArg, int argc, char **argv, char **columnNames)
{
	cc_queue_t *queue = NULL;
	char *sql = NULL;
	char *sql_order_by = NULL;
	char *queue_name = NULL;
	char *queue_strategy = NULL;
	char *queue_record_template = NULL;
	switch_bool_t tier_rules_apply;
	uint32_t tier_rule_wait_second;
	switch_bool_t tier_rule_wait_multiply_level;
	switch_bool_t tier_rule_no_agent_no_wait;
	uint32_t discard_abandoned_after;
	agent_callback_t cbt;
	const char *member_state = NULL;
	const char *member_abandoned_epoch = NULL;
	memset(&cbt, 0, sizeof(cbt));

	cbt.queue_name = argv[0];
	cbt.member_uuid = argv[1];
	cbt.member_session_uuid = argv[2];
	cbt.member_cid_number = argv[3];
	cbt.member_cid_name = argv[4];
	cbt.member_joined_epoch = argv[5];
	cbt.member_score = argv[6];
	member_state = argv[7];
	member_abandoned_epoch = argv[8];

	if (!cbt.queue_name || !(queue = get_queue(cbt.queue_name))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Queue %s not found locally, skip this member\n", cbt.queue_name);
		goto end;
	} else {
		queue_name = strdup(queue->name);
		queue_strategy = strdup(queue->strategy);
		tier_rules_apply = queue->tier_rules_apply;
		tier_rule_wait_second = queue->tier_rule_wait_second;
		tier_rule_wait_multiply_level = queue->tier_rule_wait_multiply_level;
		tier_rule_no_agent_no_wait = queue->tier_rule_no_agent_no_wait;
		discard_abandoned_after = queue->discard_abandoned_after;

		if (queue->record_template) {
			queue_record_template = strdup(queue->record_template);
		}

		queue_rwunlock(queue);
	}

	/* Checking for cleanup Abandonded calls from the db */
	if (!strcasecmp(member_state, cc_member_state2str(CC_MEMBER_STATE_ABANDONED))) {
		switch_time_t abandoned_epoch = atoll(member_abandoned_epoch);
		if (abandoned_epoch == 0) {
			abandoned_epoch = atoll(cbt.member_joined_epoch);
		}
		/* Once we pass a certain point, we want to get rid of the abandoned call */
		if (abandoned_epoch + discard_abandoned_after < local_epoch_time_now(NULL)) {
			sql = switch_mprintf("DELETE FROM members WHERE system = 'single_box' AND uuid = '%q' AND (abandoned_epoch = '%" SWITCH_TIME_T_FMT "' OR joined_epoch = '%q')", cbt.member_uuid, abandoned_epoch, cbt.member_joined_epoch);
			cc_execute_sql(sql, NULL);
			switch_safe_free(sql);
		}
		/* Skip this member */
		goto end;
	} 

	/* Check if member is in the queue waiting */
	if (zstr(cbt.member_session_uuid)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Member %s <%s> in Queue %s have no session uuid, skip this member\n", cbt.member_cid_name, cbt.member_cid_number, cbt.queue_name);
	}

	cbt.tier = 0;
	cbt.tier_agent_available = 0;

	cbt.tier_rules_apply = tier_rules_apply;
	cbt.tier_rule_wait_second = tier_rule_wait_second;
	cbt.tier_rule_wait_multiply_level = tier_rule_wait_multiply_level;
	cbt.tier_rule_no_agent_no_wait = tier_rule_no_agent_no_wait;

	cbt.strategy = queue_strategy;
	cbt.record_template = queue_record_template;
	cbt.agent_found = SWITCH_FALSE;

	if (!strcasecmp(queue->strategy, "top-down")) {
		/* WARNING this use channel variable to help dispatch... might need to be reviewed to save it in DB to make this multi server prooft in the future */
		switch_core_session_t *member_session = switch_core_session_locate(cbt.member_session_uuid);
		int position = 0, level = 0;
		const char *last_agent_tier_position, *last_agent_tier_level;
		if (member_session) {
			switch_channel_t *member_channel = switch_core_session_get_channel(member_session);

			if ((last_agent_tier_position = switch_channel_get_variable(member_channel, "cc_last_agent_tier_position"))) {
				position = atoi(last_agent_tier_position);
			}
			if ((last_agent_tier_level = switch_channel_get_variable(member_channel, "cc_last_agent_tier_level"))) {
				level = atoi(last_agent_tier_level);
			}
			switch_core_session_rwunlock(member_session);
		}

		sql = switch_mprintf("SELECT system, name, status, contact, no_answer_count, max_no_answer, reject_delay_time, busy_delay_time, no_answer_delay_time, tiers.state, agents.last_bridge_end, agents.wrap_up_time, agents.state, agents.ready_time, tiers.position as tiers_position, tiers.level as tiers_level, agents.type, agents.uuid, agents.last_offered_call as agents_last_offered_call, 1 as dyn_order FROM agents LEFT JOIN tiers ON (agents.name = tiers.agent)"
				" WHERE tiers.queue = '%q'"
				" AND (agents.status = '%q' OR agents.status = '%q' OR agents.status = '%q')"
				" AND tiers.position > %d"
				" AND tiers.level = %d"
				" UNION "
				"SELECT system, name, status, contact, no_answer_count, max_no_answer, reject_delay_time, busy_delay_time, no_answer_delay_time, tiers.state, agents.last_bridge_end, agents.wrap_up_time, agents.state, agents.ready_time, tiers.position as tiers_position, tiers.level as tiers_level, agents.type, agents.uuid, agents.last_offered_call as agents_last_offered_call, 2 as dyn_order FROM agents LEFT JOIN tiers ON (agents.name = tiers.agent)"
				" WHERE tiers.queue = '%q'"
				" AND (agents.status = '%q' OR agents.status = '%q' OR agents.status = '%q')"
				" ORDER BY dyn_order asc, tiers_level, tiers_position, agents_last_offered_call",
				queue_name,
				cc_agent_status2str(CC_AGENT_STATUS_AVAILABLE), cc_agent_status2str(CC_AGENT_STATUS_ON_BREAK), cc_agent_status2str(CC_AGENT_STATUS_AVAILABLE_ON_DEMAND),
				position,
				level,
				queue_name,
				cc_agent_status2str(CC_AGENT_STATUS_AVAILABLE), cc_agent_status2str(CC_AGENT_STATUS_ON_BREAK), cc_agent_status2str(CC_AGENT_STATUS_AVAILABLE_ON_DEMAND)
				);
	} else if (!strcasecmp(queue->strategy, "round-robin")) {
		sql = switch_mprintf("SELECT system, name, status, contact, no_answer_count, max_no_answer, reject_delay_time, busy_delay_time, no_answer_delay_time, tiers.state, agents.last_bridge_end, agents.wrap_up_time, agents.state, agents.ready_time, tiers.position as tiers_position, tiers.level as tiers_level, agents.type, agents.uuid, agents.last_offered_call as agents_last_offered_call, 1 as dyn_order FROM agents LEFT JOIN tiers ON (agents.name = tiers.agent)"
				" WHERE tiers.queue = '%q'"
				" AND (agents.status = '%q' OR agents.status = '%q' OR agents.status = '%q')"
				" AND tiers.position > (SELECT tiers.position FROM agents LEFT JOIN tiers ON (agents.name = tiers.agent) WHERE tiers.queue = '%q' AND agents.last_offered_call > 0 ORDER BY agents.last_offered_call DESC LIMIT 1)"
				" AND tiers.level = (SELECT tiers.level FROM agents LEFT JOIN tiers ON (agents.name = tiers.agent) WHERE tiers.queue = '%q' AND agents.last_offered_call > 0 ORDER BY agents.last_offered_call DESC LIMIT 1)"
				" UNION "
				"SELECT system, name, status, contact, no_answer_count, max_no_answer, reject_delay_time, busy_delay_time, no_answer_delay_time, tiers.state, agents.last_bridge_end, agents.wrap_up_time, agents.state, agents.ready_time, tiers.position as tiers_position, tiers.level as tiers_level, agents.type, agents.uuid, agents.last_offered_call as agents_last_offered_call, 2 as dyn_order FROM agents LEFT JOIN tiers ON (agents.name = tiers.agent)"
				" WHERE tiers.queue = '%q'"
				" AND (agents.status = '%q' OR agents.status = '%q' OR agents.status = '%q')"
				" ORDER BY dyn_order asc, tiers_level, tiers_position, agents_last_offered_call",
				queue_name,
				cc_agent_status2str(CC_AGENT_STATUS_AVAILABLE), cc_agent_status2str(CC_AGENT_STATUS_ON_BREAK), cc_agent_status2str(CC_AGENT_STATUS_AVAILABLE_ON_DEMAND),
				queue_name,
				queue_name,
				queue_name,
				cc_agent_status2str(CC_AGENT_STATUS_AVAILABLE), cc_agent_status2str(CC_AGENT_STATUS_ON_BREAK), cc_agent_status2str(CC_AGENT_STATUS_AVAILABLE_ON_DEMAND)
				);

	} else {

		if (!strcasecmp(queue->strategy, "longest-idle-agent")) {
			sql_order_by = switch_mprintf("level, agents.last_offered_call, position");
		} else if (!strcasecmp(queue_strategy, "agent-with-least-talk-time")) {
			sql_order_by = switch_mprintf("level, agents.talk_time, position");
		} else if (!strcasecmp(queue_strategy, "agent-with-fewest-calls")) {
			sql_order_by = switch_mprintf("level, agents.calls_answered, position");
		} else if (!strcasecmp(queue_strategy, "ring-all")) {
			sql = switch_mprintf("UPDATE members SET state = '%q' WHERE state = '%q' AND uuid = '%q' AND system = 'single_box'",
					cc_member_state2str(CC_MEMBER_STATE_TRYING), cc_member_state2str(CC_MEMBER_STATE_WAITING), cbt.member_uuid);
			cc_execute_sql(sql, NULL);
			switch_safe_free(sql);
			sql_order_by = switch_mprintf("level, position");
		} else if(!strcasecmp(queue_strategy, "random")) {
			sql_order_by = switch_mprintf("level, random()");
		} else if(!strcasecmp(queue_strategy, "sequentially-by-agent-order")) {
			sql_order_by = switch_mprintf("level, position, agents.last_offered_call"); /* Default to last_offered_call, let add new strategy if needing it differently */
		} else {
			/* If the strategy doesn't exist, just fallback to the following */
			sql_order_by = switch_mprintf("level, position, agents.last_offered_call");
		}

		sql = switch_mprintf("SELECT system, name, status, contact, no_answer_count, max_no_answer, reject_delay_time, busy_delay_time, no_answer_delay_time, tiers.state, agents.last_bridge_end, agents.wrap_up_time, agents.state, agents.ready_time, tiers.position, tiers.level, agents.type, agents.uuid FROM agents LEFT JOIN tiers ON (agents.name = tiers.agent)"
				" WHERE tiers.queue = '%q'"
				" AND (agents.status = '%q' OR agents.status = '%q' OR agents.status = '%q')"
				" ORDER BY %q",
				queue_name,
				cc_agent_status2str(CC_AGENT_STATUS_AVAILABLE), cc_agent_status2str(CC_AGENT_STATUS_ON_BREAK), cc_agent_status2str(CC_AGENT_STATUS_AVAILABLE_ON_DEMAND),
				sql_order_by);
		switch_safe_free(sql_order_by);

	}

	cc_execute_sql_callback(NULL, sql, agents_callback, &cbt /* Call back variables */);

	switch_safe_free(sql);

	/* We update a field in the queue struct so we can kick caller out if waiting for too long with no agent */
	if (!cbt.queue_name || !(queue = get_queue(cbt.queue_name))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Queue %s not found locally, skip this member\n", cbt.queue_name);
		goto end;
	} else {
		queue->last_agent_exist_check = local_epoch_time_now(NULL);
		if (cbt.agent_found) {
			queue->last_agent_exist = queue->last_agent_exist_check;
		}
		queue_rwunlock(queue);
	}

end:
	switch_safe_free(queue_name);
	switch_safe_free(queue_strategy);
	switch_safe_free(queue_record_template);

	return 0;
}

static int AGENT_DISPATCH_THREAD_RUNNING = 0;
static int AGENT_DISPATCH_THREAD_STARTED = 0;

void *SWITCH_THREAD_FUNC cc_agent_dispatch_thread_run(switch_thread_t *thread, void *obj)
{
	int done = 0;

	switch_mutex_lock(globals.mutex);
	if (!AGENT_DISPATCH_THREAD_RUNNING) {
		AGENT_DISPATCH_THREAD_RUNNING++;
		globals.threads++;
	} else {
		done = 1;
	}
	switch_mutex_unlock(globals.mutex);

	if (done) {
		return NULL;
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CONSOLE, "Agent Dispatch Thread Started\n");

	while (globals.running == 1) {
		char *sql = NULL;
		sql = switch_mprintf("SELECT queue,uuid,session_uuid,cid_number,cid_name,joined_epoch,(%" SWITCH_TIME_T_FMT "-joined_epoch)+base_score+skill_score AS score, state, abandoned_epoch FROM members"
				" WHERE state = '%q' OR state = '%q' OR (serving_agent = 'ring-all' AND state = '%q') ORDER BY score DESC",
				local_epoch_time_now(NULL),
				cc_member_state2str(CC_MEMBER_STATE_WAITING), cc_member_state2str(CC_MEMBER_STATE_ABANDONED), cc_member_state2str(CC_MEMBER_STATE_TRYING));

		cc_execute_sql_callback(NULL, sql, members_callback, NULL /* Call back variables */);
		switch_safe_free(sql);
		switch_yield(100000);
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CONSOLE, "Agent Dispatch Thread Ended\n");

	switch_mutex_lock(globals.mutex);
	globals.threads--;
	AGENT_DISPATCH_THREAD_RUNNING = AGENT_DISPATCH_THREAD_STARTED = 0;
	switch_mutex_unlock(globals.mutex);

	return NULL;
}


void cc_agent_dispatch_thread_start(void)
{
	switch_thread_t *thread;
	switch_threadattr_t *thd_attr = NULL;
	int done = 0;

	switch_mutex_lock(globals.mutex);

	if (!AGENT_DISPATCH_THREAD_STARTED) {
		AGENT_DISPATCH_THREAD_STARTED++;
	} else {
		done = 1;
	}
	switch_mutex_unlock(globals.mutex);

	if (done) {
		return;
	}

	switch_threadattr_create(&thd_attr, globals.pool);
	switch_threadattr_detach_set(thd_attr, 1);
	switch_threadattr_stacksize_set(thd_attr, SWITCH_THREAD_STACKSIZE);
	switch_threadattr_priority_set(thd_attr, SWITCH_PRI_REALTIME);
	switch_thread_create(&thread, thd_attr, cc_agent_dispatch_thread_run, NULL, globals.pool);
}

struct member_thread_helper {
	const char *queue_name;
	const char *member_uuid;
	const char *member_session_uuid;
	const char *member_cid_name;
	const char *member_cid_number;
	switch_time_t t_member_called;
	cc_member_cancel_reason_t member_cancel_reason;

	int running;
	switch_memory_pool_t *pool;
};

void *SWITCH_THREAD_FUNC cc_member_thread_run(switch_thread_t *thread, void *obj)
{
	struct member_thread_helper *m = (struct member_thread_helper *) obj;
	switch_core_session_t *member_session = switch_core_session_locate(m->member_session_uuid);
	switch_channel_t *member_channel = NULL;

	if (member_session) {
		member_channel = switch_core_session_get_channel(member_session);
	} else {
		switch_core_destroy_memory_pool(&m->pool);
		return NULL;
	}

	switch_mutex_lock(globals.mutex);
	globals.threads++;
	switch_mutex_unlock(globals.mutex);

	while(switch_channel_ready(member_channel) && m->running && globals.running) {
		cc_queue_t *queue = NULL;

		if (!m->queue_name || !(queue = get_queue(m->queue_name))) {
			switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_WARNING, "Queue %s not found\n", m->queue_name);
			break;
		}
		/* Make the Caller Leave if he went over his max wait time */
		if (queue->max_wait_time > 0 && queue->max_wait_time <= local_epoch_time_now(NULL) - m->t_member_called) {
			switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_DEBUG, "Member %s <%s> in queue '%s' reached max wait time\n", m->member_cid_name, m->member_cid_number, m->queue_name);
			m->member_cancel_reason = CC_MEMBER_CANCEL_REASON_TIMEOUT;
			switch_channel_set_flag_value(member_channel, CF_BREAK, 2);
		}

		if (queue->max_wait_time_with_no_agent > 0 && queue->last_agent_exist_check > queue->last_agent_exist) {
			if (queue->last_agent_exist_check - queue->last_agent_exist >= queue->max_wait_time_with_no_agent) {
				if (queue->max_wait_time_with_no_agent_time_reached > 0) {
					if (queue->last_agent_exist_check - m->t_member_called >= queue->max_wait_time_with_no_agent + queue->max_wait_time_with_no_agent_time_reached) {
						switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_DEBUG, "Member %s <%s> in queue '%s' reached max wait of %d sec. with no agent plus join grace period of %d sec.\n", m->member_cid_name, m->member_cid_number, m->queue_name, queue->max_wait_time_with_no_agent, queue->max_wait_time_with_no_agent_time_reached);
						m->member_cancel_reason = CC_MEMBER_CANCEL_REASON_NO_AGENT_TIMEOUT;
						switch_channel_set_flag_value(member_channel, CF_BREAK, 2);

					}
				} else {
					switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_DEBUG, "Member %s <%s> in queue '%s' reached max wait of %d sec. with no agent\n", m->member_cid_name, m->member_cid_number, m->queue_name, queue->max_wait_time_with_no_agent);
					m->member_cancel_reason = CC_MEMBER_CANCEL_REASON_NO_AGENT_TIMEOUT;
					switch_channel_set_flag_value(member_channel, CF_BREAK, 2);

				} 
			}
		}

		/* TODO Go thought the list of phrases */
		/* SAMPLE CODE to playback something over the MOH

		   switch_event_t *event;
		   if (switch_event_create(&event, SWITCH_EVENT_COMMAND) == SWITCH_STATUS_SUCCESS) {
		   switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "call-command", "execute");
		   switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "execute-app-name", "playback");
		   switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "execute-app-arg", "tone_stream://%(200,0,500,600,700)");
		   switch_core_session_queue_private_event(member_session, &event, SWITCH_TRUE);
		   }
		 */

		/* If Agent Logoff, we might need to recalculare score based on skill */
		/* Play Announcement in order */

		queue_rwunlock(queue);

		switch_yield(500000);
	}

	switch_core_session_rwunlock(member_session);
	switch_core_destroy_memory_pool(&m->pool);

	switch_mutex_lock(globals.mutex);
	globals.threads--;
	switch_mutex_unlock(globals.mutex);

	return NULL; 
}

struct moh_dtmf_helper {
	const char *queue_name;
	const char *exit_keys;
	char dtmf;
};

static switch_status_t moh_on_dtmf(switch_core_session_t *session, void *input, switch_input_type_t itype, void *buf, unsigned int buflen) {
	struct moh_dtmf_helper *h = (struct moh_dtmf_helper *) buf;

	switch (itype) {
		case SWITCH_INPUT_TYPE_DTMF:
			if (h->exit_keys && *(h->exit_keys)) {
				switch_dtmf_t *dtmf = (switch_dtmf_t *) input;
				if (strchr(h->exit_keys, dtmf->digit)) {
					h->dtmf = dtmf->digit;
					return SWITCH_STATUS_BREAK;
				}
			}
			break;
		default:
			break;
	}

	return SWITCH_STATUS_SUCCESS;
}

#define CC_DESC "callcenter"
#define CC_USAGE "queue_name"

SWITCH_STANDARD_APP(callcenter_function)
{
	char *argv[6] = { 0 };
	char *mydata = NULL;
	cc_queue_t *queue = NULL;
	const char *queue_name = NULL;
	switch_core_session_t *member_session = session;
	switch_channel_t *member_channel = switch_core_session_get_channel(member_session);
	char *sql = NULL;
	char *member_session_uuid = switch_core_session_get_uuid(member_session);
	struct member_thread_helper *h = NULL;
	switch_thread_t *thread;
	switch_threadattr_t *thd_attr = NULL;
	switch_memory_pool_t *pool;
	switch_channel_timetable_t *times = NULL;
	const char *cc_moh_override = switch_channel_get_variable(member_channel, "cc_moh_override");
	const char *cc_base_score = switch_channel_get_variable(member_channel, "cc_base_score");
	int cc_base_score_int = 0;
	const char *cur_moh = NULL;
	char start_epoch[64];
	switch_event_t *event;
	switch_time_t t_member_called = local_epoch_time_now(NULL);
	long abandoned_epoch = 0;
	switch_uuid_t smember_uuid;
	char member_uuid[SWITCH_UUID_FORMATTED_LENGTH + 1] = "";
	switch_bool_t agent_found = SWITCH_FALSE;
	switch_bool_t moh_valid = SWITCH_TRUE;
	const char *p;

	if (!zstr(data)) {
		mydata = switch_core_session_strdup(member_session, data);
		switch_separate_string(mydata, ' ', argv, (sizeof(argv) / sizeof(argv[0])));
	} else {
		switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_WARNING, "No Queue name provided\n");
		goto end;
	}

	if (argv[0]) {
		queue_name = argv[0];
	}

	if (!queue_name || !(queue = get_queue(queue_name))) {
		switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_WARNING, "Queue %s not found\n", queue_name);
		goto end;
	}

	/* Make sure we answer the channel before getting the switch_channel_time_table_t answer time */
	switch_channel_answer(member_channel);

	/* Grab the start epoch of a channel */
	times = switch_channel_get_timetable(member_channel);
	switch_snprintf(start_epoch, sizeof(start_epoch), "%" SWITCH_TIME_T_FMT, times->answered / 1000000);

	/* Check if we support and have a queued abandoned member we can resume from */
	if (queue->abandoned_resume_allowed == SWITCH_TRUE) {
		char res[256];

		/* Check to see if agent already exist */
		sql = switch_mprintf("SELECT uuid FROM members WHERE queue = '%q' AND cid_number = '%q' AND state = '%q' ORDER BY abandoned_epoch DESC",
				queue_name, switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_number")), cc_member_state2str(CC_MEMBER_STATE_ABANDONED));
		cc_execute_sql2str(NULL, sql, res, sizeof(res));
		switch_safe_free(sql);
		strncpy(member_uuid, res, sizeof(member_uuid));
		
		if (!zstr(member_uuid)) {
			sql = switch_mprintf("SELECT abandoned_epoch FROM members WHERE uuid = '%q'", member_uuid);
			cc_execute_sql2str(NULL, sql, res, sizeof(res));
			switch_safe_free(sql);
			abandoned_epoch = atol(res);
		}
	}

	/* If no existing uuid is restored, let create a new one */
	if (abandoned_epoch == 0) {
		switch_uuid_get(&smember_uuid);
		switch_uuid_format(member_uuid, &smember_uuid);
	}

	switch_channel_set_variable(member_channel, "cc_side", "member");
	switch_channel_set_variable(member_channel, "cc_member_uuid", member_uuid);

	/* Add manually imported score */
	if (cc_base_score) {
		cc_base_score_int += atoi(cc_base_score);
	}

	/* If system, will add the total time the session is up to the base score */
	if (!switch_strlen_zero(start_epoch) && !strcasecmp("system", queue->time_base_score)) {
		cc_base_score_int += ((long) local_epoch_time_now(NULL) - atol(start_epoch));
	}

	if (switch_event_create_subclass(&event, SWITCH_EVENT_CUSTOM, CALLCENTER_EVENT) == SWITCH_STATUS_SUCCESS) {
		switch_channel_event_set_data(member_channel, event);
		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Queue", queue_name);
		switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Action", "member-queue-%s", (abandoned_epoch==0?"start":"resume"));
		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-UUID", member_uuid);
		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-Session-UUID", member_session_uuid);
		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Name", switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_name")));
		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Number", switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_number")));
		switch_event_fire(&event);
	}
	/* for xml_cdr needs */
	switch_channel_set_variable_printf(member_channel, "cc_queue_joined_epoch", "%" SWITCH_TIME_T_FMT, local_epoch_time_now(NULL));
	switch_channel_set_variable(member_channel, "cc_queue", queue_name);

	if (abandoned_epoch == 0) {
		/* Add the caller to the member queue */
		switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_DEBUG, "Member %s <%s> joining queue %s\n", switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_name")), switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_number")), queue_name);

		sql = switch_mprintf("INSERT INTO members"
				" (queue,system,uuid,session_uuid,system_epoch,joined_epoch,base_score,skill_score,cid_number,cid_name,serving_agent,serving_system,state)"
				" VALUES('%q','single_box','%q','%q','%q','%" SWITCH_TIME_T_FMT "','%d','%d','%q','%q','%q','','%q')", 
				queue_name,
				member_uuid,
				member_session_uuid,
				start_epoch,
				local_epoch_time_now(NULL),
				cc_base_score_int,
				0 /*TODO SKILL score*/,
				switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_number")),
				switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_name")),
				(!strcasecmp(queue->strategy,"ring-all")?"ring-all":""),
				cc_member_state2str(CC_MEMBER_STATE_WAITING));
		cc_execute_sql(sql, NULL);
		switch_safe_free(sql);
	} else {
		char res[256];

		switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_DEBUG, "Member %s <%s> restoring it previous position in queue %s\n", switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_name")), switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_number")), queue_name);

		/* Update abandoned member */
		sql = switch_mprintf("UPDATE members SET session_uuid = '%q', state = '%q', rejoined_epoch = '%" SWITCH_TIME_T_FMT "' WHERE uuid = '%q' AND state = '%q'",
				member_session_uuid, cc_member_state2str(CC_MEMBER_STATE_WAITING), local_epoch_time_now(NULL), member_uuid, cc_member_state2str(CC_MEMBER_STATE_ABANDONED)); 
		cc_execute_sql(sql, NULL);
		switch_safe_free(sql);

		/* Confirm we took that member in */
		sql = switch_mprintf("SELECT abandoned_epoch FROM members WHERE uuid = '%q' AND session_uuid = '%q' AND state = '%q' AND queue = '%q'", member_uuid, member_session_uuid, cc_member_state2str(CC_MEMBER_STATE_WAITING), queue_name);
		cc_execute_sql2str(NULL, sql, res, sizeof(res));
		switch_safe_free(sql);

		if (atol(res) == 0) {
			/* Failed to get the member !!! */
			/* TODO Loop back to just create a uuid and add the member as a new member */
			switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_ERROR, "Member %s <%s> restoring action failed in queue %s, exiting\n", switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_name")), switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_number")), queue_name);
			queue_rwunlock(queue);
			goto end;
		}

	}

	/* Send Event with queue count */
	cc_queue_count(queue_name);

	/* Start Thread that will playback different prompt to the channel */
	switch_core_new_memory_pool(&pool);
	h = (struct member_thread_helper *)switch_core_alloc(pool, sizeof(*h));

	h->pool = pool;
	h->member_uuid = switch_core_strdup(h->pool, member_uuid);
	h->member_session_uuid = switch_core_strdup(h->pool, member_session_uuid);
	h->member_cid_name = switch_core_strdup(h->pool, switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_name")));
	h->member_cid_number = switch_core_strdup(h->pool, switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_number")));
	h->queue_name = switch_core_strdup(h->pool, queue_name);
	h->t_member_called = t_member_called;
	h->member_cancel_reason = CC_MEMBER_CANCEL_REASON_NONE;
	h->running = 1;

	switch_threadattr_create(&thd_attr, h->pool);
	switch_threadattr_detach_set(thd_attr, 1);
	switch_threadattr_stacksize_set(thd_attr, SWITCH_THREAD_STACKSIZE);
	switch_thread_create(&thread, thd_attr, cc_member_thread_run, h, h->pool);

	/* Playback MOH */
	if (cc_moh_override) {
		cur_moh = switch_core_session_strdup(member_session, cc_moh_override);
	} else {
		cur_moh = switch_core_session_strdup(member_session, queue->moh);
	}
	queue_rwunlock(queue);

	while (switch_channel_ready(member_channel)) {
		switch_input_args_t args = { 0 };
		struct moh_dtmf_helper ht;

		ht.exit_keys = switch_channel_get_variable(member_channel, "cc_exit_keys");
		ht.dtmf = '\0';
		args.input_callback = moh_on_dtmf;
		args.buf = (void *) &ht;
		args.buflen = sizeof(h);

		/* An agent was found, time to exit and let the bridge do it job */
		if ((p = switch_channel_get_variable(member_channel, "cc_agent_found")) && (agent_found = (switch_bool_t)switch_true(p))) {
			break;
		}
		/* If the member thread set a different reason, we monitor it so we can quit the wait */
		if (h->member_cancel_reason != CC_MEMBER_CANCEL_REASON_NONE) {
			break;
		}

		switch_core_session_flush_private_events(member_session);

		if (moh_valid && cur_moh) {
			switch_status_t status = switch_ivr_play_file(member_session, NULL, cur_moh, &args);

			if (status == SWITCH_STATUS_FALSE /* Invalid Recording */ && SWITCH_READ_ACCEPTABLE(status)) { 
				/* Sadly, there doesn't seem to be a return to switch_ivr_play_file that tell you the file wasn't found.  FALSE also mean that the channel got switch to BRAKE state, so we check for read acceptable */
				switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_WARNING, "Couldn't play file '%s', continuing wait with no audio\n", cur_moh);
				moh_valid = SWITCH_FALSE;
			} else if (status == SWITCH_STATUS_BREAK) {
				char buf[2] = { ht.dtmf, 0 };
				switch_channel_set_variable(member_channel, "cc_exit_key", buf);
				break;
			} else if (!SWITCH_READ_ACCEPTABLE(status)) {
				break;
			}
		} else {
			if ((switch_ivr_collect_digits_callback(member_session, &args, 0, 0)) == SWITCH_STATUS_BREAK) {
				char buf[2] = { ht.dtmf, 0 };
				switch_channel_set_variable(member_channel, "cc_exit_key", buf);
				break;
			}
		}
		switch_yield(1000);
	}

	/* Make sure an agent was found, as we might break above without setting it */
	if (!agent_found && (p = switch_channel_get_variable(member_channel, "cc_agent_found"))) {
		agent_found = (switch_bool_t)switch_true(p);
	}

	/* Stop member thread */
	if (h) {
		h->running = 0;
	}

	/* Check if we were removed because FS Core(BREAK) asked us to */
	if (h->member_cancel_reason == CC_MEMBER_CANCEL_REASON_NONE && !agent_found) {
		h->member_cancel_reason = CC_MEMBER_CANCEL_REASON_BREAK_OUT;
	}

	switch_channel_set_variable(member_channel, "cc_agent_found", NULL);
	/* Canceled for some reason */
	if (!switch_channel_up(member_channel) || h->member_cancel_reason != CC_MEMBER_CANCEL_REASON_NONE) {
		switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_DEBUG, "Member %s <%s> abandoned waiting in queue %s\n", switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_name")), switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_number")), queue_name);

		/* Update member state */
		sql = switch_mprintf("UPDATE members SET state = '%q', session_uuid = '', abandoned_epoch = '%" SWITCH_TIME_T_FMT "' WHERE system = 'single_box' AND uuid = '%q'",
				cc_member_state2str(CC_MEMBER_STATE_ABANDONED), local_epoch_time_now(NULL), member_uuid);
				cc_execute_sql(sql, NULL);
		switch_safe_free(sql);

		/* Hangup any callback agents  */
		//switch_core_session_hupall_matching_var("cc_member_pre_answer_uuid", member_uuid, SWITCH_CAUSE_ORIGINATOR_CANCEL);
		switch_core_session_hupall_matching_var_ans("cc_member_pre_answer_uuid", member_uuid, SWITCH_CAUSE_ORIGINATOR_CANCEL, (switch_hup_type_t) (SHT_UNANSWERED | SHT_ANSWERED));  //hmeng

		/* Generate an event */
		if (switch_event_create_subclass(&event, SWITCH_EVENT_CUSTOM, CALLCENTER_EVENT) == SWITCH_STATUS_SUCCESS) {
			switch_channel_event_set_data(member_channel, event);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Queue", queue_name);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Action", "member-queue-end");
			switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Member-Leaving-Time", "%" SWITCH_TIME_T_FMT, local_epoch_time_now(NULL));
			switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Member-Joined-Time", "%" SWITCH_TIME_T_FMT, t_member_called);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Cause", "Cancel");
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Cancel-Reason", cc_member_cancel_reason2str(h->member_cancel_reason));
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-UUID", member_uuid);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-Session-UUID", member_session_uuid);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Name", switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_name")));
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Number", switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_number")));
			switch_event_fire(&event);
		}

		/* Update some channel variables for xml_cdr needs */
		switch_channel_set_variable_printf(member_channel, "cc_queue_canceled_epoch", "%" SWITCH_TIME_T_FMT, local_epoch_time_now(NULL));
		switch_channel_set_variable_printf(member_channel, "cc_cause", "%s", "cancel");
		switch_channel_set_variable_printf(member_channel, "cc_cancel_reason", "%s", cc_member_cancel_reason2str(h->member_cancel_reason));

		/* Print some debug log information */
		switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_DEBUG, "Member \"%s\" <%s> exit queue %s due to %s\n",
						  switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_name")),
						  switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_number")),
						  queue_name, cc_member_cancel_reason2str(h->member_cancel_reason));

	} else {
		switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_DEBUG, "Member %s <%s> is answered by an agent in queue %s\n", switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_name")), switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_number")), queue_name);

		/* Update member state */
		sql = switch_mprintf("UPDATE members SET state = '%q', bridge_epoch = '%" SWITCH_TIME_T_FMT "' WHERE system = 'single_box' AND uuid = '%q'",
				cc_member_state2str(CC_MEMBER_STATE_ANSWERED), local_epoch_time_now(NULL), member_uuid);
		cc_execute_sql(sql, NULL);
		switch_safe_free(sql);

		/* Update some channel variables for xml_cdr needs */
		switch_channel_set_variable_printf(member_channel, "cc_cause", "%s", "answered");

	}

	/* Send Event with queue count */
	cc_queue_count(queue_name);

end:

	return;
}



/* Macro expands to: switch_status_t mod_callcenter_load(switch_loadable_module_interface_t **module_interface, switch_memory_pool_t *pool) */
SWITCH_MODULE_LOAD_FUNCTION(mod_callcenter_load)
{
	switch_application_interface_t *app_interface;
	switch_api_interface_t *api_interface = NULL;
	switch_status_t status;

	memset(&globals, 0, sizeof(globals));
	globals.pool = pool;

	switch_core_hash_init(&globals.queue_hash, globals.pool);
	switch_mutex_init(&globals.mutex, SWITCH_MUTEX_NESTED, globals.pool);

	if ((status = load_config()) != SWITCH_STATUS_SUCCESS) {
		return status;
	}

	switch_mutex_lock(globals.mutex);
	globals.running = 1;
	switch_mutex_unlock(globals.mutex);

	/* connect my internal structure to the blank pointer passed to me */
	*module_interface = switch_loadable_module_create_module_interface(pool, modname);

	if (!AGENT_DISPATCH_THREAD_STARTED) {
		cc_agent_dispatch_thread_start();
	}

	SWITCH_ADD_APP(app_interface, "callcenter", "CallCenter", CC_DESC, callcenter_function, CC_USAGE, SAF_NONE);


	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "%s\n", global_cf);
        acd_config_init(module_interface, api_interface);
	/* indicate that the module should continue to be loaded */
	return SWITCH_STATUS_SUCCESS;
}

/*
   Called when the system shuts down
   Macro expands to: switch_status_t mod_callcenter_shutdown() */
SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_callcenter_shutdown)
{
	switch_hash_index_t *hi;
	cc_queue_t *queue;
	void *val = NULL;
	const void *key;
	switch_ssize_t keylen;
	int sanity = 0;

	switch_mutex_lock(globals.mutex);
	if (globals.running == 1) {
		globals.running = 0;
	}
	switch_mutex_unlock(globals.mutex);

	while (globals.threads) {
		switch_cond_next();
		if (++sanity >= 60000) {
			break;
		}
	}

	switch_mutex_lock(globals.mutex);
	while ((hi = switch_hash_first(NULL, globals.queue_hash))) {
		switch_hash_this(hi, &key, &keylen, &val);
		queue = (cc_queue_t *) val;

		switch_core_hash_delete(globals.queue_hash, queue->name);

		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Waiting for write lock (queue %s)\n", queue->name);
		switch_thread_rwlock_wrlock(queue->rwlock);

		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Destroying queue %s\n", queue->name);

		switch_core_destroy_memory_pool(&queue->pool);
		queue = NULL;
	}

	switch_safe_free(globals.odbc_dsn);
	switch_safe_free(globals.dbname);
	switch_mutex_unlock(globals.mutex);

	return SWITCH_STATUS_SUCCESS;
}

