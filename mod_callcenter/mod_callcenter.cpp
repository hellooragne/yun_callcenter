#include <switch.h>

 //add by djxie
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>

#include "acd_tools.h"
#include "acd_state.h"
#include "acd_config.h"
#include "acd_common.h"

/* Prototypes */
SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_callcenter_shutdown);
SWITCH_MODULE_RUNTIME_FUNCTION(mod_callcenter_runtime);
SWITCH_MODULE_LOAD_FUNCTION(mod_callcenter_load);

SWITCH_MODULE_DEFINITION(mod_callcenter, mod_callcenter_load, mod_callcenter_shutdown, NULL);


static switch_xml_config_int_options_t config_int_0_86400 = { SWITCH_TRUE, 0, SWITCH_TRUE, 86400 };

//add by djxie
void get_queue_context();
void get_queue_context_cli(switch_stream_handle_t *, char *);

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
    switch_time_t t_curr_time = 0;
	switch_time_t t_member_called = atoi(h->member_joined_epoch);
	switch_event_t *event = NULL;

	switch_mutex_lock(globals.mutex);
	globals.threads++;
	switch_mutex_unlock(globals.mutex);

	/* member is gone before we could process it */
	if (!member_session) {
        char res[256];
        
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Member %s <%s> with uuid %s in queue %s is gone just before we assigned an agent\n", h->member_cid_name, h->member_cid_number, h->member_session_uuid, h->queue_name);
        
        /* Check to see if this is HA.recover member */
        sql = switch_mprintf("SELECT count(*) FROM members WHERE uuid = '%q' AND system = '%q'", h->member_uuid, SYSTEM_RECOVERY);
        cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
        switch_safe_free(sql);

        if (atoi(res) > 0) {
            /* for HA, send event. */
            if (switch_event_create_subclass(&event, SWITCH_EVENT_CUSTOM, CALLCENTER_EVENT) == SWITCH_STATUS_SUCCESS) {
                //switch_channel_event_set_data(member_channel, event);
                switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Queue", h->queue_name);
                switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Action", "member-queue-end");
                switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Member-Leaving-Time", "%" SWITCH_TIME_T_FMT, local_epoch_time_now(NULL));
                switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Member-Joined-Time", "%" SWITCH_TIME_T_FMT, t_member_called);
                switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Cause", "Cancel");
                switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-UUID", h->member_uuid);
                switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-Session-UUID", h->member_session_uuid);
                switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Name", h->member_cid_name);
                switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Number", h->member_cid_number);
                switch_event_fire(&event);
            }

        }

        // delete member
        sql = switch_mprintf("DELETE FROM members WHERE uuid = '%q'", h->member_uuid);

		cc_execute_sql(NULL, sql, NULL);
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
        // update by wyh. 20150521
        //status = switch_ivr_originate(member_session, &agent_session, &cause, dialstr, 60, NULL, cid_name ? cid_name : h->member_cid_name, h->member_cid_number, NULL, ovars, SOF_NONE, NULL);
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


		/*
        if (!strcasecmp(h->queue_strategy,"ring-all")) {
			// TBD...
		}
        */
        
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
        
        t_curr_time = local_epoch_time_now(NULL);
        
		/* for xml_cdr needs */
		switch_channel_set_variable(member_channel, "cc_agent", h->agent_name);
		switch_channel_set_variable_printf(member_channel, "cc_queue_answered_epoch", "%" SWITCH_TIME_T_FMT, t_curr_time); 

		/* Set UUID of the Agent channel */
		sql = switch_mprintf("UPDATE agents SET uuid = '%q', last_bridge_start = '%" SWITCH_TIME_T_FMT "', calls_answered = calls_answered + 1, no_answer_count = 0"
				" WHERE name = '%q'",
				agent_uuid, t_curr_time,
				h->agent_name);
		cc_execute_sql(NULL, sql, NULL);
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
        
        /** add by lxt. 
        member_session = switch_core_session_locate(h->member_session_uuid);
        
        if (!member_session) {
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Member %s <%s> with uuid %s in queue %s is gone just before we assigned an agent\n", h->member_cid_name, h->member_cid_number, h->member_session_uuid, h->queue_name);
            
            sql = switch_mprintf("DELETE FROM members WHERE uuid = '%q'", h->member_uuid);

            cc_execute_sql(NULL, sql, NULL);
            switch_safe_free(sql);
            goto done;
        }
        */

		switch_ivr_uuid_bridge(h->member_session_uuid, switch_core_session_get_uuid(agent_session));

		switch_channel_set_variable(member_channel, "cc_agent_uuid", agent_uuid);

		/* This is used for the waiting caller to quit waiting for a agent */
		switch_channel_set_variable(member_channel, "cc_agent_found", "true");

		/* Wait until the member hangup or the agent hangup.  This will quit also if the agent transfer the call */
		while(switch_channel_up(member_channel) && switch_channel_up(agent_channel) && globals.running) {
			switch_yield(100000);
		}
        /* 
        //add by djxie
        if (!strcasecmp(h->agent_type, CC_AGENT_TYPE_CALLBACK) && switch_channel_up(agent_channel) ) {
            switch_channel_hangup(agent_channel, SWITCH_CAUSE_NORMAL_CLEARING);
            
            while(switch_channel_up(agent_channel)) {
                switch_yield(100000);
            }
        }
        */
		tiers_state = CC_TIER_STATE_READY;
        
        t_curr_time = local_epoch_time_now(NULL);

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
			switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Bridge-Terminated-Time", "%" SWITCH_TIME_T_FMT, t_curr_time);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-UUID", h->member_uuid);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-Session-UUID", h->member_session_uuid);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Name", h->member_cid_name);
			switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Number", h->member_cid_number);
			switch_event_fire(&event);
		}
		/* for xml_cdr needs */
		switch_channel_set_variable_printf(member_channel, "cc_queue_terminated_epoch", "%" SWITCH_TIME_T_FMT, t_curr_time);

		/* Update Agents Items */
		/* Do not remove uuid of the agent if we are a standby agent */
        sql = switch_mprintf("UPDATE agents SET %s last_bridge_end = %" SWITCH_TIME_T_FMT ", talk_time = talk_time + (%" SWITCH_TIME_T_FMT "-last_bridge_start) WHERE name = '%q';", 
            (strcasecmp(h->agent_type, CC_AGENT_TYPE_UUID_STANDBY)?"uuid = '',":""), t_curr_time, t_curr_time, h->agent_name);
        cc_execute_sql(NULL, sql, NULL);
        switch_safe_free(sql);

		/* Remove the member entry from the db (Could become optional to support latter processing) */
		sql = switch_mprintf("DELETE FROM members WHERE uuid = '%q'", h->member_uuid);
		cc_execute_sql(NULL, sql, NULL);
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
		sql = switch_mprintf("UPDATE members SET state = '%q', serving_agent = '' WHERE serving_agent = '%q' AND uuid = '%q'",
                cc_member_state2str(CC_MEMBER_STATE_WAITING), h->agent_name, h->member_uuid);
		cc_execute_sql(NULL, sql, NULL);
		switch_safe_free(sql);

		switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_WARNING, "Agent %s Origination Canceled : %s\n", h->agent_name, switch_channel_cause2str(cause));

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
				sql = switch_mprintf("UPDATE agents SET no_answer_count = no_answer_count + 1 WHERE name = '%q';",
						h->agent_name);
				cc_execute_sql(NULL, sql, NULL);
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
	cc_execute_sql(NULL, sql, NULL);
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

#define MAX_QUEUE4DISPATCH               15
#define MAX_MEMBER4DISPATCH              10
#define MAX_MEMBER4DISPATCH_PER_QUEUE    5
#define MAX_AGENT4DISPATCH_PER_QUEUE     10

typedef struct {
	const char *uuid;
	const char *session_uuid;
	const char *cid_number;
	const char *cid_name;
	const char *joined_epoch;
	const char *score;
    const char *state;
}member_t;

struct member_list {
    member_t    members[MAX_MEMBER4DISPATCH_PER_QUEUE];
    uint32_t    num;
    
    switch_memory_pool_t   *pool;
};
typedef struct member_list member_list_t;

static int members_callback(void *pArg, int argc, char **argv, char **columnNames)
{
    member_list_t *cbt = (member_list_t *)pArg;
    
    if (cbt->num >= MAX_MEMBER4DISPATCH_PER_QUEUE) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Beyond MAX callback member. Abort! \n");
        return 0;
    }

	cbt->members[cbt->num].uuid         = switch_core_strdup(cbt->pool, argv[0]);
	cbt->members[cbt->num].session_uuid = switch_core_strdup(cbt->pool, argv[1]);
	cbt->members[cbt->num].cid_number   = switch_core_strdup(cbt->pool, argv[2]);
	cbt->members[cbt->num].cid_name     = switch_core_strdup(cbt->pool, argv[3]);
	cbt->members[cbt->num].joined_epoch = switch_core_strdup(cbt->pool, argv[4]);
	cbt->members[cbt->num].score        = switch_core_strdup(cbt->pool, argv[5]);
	cbt->members[cbt->num].state        = switch_core_strdup(cbt->pool, argv[6]);
    
    cbt->num++;

	return 0;
}


typedef struct {
	const char *name;
	const char *status;
	const char *originate_string;
	const char *no_answer_count;
	const char *max_no_answer;
	const char *reject_delay_time;
	const char *busy_delay_time;
	const char *no_answer_delay_time;
	const char *tier_state;
	const char *last_bridge_end;
	const char *wrap_up_time;
	const char *state;
	const char *ready_time;
	const char *tier_position;
	const char *tier_level;
	const char *type;
	const char *uuid;
    const char *system;
} agent_t;

struct agent_list {
    agent_t     agents[MAX_AGENT4DISPATCH_PER_QUEUE];
    uint32_t    num;
    
    switch_memory_pool_t   *pool;
};
typedef struct agent_list agent_list_t;


static int agents_callback(void *pArg, int argc, char **argv, char **columnNames)
{
	agent_list_t *cbt = (agent_list_t *)pArg;
    
    if (cbt->num >= MAX_AGENT4DISPATCH_PER_QUEUE) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Beyond MAX callback agent. Abort! \n");
        return 0;
    }
    
    cbt->agents[cbt->num].name              = switch_core_strdup(cbt->pool, argv[0]);
	cbt->agents[cbt->num].status            = switch_core_strdup(cbt->pool, argv[1]);
	cbt->agents[cbt->num].originate_string  = switch_core_strdup(cbt->pool, argv[2]);
	cbt->agents[cbt->num].no_answer_count   = switch_core_strdup(cbt->pool, argv[3]);
	cbt->agents[cbt->num].max_no_answer     = switch_core_strdup(cbt->pool, argv[4]);
	cbt->agents[cbt->num].reject_delay_time = switch_core_strdup(cbt->pool, argv[5]);
	cbt->agents[cbt->num].busy_delay_time   = switch_core_strdup(cbt->pool, argv[6]);
	cbt->agents[cbt->num].no_answer_delay_time = switch_core_strdup(cbt->pool, argv[7]);
	cbt->agents[cbt->num].tier_state        = switch_core_strdup(cbt->pool, argv[8]);
	cbt->agents[cbt->num].last_bridge_end   = switch_core_strdup(cbt->pool, argv[9]);
	cbt->agents[cbt->num].wrap_up_time      = switch_core_strdup(cbt->pool, argv[10]);
	cbt->agents[cbt->num].state             = switch_core_strdup(cbt->pool, argv[11]);
	cbt->agents[cbt->num].ready_time        = switch_core_strdup(cbt->pool, argv[12]);
	cbt->agents[cbt->num].tier_position     = switch_core_strdup(cbt->pool, argv[13]);
	cbt->agents[cbt->num].tier_level        = switch_core_strdup(cbt->pool, argv[14]);
	cbt->agents[cbt->num].type              = switch_core_strdup(cbt->pool, argv[15]);
	cbt->agents[cbt->num].uuid              = switch_core_strdup(cbt->pool, argv[16]);
    
    cbt->num++;

	return 0;
}

static int agents_sync_state_callback(void *pArg, int argc, char **argv, char **columnNames)
{
	agent_list_t *cbt = (agent_list_t *)pArg;
    
    if (cbt->num >= MAX_AGENT4DISPATCH_PER_QUEUE) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Beyond MAX callback agent. Abort! \n");
        return 0;
    }
    
    cbt->agents[cbt->num].name        = switch_core_strdup(cbt->pool, argv[0]);
    cbt->agents[cbt->num].system      = switch_core_strdup(cbt->pool, argv[1]);
	//cbt->agents[cbt->num].state     = switch_core_strdup(cbt->pool, argv[11]);
    
    cbt->num++;
    
    return 0;
}

int32_t getMemberList(cc_queue_t * queue, member_list_t *pMembers, int nLimit)
{
    char *sql = NULL;
    
    sql = switch_mprintf("SELECT uuid,session_uuid,cid_number,cid_name,joined_epoch,(%" SWITCH_TIME_T_FMT "-joined_epoch)+base_score AS score, state FROM members"
        " WHERE queue = '%q' AND state = '%q'"
        " ORDER BY score DESC LIMIT %d",
        local_epoch_time_now(NULL),
        queue->name, cc_member_state2str(CC_MEMBER_STATE_WAITING), 
        nLimit );

    cc_execute_sql_callback(NULL, NULL, sql, members_callback, pMembers );
    switch_safe_free(sql);

    return 0;
}

int32_t getAgentList(cc_queue_t * queue, agent_list_t *pAgents, int nLimit)
{
	char *sql = NULL;
	char *sql_order_by = NULL;
    switch_time_t currTime = 0;
    
    if (!strcasecmp(queue->strategy, "longest-hangup-time")) {
        sql_order_by = switch_mprintf("agents.last_bridge_end, position");
    } else if (!strcasecmp(queue->strategy, "longest-idle-agent")) {
        sql_order_by = switch_mprintf("agents.last_offered_call, position");
    } else if (!strcasecmp(queue->strategy, "agent-with-least-talk-time")) {
        sql_order_by = switch_mprintf("agents.talk_time, position");
    } else if (!strcasecmp(queue->strategy, "agent-with-fewest-calls")) {
        sql_order_by = switch_mprintf("agents.calls_answered, position");
    } else if(!strcasecmp(queue->strategy, "random")) {
        sql_order_by = switch_mprintf("random()");
    } else if(!strcasecmp(queue->strategy, "sequentially-by-agent-order")) {
        sql_order_by = switch_mprintf("position, agents.last_offered_call"); /* Default to last_offered_call, let add new strategy if needing it differently */
    } else {
        /* If the strategy doesn't exist, just fallback to the following */
        sql_order_by = switch_mprintf("position, agents.last_offered_call");
    }

    currTime = local_epoch_time_now(NULL);
    sql = switch_mprintf("SELECT name, status, contact, no_answer_count, max_no_answer, reject_delay_time, busy_delay_time, no_answer_delay_time, tiers.state, agents.last_bridge_end, agents.wrap_up_time, agents.state, agents.ready_time, tiers.position, tiers.level, agents.type, agents.uuid FROM agents JOIN tiers ON (agents.name = tiers.agent)"
        " WHERE tiers.queue = '%q'"
        " AND agents.status = '%q'"
        " AND agents.state = '%q'"
        " AND agents.ready_time < %" SWITCH_TIME_T_FMT " AND (agents.last_bridge_end + agents.wrap_up_time) < %" SWITCH_TIME_T_FMT
        " ORDER BY %q"
        " LIMIT %d",
        queue->name,
        cc_agent_status2str(CC_AGENT_STATUS_AVAILABLE),
        cc_agent_state2str(CC_AGENT_STATE_WAITING),
        currTime, currTime, 
        sql_order_by,
        nLimit );

    cc_execute_sql_callback(NULL, NULL, sql, agents_callback, pAgents);
    
    switch_safe_free(sql_order_by);
	switch_safe_free(sql);
    
    return 0;
}

int32_t sync_agent_InADirectCall(agent_list_t *pAgents, int nLimit)
{
	char    *sql = NULL;
    char    res[256];
    int32_t i=0;
    
    // get in-a-direct-call agent list
    sql = switch_mprintf("SELECT name, system FROM agents WHERE state = '%q' LIMIT %d",
              cc_agent_state2str(CC_AGENT_STATE_IN_A_DIRECT_CALL), nLimit );
    cc_execute_sql_callback(NULL, NULL, sql, agents_sync_state_callback, pAgents);
    switch_safe_free(sql);
    
    //switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, " 11111, number[%d]\n", pAgents->num);
    
    for (i=0; i<pAgents->num; i++) {
        //sql = switch_mprintf("select count(*) from channels where cid_num='%q' or dest='%q'", pAgents->agents[i].name, pAgents->agents[i].name); 
        sql = switch_mprintf("select count(*) from channels where presence_id like '%%%q%%'", pAgents->agents[i].name); 
        cc_coredb_execute_sql2str(NULL, sql, res, sizeof(res));
        switch_safe_free(sql);
        
        //switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, " 11111\n");
        
        if (atoi(res) == 0) {
        // now, agent is free.
            cc_agent_update("state", cc_agent_state2str(CC_AGENT_STATE_WAITING), pAgents->agents[i].name);
            
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, " update agent[%s] to state-waiting.\n", pAgents->agents[i].name);
            
            // For HA. If this call is a recovery call, we need send 2 events.
            if (!strcasecmp(pAgents->agents[i].system, SYSTEM_RECOVERY)) {
                switch_event_t *event = NULL;
                char queuename[256];
                char memberuuid[256];
                char membersessionuuid[256];
                
                sql = switch_mprintf("select queue from members where serving_agent='%q'", pAgents->agents[i].name ); 
                cc_coredb_execute_sql2str(NULL, sql, queuename, sizeof(queuename));
                switch_safe_free(sql);
                
                sql = switch_mprintf("select uuid from members where serving_agent='%q'", pAgents->agents[i].name ); 
                cc_coredb_execute_sql2str(NULL, sql, memberuuid, sizeof(memberuuid));
                switch_safe_free(sql);
                
                sql = switch_mprintf("select session_uuid from members where serving_agent='%q'", pAgents->agents[i].name ); 
                cc_coredb_execute_sql2str(NULL, sql, membersessionuuid, sizeof(membersessionuuid));
                switch_safe_free(sql);

                if (switch_event_create_subclass(&event, SWITCH_EVENT_CUSTOM, CALLCENTER_EVENT) == SWITCH_STATUS_SUCCESS) {
                    //switch_channel_event_set_data(agent_channel, event);
                    switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Queue", queuename);
                    switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Action", "bridge-agent-end");
                    //switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Hangup-Cause", switch_channel_cause2str(cause));
                    switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent", pAgents->agents[i].name);
                    //switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent-System", h->agent_system);
                    //switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent-UUID", agent_uuid);
                    //switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Agent-Called-Time", "%" SWITCH_TIME_T_FMT, t_agent_called);
                    //switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Agent-Answered-Time", "%" SWITCH_TIME_T_FMT, t_agent_answered);
                    //switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Member-Joined-Time", "%" SWITCH_TIME_T_FMT,  t_member_called);
                    //switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Bridge-Terminated-Time", "%" SWITCH_TIME_T_FMT, t_curr_time);
                    switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-UUID", memberuuid);
                    switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-Session-UUID", membersessionuuid);
                    //switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Name", h->member_cid_name);
                    //switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Number", h->member_cid_number);
                    switch_event_fire(&event);
                }

                /* Caller off event */
                if (switch_event_create_subclass(&event, SWITCH_EVENT_CUSTOM, CALLCENTER_EVENT) == SWITCH_STATUS_SUCCESS) {
                    //switch_channel_event_set_data(member_channel, event);
                    switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Queue", queuename);
                    switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Action", "member-queue-end");
                    //switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Hangup-Cause", switch_channel_cause2str(cause));
                    //switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Cause", "Terminated");
                    switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent", pAgents->agents[i].name);
                    //switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent-System", h->agent_system);
                    //switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent-UUID", agent_uuid);
                    //switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Agent-Called-Time", "%" SWITCH_TIME_T_FMT, t_agent_called);
                    //switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Agent-Answered-Time", "%" SWITCH_TIME_T_FMT, t_agent_answered);
                    //switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Member-Leaving-Time", "%" SWITCH_TIME_T_FMT,  local_epoch_time_now(NULL));
                    //switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Member-Joined-Time", "%" SWITCH_TIME_T_FMT, t_member_called);
                    switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-UUID", memberuuid);
                    switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-Session-UUID", membersessionuuid);
                    //switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Name", h->member_cid_name);
                    //switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Number", h->member_cid_number);
                    switch_event_fire(&event);
                }
                
                // DEL members
                sql = switch_mprintf("DELETE FROM members WHERE serving_agent='%q'", pAgents->agents[i].name );
                cc_execute_sql(NULL, sql, NULL);
                switch_safe_free(sql);
                
            }
        }
        
    }
    
    return i;
}

/**
    return:  -1   error.
             0    no match.
             1~N  N member dispatched.
    
*/
int cc_agent_dispatch(cc_queue_t *pq, member_list_t *members, agent_list_t *agents)
{
    int nRlt = 0;
    char *sql = NULL;
    uint32_t  agent_cursor = 0;
    char res[256];
    
    // test git commit. by lxt
    if (!pq || !members || !agents) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "cc_agent_dispatch got NULL pointer, pq || members || agents.\n");
    }
    
    for (int i=0; i<members->num; i++) {
        for (int j=agent_cursor; j<agents->num; j++) {
            /* Check if we switch to a different tier, if so, check if we should continue further for that member */
            // disable tier-level, tier-position right now.
            
            /* If Agent is not in a acceptable tier state, continue */
            // TBD...
            // if (NOT match ) { continue };
            
            // when agent involves is a direct call which bypass callcenter...
            //sql = switch_mprintf("select count(*) from channels where cid_num='%q' or dest='%q'", agents->agents[j].name, agents->agents[j].name); 
            sql = switch_mprintf("select count(*) from channels where presence_id like '%%%q%%'", agents->agents[j].name); 
            //switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, " sql statement: [%s] \n", sql);
            cc_coredb_execute_sql2str(NULL, sql, res, sizeof(res));
            switch_safe_free(sql);
            
            //switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, " agent[%s] in call state[%s]\n", agents->agents[j].name, res); 
            
            if (atoi(res) != 0) {
            // agent already in-a-direct-call.
                cc_agent_update("state", cc_agent_state2str(CC_AGENT_STATE_IN_A_DIRECT_CALL), agents->agents[j].name);
                
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, " update agent[%s] to state-in-a-direct-call.\n", agents->agents[j].name);
                
                agent_cursor = j+1;  // move to next available agent  
                continue;
            }
            
            /* Map the Agent to the member */
            sql = switch_mprintf("UPDATE members SET serving_agent = '%q', state = '%q'"
                    //" WHERE state = '%q' AND uuid = '%q'", 
                    " WHERE uuid = '%q'", 
                    agents->agents[j].name, cc_member_state2str(CC_MEMBER_STATE_TRYING),
                    //cc_member_state2str(CC_MEMBER_STATE_WAITING), members->members[i].member_uuid);
                    members->members[i].uuid);
            cc_execute_sql(NULL, sql, NULL);
            switch_safe_free(sql);

            /* Check if we won the race to get the member to our selected agent (Used for Multi system purposes) * /
            sql = switch_mprintf("SELECT count(*) FROM members WHERE serving_agent = '%q' AND AND uuid = '%q'",
                    agents->agents[j].name, members->members[i].member_uuid);
            cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
            switch_safe_free(sql);
            
            if (0 == atoi(res)) {
                / * Ok, someone else took it, or user hanged up already * /
                return 1;
            } else */ 
            {
			/* We default to default even if more entry is returned... Should never happen	anyway */
                /* Go ahead, start thread to try to bridge these 2 caller */
                switch_thread_t *thread;
                switch_threadattr_t *thd_attr = NULL;
                switch_memory_pool_t *pool;
                struct call_helper *h;

                switch_core_new_memory_pool(&pool);
                h = (struct call_helper *)switch_core_alloc(pool, sizeof(*h));
                h->pool = pool;
                h->member_uuid = switch_core_strdup(h->pool, members->members[i].uuid);
                h->member_session_uuid = switch_core_strdup(h->pool, members->members[i].session_uuid);
                h->queue_strategy = switch_core_strdup(h->pool, pq->strategy);
                h->originate_string = switch_core_strdup(h->pool, agents->agents[j].originate_string);
                h->agent_name = switch_core_strdup(h->pool, agents->agents[j].name);
                //h->agent_system = switch_core_strdup(h->pool, "single_box");
                h->agent_status = switch_core_strdup(h->pool, agents->agents[j].status);
                h->agent_type = switch_core_strdup(h->pool, agents->agents[j].type);
                h->agent_uuid = switch_core_strdup(h->pool, agents->agents[j].uuid);
                h->member_joined_epoch = switch_core_strdup(h->pool, members->members[i].joined_epoch); 
                h->member_cid_name = switch_core_strdup(h->pool, members->members[i].cid_name);
                h->member_cid_number = switch_core_strdup(h->pool, members->members[i].cid_number);
                h->queue_name = switch_core_strdup(h->pool, pq->name);
                h->record_template = switch_core_strdup(h->pool, pq->record_template);
                h->no_answer_count = atoi(agents->agents[j].no_answer_count);
                h->max_no_answer = atoi(agents->agents[j].max_no_answer);
                h->reject_delay_time = atoi(agents->agents[j].reject_delay_time);
                h->busy_delay_time = atoi(agents->agents[j].busy_delay_time);
                h->no_answer_delay_time = atoi(agents->agents[j].no_answer_delay_time);

                cc_agent_update("state", cc_agent_state2str(CC_AGENT_STATE_RECEIVING), h->agent_name);

                sql = switch_mprintf(
                        "UPDATE tiers SET state = '%q' WHERE agent = '%q' AND queue = '%q';"
                        "UPDATE tiers SET state = '%q' WHERE agent = '%q' AND NOT queue = '%q' AND state = '%q';",
                        cc_tier_state2str(CC_TIER_STATE_OFFERING), h->agent_name, h->queue_name,
                        cc_tier_state2str(CC_TIER_STATE_STANDBY), h->agent_name, h->queue_name, cc_tier_state2str(CC_TIER_STATE_READY));
                cc_execute_sql(NULL, sql, NULL);
                switch_safe_free(sql);

                switch_threadattr_create(&thd_attr, h->pool);
                switch_threadattr_detach_set(thd_attr, 1);
                switch_threadattr_stacksize_set(thd_attr, SWITCH_THREAD_STACKSIZE);
                switch_thread_create(&thread, thd_attr, outbound_agent_thread_run, h, h->pool);
                
                nRlt++;
                
                // <member, agent> pair found, break out...
                agent_cursor = j+1;
                break;
            }
        }
            
        if (agent_cursor >= agents->num) {
            break;   // no agent available, so break out for-member
        }
    }
    
    return nRlt;
}

static int AGENT_DISPATCH_THREAD_RUNNING = 0;
static int AGENT_DISPATCH_THREAD_STARTED = 0;

void *SWITCH_THREAD_FUNC cc_agent_dispatch_thread_run(switch_thread_t *thread, void *obj)
{
	int done = 0;
    cc_queue_t * queues[MAX_QUEUE4DISPATCH+1] = {0};
    cc_queue_t * pq = NULL;
    int queue_cursor_base = 0;
    uint32_t   magic_feed = 0xff;
    
    char *sql;
    char res[256];
    
    member_list_t memberList;
    agent_list_t  agentList;

    //add by djxie
    //long int time_start = gettime();

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
		//add by djxie
		//long int time_now = gettime();

        int nLIMIT_MEMBERS           = MAX_MEMBER4DISPATCH;
        int nLIMIT_MEMBERS_PER_QUEUE = MAX_MEMBER4DISPATCH_PER_QUEUE;
        int nLIMIT_AGENTS_PER_QUEUE  = MAX_AGENT4DISPATCH_PER_QUEUE;
        
        switch_hash_index_t *hi;
        int queue_count      = 0;
        int queue_cursor     = 0;
        int total_member     = 0;
        int member_per_queue = 0;
        //switch_bool_t   blHasQueue = SWITCH_FALSE;
        switch_memory_pool_t *pool;
        //switch_memory_pool_t *apool;
        
        if (magic_feed > 0xffff0000) {
            magic_feed = 0xff;
        }
        magic_feed++;
        
        // CC in slave mode
        if (SWITCH_FALSE == globals.blMaster) {
            if (magic_feed%10000 == 0) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Callcenter is @slave mode.\n");
            }
            
            switch_yield(100000);
            continue;
        } else {
            /* Check to see the system has already do HA without CC knows */
            sql = switch_mprintf("SELECT count(*) FROM cc_flags WHERE name = '%q' AND value = '%q'", MASTER_NODE, switch_core_get_switchname());
            cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
            switch_safe_free(sql);

            if (atoi(res) == 0) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "SYSTEM HA occured without notifying CC, Callcenter has to transfer to @slave mode.\n");
                
                // UPDATE this tuple
                switch_mutex_lock(globals.mutex);
                globals.blMaster = SWITCH_FALSE;
                switch_mutex_unlock(globals.mutex);
                
                continue;   // this CC already transfered to @slave state
            }

        }
        
        // get queue list
        memset(&queues[0], 0, sizeof(queues));
        
        switch_mutex_lock(globals.mutex);
        for (hi = switch_hash_first(NULL, globals.queue_hash); hi; hi = switch_hash_next(hi)) {
            if (queue_count > MAX_QUEUE4DISPATCH) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "MAX dispatch queue reached.\n");
                break;
            }
            
            switch_core_hash_this(hi, NULL, NULL, (void **)&queues[queue_count++]);
        }
        switch_mutex_unlock(globals.mutex);
        
        if ((0 == queue_count) || (MAX_QUEUE4DISPATCH < queue_count)) {
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "NO queue found. OR too much queues in system\n");
            switch_yield(1000000);
            continue;
        }
        
        // dispatch process.   Per queue.
        
        // last dispatch position
        queue_cursor = queue_cursor_base;
        
        // in case operator modify queues...
        pq = queues[queue_cursor++];
        if (!pq) {
            queue_cursor = 0;
            pq   = queues[0];
        }
        
        for (int i=0; i<queue_count; i++){
            // round-robin dispatch
            
            switch_core_new_memory_pool(&pool);
            //switch_core_new_memory_pool(&apool);
            
            // get waiting members
            memset(&memberList, 0, sizeof(memberList));
            memberList.pool = pool;
            getMemberList(pq, &memberList, nLIMIT_MEMBERS_PER_QUEUE);
            if (0 == memberList.num) {
                goto one_round_check;
            }

            // get available agents
            memset(&agentList, 0, sizeof(agentList));
            agentList.pool = pool;
            getAgentList(pq, &agentList, nLIMIT_AGENTS_PER_QUEUE);

            /*
            // add debug info by lxt
            if (agentList.num > 0) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "available_agents: %d\n", agentList.num);
                for (int k=0; k<agentList.num; k++) {
                    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "agents[%d]: %s\n", k, agentList.agents[k].name);
                }
            }*/
            if (0 == agentList.num) {
                goto one_round_check;
            }
            
            // find <member, agent> pair
            member_per_queue = cc_agent_dispatch(pq, &memberList, &agentList);
            if (0 < member_per_queue) {
                total_member += member_per_queue;
                
                if (total_member >= nLIMIT_MEMBERS) {
                    // enough, finish this round
                    queue_cursor_base = queue_cursor;
                    goto one_round_check;
                }
            } else if (0 == member_per_queue) {
                ; //NULL; // TBD... why, we need check ...
            } else {
                ; // NULL; // err.
            }
            

one_round_check:
            switch_core_destroy_memory_pool(&pool);
            //switch_core_destroy_memory_pool(&apool);
            
            // check 
            if ( !(pq = queues[queue_cursor++])) {
                queue_cursor = 0;
                pq = queues[0];
            }
            
        }
        

        //add by djxie
		/*printf state 10s*/
		//if (time_now >= (time_start + 5000000)) {
        if (magic_feed%60 == 0) {

			//switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "process member number [%d]\n", run_count);
			//switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "dispatch all time %ld\n", end - start);

			//switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "-----------------------------------------\n");
			//get_queue_context();

			//time_start = gettime();
		}
        
        if (magic_feed%10 == 0) {
        // check agent state: in-a-direct-call
            //switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, " --- agent still in in-a-direct-call? -----------------------------------------\n");
            switch_core_new_memory_pool(&pool);
            
            memset(&agentList, 0, sizeof(agentList));
            agentList.pool = pool;
            sync_agent_InADirectCall(&agentList, nLIMIT_AGENTS_PER_QUEUE);
            
            switch_core_destroy_memory_pool(&pool);
        }

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
	switch_uuid_t smember_uuid;
	char member_uuid[SWITCH_UUID_FORMATTED_LENGTH + 1] = "";
	switch_bool_t agent_found = SWITCH_FALSE;
	switch_bool_t moh_valid = SWITCH_TRUE;
	const char *p;
    /*add display_number for bug BBZ15CTI-483*/
	const char* display_number = switch_channel_get_variable(member_channel, "display_caller_number");

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

	/* create a new uuid */
    switch_uuid_get(&smember_uuid);
    switch_uuid_format(member_uuid, &smember_uuid);

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
		switch_event_add_header(event, SWITCH_STACK_BOTTOM, "CC-Action", "member-queue-%s", "start");
		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-UUID", member_uuid);
		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-Session-UUID", member_session_uuid);
		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Name", switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_name")));
		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Member-CID-Number", switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_number")));
		switch_event_fire(&event);
	}
	/* for xml_cdr needs */
	switch_channel_set_variable_printf(member_channel, "cc_queue_joined_epoch", "%" SWITCH_TIME_T_FMT, local_epoch_time_now(NULL));
	switch_channel_set_variable(member_channel, "cc_queue", queue_name);

    /* Add the caller to the member queue */
    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(member_session), SWITCH_LOG_DEBUG, "Member %s <%s> joining queue %s\n", switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_name")), switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_number")), queue_name);

    sql = switch_mprintf("INSERT INTO members"
            " (queue,system,uuid,session_uuid,system_epoch,joined_epoch,base_score,cid_number,cid_name,serving_agent,serving_system,state)"
            " VALUES('%q','single_box','%q','%q','%q','%" SWITCH_TIME_T_FMT "','%d','%q','%q','%q','','%q')", 
            queue_name,
            member_uuid,
            member_session_uuid,
            start_epoch,
            local_epoch_time_now(NULL),
            cc_base_score_int,
            ((display_number != NULL)? switch_str_nil(switch_channel_get_variable(member_channel, "display_caller_number")) : switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_number"))),
            switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_name")),
            "",
            cc_member_state2str(CC_MEMBER_STATE_WAITING));
    cc_execute_sql(queue, sql, NULL);
    switch_safe_free(sql);

	/* Send Event with queue count */
	cc_queue_count(queue_name);

	/* Start Thread that will playback different prompt to the channel */
	switch_core_new_memory_pool(&pool);
	h = (struct member_thread_helper *)switch_core_alloc(pool, sizeof(*h));

	h->pool = pool;
	h->member_uuid = switch_core_strdup(h->pool, member_uuid);
	h->member_session_uuid = switch_core_strdup(h->pool, member_session_uuid);
	h->member_cid_name = switch_core_strdup(h->pool, switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_name")));
	h->member_cid_number = ((display_number != NULL)?display_number:(switch_core_strdup(h->pool, switch_str_nil(switch_channel_get_variable(member_channel, "caller_id_number")))));
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
		if ((p = (const char *)switch_channel_get_variable(member_channel, "cc_agent_found")) && (agent_found = (switch_bool_t)switch_true(p))) {
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

		/* DELETE member */
        sql = switch_mprintf("DELETE FROM members WHERE uuid = '%q'", member_uuid);
        cc_execute_sql(NULL, sql, NULL);
		switch_safe_free(sql);

		/* Hangup any callback agents  // FS core break member-channel, so let's remove agent channel if exist. */
		//switch_core_session_hupall_matching_var("cc_member_pre_answer_uuid", member_uuid, SWITCH_CAUSE_ORIGINATOR_CANCEL);

		/*hmeng*/
		switch_core_session_hupall_matching_var_ans("cc_member_pre_answer_uuid", member_uuid, SWITCH_CAUSE_ORIGINATOR_CANCEL, (switch_hup_type_t) (SHT_UNANSWERED | SHT_ANSWERED));

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
		sql = switch_mprintf("UPDATE members SET state = '%q', bridge_epoch = '%" SWITCH_TIME_T_FMT "' WHERE uuid = '%q'",
				cc_member_state2str(CC_MEMBER_STATE_ANSWERED), local_epoch_time_now(NULL), member_uuid);
		cc_execute_sql(NULL, sql, NULL);
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
	switch_api_interface_t *api_interface;
	switch_status_t status;

	memset(&globals, 0, sizeof(globals));
	globals.pool = pool;
    globals.blMaster = SWITCH_FALSE;

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

