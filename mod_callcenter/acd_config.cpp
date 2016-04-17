#include "acd_config.h"

#define CC_CONFIG_API_SYNTAX "callcenter_config <target> <args>,\n"\
"\tcallcenter_config agent add [name] [type] | \n" \
"\tcallcenter_config agent del [name] | \n" \
"\tcallcenter_config agent reload [name] | \n" \
"\tcallcenter_config agent set status [agent_name] [status] | \n" \
"\tcallcenter_config agent set state [agent_name] [state] | \n" \
"\tcallcenter_config agent set contact [agent_name] [contact] | \n" \
"\tcallcenter_config agent set ready_time [agent_name] [wait till epoch] | \n"\
"\tcallcenter_config agent set reject_delay_time [agent_name] [wait second] | \n"\
"\tcallcenter_config agent set busy_delay_time [agent_name] [wait second] | \n"\
"\tcallcenter_config agent set no_answer_delay_time [agent_name] [wait second] | \n"\
"\tcallcenter_config agent get status [agent_name] | \n" \
"\tcallcenter_config agent get state [agent_name] | \n" \
"\tcallcenter_config agent get uuid [agent_name] | \n" \
"\tcallcenter_config agent list [[agent_name]] | \n" \
"\tcallcenter_config tier list | \n" \
"\tcallcenter_config operator add [name] [agent] | \n" \
"\tcallcenter_config operator del [name] | \n" \
"\tcallcenter_config operator reload [name] | \n" \
"\tcallcenter_config operator set agent [operator_name] [agent_name] | \n" \
"\tcallcenter_config operator get agent [operator_name] | \n" \
"\tcallcenter_config operator list [[operator_name]] | \n" \
"\tcallcenter_config operator status [operator_name] [agent_name] [status] | \n" \
"\tcallcenter_config opgroup add [queue_name] [operator_name] [level] [position] | \n" \
"\tcallcenter_config opgroup set level [queue_name] [operator_name] [level] | \n" \
"\tcallcenter_config opgroup set position [queue_name] [operator_name] [position] | \n" \
"\tcallcenter_config opgroup del [queue_name] [operator_name] | \n" \
"\tcallcenter_config opgroup reload [queue_name] [operator_name] | \n" \
"\tcallcenter_config opgroup list | \n" \
"\tcallcenter_config vdn add [name] [queue] | \n" \
"\tcallcenter_config vdn del [name] | \n" \
"\tcallcenter_config vdn reload [name] | \n" \
"\tcallcenter_config vdn set queue [vdn] [queue] | \n" \
"\tcallcenter_config vdn get queue [vdn] | \n" \
"\tcallcenter_config vdn list [[vdn]] | \n" \
"\tcallcenter_config qcontrol add [name] [control] | \n" \
"\tcallcenter_config qcontrol del [name] | \n" \
"\tcallcenter_config qcontrol set control [qcontrol] [control] | \n" \
"\tcallcenter_config qcontrol get control [qcontrol] | \n" \
"\tcallcenter_config qcontrol list [[qcontrol]] | \n" \
"\tcallcenter_config opcontrol add [name] [control] | \n" \
"\tcallcenter_config opcontrol del [name] | \n" \
"\tcallcenter_config opcontrol set control [opcontrol] [control] | \n" \
"\tcallcenter_config opcontrol get control [opcontrol] | \n" \
"\tcallcenter_config opcontrol list [[opcontrol]] | \n" \
"\tcallcenter_config queue load [queue_name] | \n" \
"\tcallcenter_config queue unload [queue_name] | \n" \
"\tcallcenter_config queue reload [queue_name] | \n" \
"\tcallcenter_config queue list | \n" \
"\tcallcenter_config queue list agents [queue_name] [status] | \n" \
"\tcallcenter_config queue list members [queue_name] | \n" \
"\tcallcenter_config queue list tiers [queue_name] | \n" \
"\tcallcenter_config queue count | \n" \
"\tcallcenter_config queue count agents [queue_name] [status] | \n" \
"\tcallcenter_config queue count members [queue_name] | \n" \
"\tcallcenter_config queue count tiers [queue_name]"


/////////////////////////////////////////////////////////////

static char ccflags_sql[] =
"CREATE TABLE cc_flags (\n"
"   name         VARCHAR(255),\n"
"   value	     VARCHAR(255)\n" ");\n";


static char operators_sql[] =
"CREATE TABLE operators (\n"
"   name     VARCHAR(255),\n"
"   agent    VARCHAR(255)\n" ");\n";

static char opgroup_sql[] =
"CREATE TABLE opgroup (\n"
"   operator VARCHAR(255),\n"
"   queue    VARCHAR(255),\n"
"   level    INTEGER NOT NULL DEFAULT 1,\n"
"   position INTEGER NOT NULL DEFAULT 1\n" ");\n";

static char vdn_sql[] =
"CREATE TABLE vdn (\n"
"   name     VARCHAR(255),\n"
"   queue    VARCHAR(255)\n" ");\n";

static char qcontrol_sql[] =
"CREATE TABLE qcontrol (\n"
"   name         VARCHAR(255),\n"
"   disp_num     VARCHAR(255),\n"
"   control      VARCHAR(255)\n" ");\n";

static char opcontrol_sql[] =
"CREATE TABLE opcontrol (\n"
"   name         VARCHAR(255),\n"
"   disp_num     VARCHAR(255),\n"
"   control      VARCHAR(255)\n" ");\n";

#define MAX_INFO_TUPLE_NUM   20
#define MAX_INFO_PER_TUPLE    4


typedef struct {
	const char *info1;
	const char *info2;
	const char *info3;
	const char *info4;
}info_t;

struct info_list {
    info_t      info[MAX_INFO_TUPLE_NUM];
    uint32_t    num;
    
    switch_memory_pool_t   *pool;
};
typedef struct info_list info_list_t;

static int info_list_callback(void *pArg, int argc, char **argv, char **columnNames)
{
    info_list_t *cbt = (info_list_t *)pArg;
    
    if (cbt->num >= MAX_INFO_TUPLE_NUM) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Beyond MAX info-list-callback member[%d]. Abort! \n", MAX_INFO_TUPLE_NUM);
        return 0;
    }

    if (0 == argc) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "info-list-callback error occurred. Abort! \n");
        return 0;
    }
    
    switch (argc) {
    case 4:
        cbt->info[cbt->num].info4   = switch_core_strdup(cbt->pool, argv[3]);
    case 3:
        cbt->info[cbt->num].info3   = switch_core_strdup(cbt->pool, argv[2]);
    case 2:
        cbt->info[cbt->num].info2   = switch_core_strdup(cbt->pool, argv[1]);
    case 1:
        cbt->info[cbt->num].info1   = switch_core_strdup(cbt->pool, argv[0]);
        break;
        
    default: 
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Too many info[>%d], beyond info-list-callback capability. Abort! \n", MAX_INFO_PER_TUPLE);
        return 0;
    }
    
    cbt->num++;

	return 0;
}

cc_status_t cc_ha_set(const char *key, const char *value)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
    char res[256] = "";

    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "hostname is: %s\n", switch_core_get_switchname());
            
    if (!strcasecmp(key, "state")) {
        
        if (!strcasecmp(value, "master")) {
            switch_mutex_lock(globals.mutex);
            globals.blMaster = SWITCH_TRUE;
            switch_mutex_unlock(globals.mutex);
        } else if (!strcasecmp(value, "slave")) {
            switch_mutex_lock(globals.mutex);
            globals.blMaster = SWITCH_FALSE;
            switch_mutex_unlock(globals.mutex);
        } else {
            result = CC_STATUS_FALSE;
            goto done;
        }
        
        if (globals.blMaster) {
            /* Check to see if record exist */
            sql = switch_mprintf("SELECT count(*) FROM cc_flags WHERE name = '%q'", MASTER_NODE);
            cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
            switch_safe_free(sql);

            // set node master
            if (atoi(res) > 0) {
                sql = switch_mprintf("UPDATE cc_flags SET value = '%q' WHERE name = '%q'", switch_core_get_switchname(), MASTER_NODE);
                cc_execute_sql(NULL, sql, NULL);
                switch_safe_free(sql);
                
            } else {
                sql = switch_mprintf("INSERT INTO cc_flags(name, value) VALUES('%q', '%q')", MASTER_NODE, switch_core_get_switchname());
                cc_execute_sql(NULL, sql, NULL);
                switch_safe_free(sql);
            
            }
            
        } else {
            
            /* Check to see if this node is master */
            sql = switch_mprintf("SELECT count(*) FROM cc_flags WHERE name = '%q' AND value = '%q'", MASTER_NODE, switch_core_get_switchname());
            cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
            switch_safe_free(sql);

            if (atoi(res) > 0) {
                // UPDATE this tuple
                sql = switch_mprintf("UPDATE cc_flags SET value = '' WHERE name = '%q'", MASTER_NODE);
                cc_execute_sql(NULL, sql, NULL);
                switch_safe_free(sql);
            }

        }
        
    } else {
        result = CC_STATUS_INVALID_KEY;
		goto done;
    }
    
done:		
	return result;
}

cc_status_t cc_ha_get(const char *key, char *ret_result, size_t ret_result_size)
{
	cc_status_t result = CC_STATUS_SUCCESS;

	if (!strcasecmp(key, "state")) { 
        if (globals.blMaster) {
            strcpy(ret_result, "master");
        } else {
            strcpy(ret_result, "slave");
        }

	} else {
		result = CC_STATUS_INVALID_KEY;
		goto done;

	}

done:   
	if (result == CC_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Get Info HA.[%s] is: %s\n", key, ret_result);
	}

	return result;
}

cc_status_t cc_ha_recover()
{
	cc_status_t result = CC_STATUS_SUCCESS;
    char *sql;
    char res[256];

    switch_mutex_lock(globals.mutex);
    globals.blMaster = SWITCH_TRUE;
    switch_mutex_unlock(globals.mutex);
    
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "hostname is: %s\n", switch_core_get_switchname());
    
    /* Check to see if record exist */
    sql = switch_mprintf("SELECT count(*) FROM cc_flags WHERE name = '%q'", MASTER_NODE);
    cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
    switch_safe_free(sql);

    // set node master
    if (atoi(res) > 0) {
        sql = switch_mprintf("UPDATE cc_flags SET value = '%q' WHERE name = '%q'", switch_core_get_switchname(), MASTER_NODE);
        cc_execute_sql(NULL, sql, NULL);
        switch_safe_free(sql);
        
    } else {
        sql = switch_mprintf("INSERT INTO cc_flags(name, value) VALUES('%q', '%q')", MASTER_NODE, switch_core_get_switchname());
        cc_execute_sql(NULL, sql, NULL);
        switch_safe_free(sql);
    
    }
    
    // set members.system = 'systemrecovery'
    sql = switch_mprintf("UPDATE members SET system = '%q'", SYSTEM_RECOVERY);
    cc_execute_sql(NULL, sql, NULL);
    switch_safe_free(sql);
    
    // set agents.system = 'systemrecovery', and agent.state = 'waiting'
    sql = switch_mprintf("UPDATE agents SET system = '%q', state = '%q' WHERE state != '%q'", 
                   SYSTEM_RECOVERY, cc_agent_state2str(CC_AGENT_STATE_WAITING),  cc_agent_state2str(CC_AGENT_STATE_WAITING));
    cc_execute_sql(NULL, sql, NULL);
    switch_safe_free(sql);
    
    // set tier.state = ready
    sql = switch_mprintf("UPDATE tiers SET state = '%q' WHERE state != '%q'", 
                   cc_tier_state2str(CC_TIER_STATE_READY), cc_tier_state2str(CC_TIER_STATE_READY));
    cc_execute_sql(NULL, sql, NULL);
    switch_safe_free(sql);

    // recover calls when agent is alerting but NOT answered
    {
        // if member.state == trying, put member back to waiting list.
        sql = switch_mprintf("UPDATE members SET state = '%q' WHERE state = '%q'", 
                       cc_member_state2str(CC_MEMBER_STATE_WAITING), cc_member_state2str(CC_MEMBER_STATE_TRYING));
        cc_execute_sql(NULL, sql, NULL);
        switch_safe_free(sql);
        
        // if member.state == answered, member already connected to agent. ACD will treat this case as InADirectCall, then inside InADirectCall, delete member
        /*
        sql = switch_mprintf("DELETE FROM members WHERE state = '%q'", cc_member_state2str(CC_MEMBER_STATE_ANSWERED) );
        cc_execute_sql(NULL, sql, NULL);
        switch_safe_free(sql);
        */
    }
    
	return result;
}


cc_status_t cc_operator_add(const char *operators, const char *agent)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
    char res[256] = "";
    
    /* Check to see if operator already exist */
    sql = switch_mprintf("SELECT count(*) FROM operators WHERE name = '%q'", operators);
    cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
    switch_safe_free(sql);

    if (atoi(res) != 0) {
        result = CC_STATUS_OPERATOR_ALREADY_EXIST;
        goto done;
    }
    
    /* Check to see if agent DOES exist */
    if (!zstr(agent)) {
        sql = switch_mprintf("SELECT count(*) FROM agents WHERE name = '%q'", agent);
        cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
        switch_safe_free(sql);

        if (atoi(res) == 0) {
            result = CC_STATUS_AGENT_NOT_FOUND;
            goto done;
        }
        
        // check to see if agent ALREADY bind to another operator
        sql = switch_mprintf("SELECT count(*) FROM operators WHERE agent = '%q'", agent);
        cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
        switch_safe_free(sql);

        if (atoi(res) != 0) {
            result = CC_STATUS_AGENT_ALREADY_BIND;
            goto done;
        }
        
    }
    
    /* Add Operator */
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Adding Operator [%s] with Agent [%s]\n", operators, agent);
    sql = switch_mprintf("INSERT INTO operators (name, agent) VALUES('%q', '%q');", operators, agent);
    cc_execute_sql(NULL, sql, NULL);
    switch_safe_free(sql);
    
    // Add to tiers table: No operator, No operator-group
    /*
    if (!zstr(agent)) {
        info_list_t  queue_list;
        switch_memory_pool_t *pool;
        
        switch_core_new_memory_pool(&pool);
        memset(&queue_list, 0, sizeof(queue_list));
        queue_list.pool = pool;
        
        sql = switch_mprintf("SELECT queue, level, position FROM opgroup WHERE operator = '%q'", operator);
        cc_execute_sql_callback(NULL, NULL, sql, info_list_callback, &queue_list );
        switch_safe_free(sql);
        
        for (int i=0; i<queue_list.num; i++) {
            cc_tier_add(queue_list.info[i].info1, agent, cc_tier_state2str(CC_TIER_STATE_READY), atoi(queue_list.info[i].info2), atoi(queue_list.info[i].info3));
        }
        
        switch_core_destroy_memory_pool(&pool);
    }
    */
    
done:		
	return result;
}

cc_status_t cc_operator_del(const char *operators)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
    char agent[256] = {'\0'};

    // find Agent
    sql = switch_mprintf("SELECT agent FROM operators WHERE name = '%q'", operators);
    cc_execute_sql2str(NULL, NULL, sql, agent, sizeof(agent));
    switch_safe_free(sql);

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Deleted Operator %s\n", operators);

    // remove tier:<agent, queue>
    // remove opgroup_item:<operator, queue>
    // Finally, remove operator:<operator, agent>
    if (!zstr(agent)) {
        sql = switch_mprintf("DELETE FROM tiers WHERE agent = '%q';"
                "DELETE FROM opgroup WHERE operator = '%q';"
                "DELETE FROM operators WHERE name = '%q';",
                agent, operators, operators);
    } else {
        sql = switch_mprintf("DELETE FROM opgroup WHERE operator = '%q';"
                "DELETE FROM operators WHERE name = '%q';",
                operators, operators);
    }
	cc_execute_sql(NULL, sql, NULL);
	switch_safe_free(sql);
    
	return result;
}

cc_status_t cc_operator_get(const char *key, const char *operators, char *ret_result, size_t ret_result_size)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
	char res[256];

	/* Check to see if agent already exists */
	sql = switch_mprintf("SELECT count(*) FROM operators WHERE name = '%q'", operators);
	cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
	switch_safe_free(sql);

	if (atoi(res) == 0) {
		result = CC_STATUS_OPERATOR_NOT_FOUND;
		goto done;
	}

	if (!strcasecmp(key, "agent")) {
		sql = switch_mprintf("SELECT %q FROM operators WHERE name = '%q'", key, operators);
		cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
		switch_safe_free(sql);
		switch_snprintf(ret_result, ret_result_size, "%s", res);

		if (switch_strlen_zero(res)) {
			result = CC_STATUS_AGENT_NOT_FOUND;
			goto done;
		}
        
	} else {
		result = CC_STATUS_INVALID_KEY;
		goto done;

	}

done:   
	if (result == CC_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Get Info Operator %s %s = %s\n", operators, key, res);
	}

	return result;
}

cc_status_t cc_operator_get_operator(const char *agent, char *ret_result, size_t ret_result_size)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
	char res[256];

    res[0] = '\0';
    sql = switch_mprintf("SELECT name FROM operators WHERE agent = '%q'", agent);
    cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
    switch_safe_free(sql);
    switch_snprintf(ret_result, ret_result_size, "%s", res);

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Get Operator by agent[%s] %s\n", agent, res);
	return result;
}

cc_status_t cc_operator_update(const char *key, const char *value, const char *operators)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
	char res[256];
    char unBoundAgent[256];
    
	/* Check to see if operator already exist */
	sql = switch_mprintf("SELECT count(*) FROM operators WHERE name = '%q'", operators);
	cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
	switch_safe_free(sql);

	if (atoi(res) == 0) {
		result = CC_STATUS_OPERATOR_NOT_FOUND;
		goto done;
	}

	if (!strcasecmp(key, "agent")) {
        switch_bool_t blDelAgent = (switch_bool_t)zstr(value);
        
        if (!blDelAgent) {
        // bound agent to operator
            /* Check to see if agent already exist */
            sql = switch_mprintf("SELECT count(*) FROM agents WHERE name = '%q'", value);
            cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
            switch_safe_free(sql);

            if (atoi(res) == 0) {
                result = CC_STATUS_AGENT_NOT_FOUND;
                goto done;
            }
            
            // check to see if agent ALREADY bind to another operator
            sql = switch_mprintf("SELECT count(*) FROM operators WHERE agent = '%q'", value);
            cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
            switch_safe_free(sql);

            if (atoi(res) != 0) {
                result = CC_STATUS_AGENT_ALREADY_BIND;
                goto done;
            }

            // check to see if operator ALREADY bound one agent
            res[0] = '\0';
            sql = switch_mprintf("SELECT agent FROM operators WHERE name = '%q'", operators);
            cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
            switch_safe_free(sql);
            
            if (!zstr(res)) {
                result = CC_STATUS_OPERATOR_ALREADY_BIND;
                goto done;
            }
        } else {
        // unBound Agent
            // check to see if operator ALREADY unbound agent
            unBoundAgent[0] = '\0';
            sql = switch_mprintf("SELECT agent FROM operators WHERE name = '%q'", operators);
            cc_execute_sql2str(NULL, NULL, sql, unBoundAgent, sizeof(unBoundAgent));
            switch_safe_free(sql);
            
            if (zstr(unBoundAgent)) {
                // ALREADY unbound, so just break out.
                //result = CC_STATUS_OPERATOR_ALREADY_UNBIND;
                goto done;
            }
        }
        
        // update operators.agent
        sql = switch_mprintf("UPDATE operators SET agent = '%q' WHERE name = '%q'", value, operators);
        cc_execute_sql(NULL, sql, NULL);
        switch_safe_free(sql);
        
        if (!blDelAgent) {
        // Add to tiers table
            info_list_t  queue_list;
            switch_memory_pool_t *pool;
            
            switch_core_new_memory_pool(&pool);
            memset(&queue_list, 0, sizeof(queue_list));
            queue_list.pool = pool;
            
            sql = switch_mprintf("SELECT queue, level, position FROM opgroup WHERE operator = '%q'", operators);
            cc_execute_sql_callback(NULL, NULL, sql, info_list_callback, &queue_list );
            switch_safe_free(sql);
            
            for (int i=0; i<queue_list.num; i++) {
                cc_tier_add(queue_list.info[i].info1, value, cc_tier_state2str(CC_TIER_STATE_READY), atoi(queue_list.info[i].info2), atoi(queue_list.info[i].info3));
            }
            
            switch_core_destroy_memory_pool(&pool);
        } else {
        // Del ALL entry in tiers table
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Deleted ALL tiers of Agent %s\n", unBoundAgent);
            sql = switch_mprintf("DELETE FROM tiers WHERE agent = '%q';", unBoundAgent);
            cc_execute_sql(NULL, sql, NULL);
            switch_safe_free(sql);
        }
        
	} else {
		result = CC_STATUS_INVALID_KEY;
		goto done;
	}

done:
	if (result == CC_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Updated Operator %s set %s = %s\n", operators, key, value);
	}

	return result;
}

cc_status_t cc_operator_logon(const char *operators, const char *agent)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
	char res[256];
    
	/* Check to see if operator already exist */
	sql = switch_mprintf("SELECT count(*) FROM operators WHERE name = '%q'", operators);
	cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
	switch_safe_free(sql);

	if (atoi(res) == 0) {
		result = CC_STATUS_OPERATOR_NOT_FOUND;
		goto done;
	}

    // check to see if operator ALREADY bound one agent
    res[0] = '\0';
    sql = switch_mprintf("SELECT agent FROM operators WHERE name = '%q'", operators);
    cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
    switch_safe_free(sql);
    
    if (!zstr(res)) {
        result = CC_STATUS_OPERATOR_ALREADY_BIND;
        goto done;
    }

    /* Check to see if agent already exist */
    sql = switch_mprintf("SELECT count(*) FROM agents WHERE name = '%q'", agent);
    cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
    switch_safe_free(sql);

    if (atoi(res) == 0) {
        result = CC_STATUS_AGENT_NOT_FOUND;
        goto done;
    }
    
    // check to see if agent ALREADY belongs to another operator
    sql = switch_mprintf("SELECT count(*) FROM operators WHERE agent = '%q'", agent);
    cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
    switch_safe_free(sql);

    if (atoi(res) != 0) {
        result = CC_STATUS_AGENT_ALREADY_BIND;
        goto done;
    }
    
    // update operators.agent AND agent.status
    sql = switch_mprintf("UPDATE agents SET status = '%q', last_status_change = '%" SWITCH_TIME_T_FMT "' WHERE name = '%q';"
            "UPDATE operators SET agent = '%q' WHERE name = '%q'",
            cc_agent_status2str(CC_AGENT_STATUS_LOGGED_ON), local_epoch_time_now(NULL), agent, 
            agent, operators);
    cc_execute_sql(NULL, sql, NULL);
    switch_safe_free(sql);

    // Add to tiers table
    {
        info_list_t  queue_list;
        switch_memory_pool_t *pool;
        
        switch_core_new_memory_pool(&pool);
        memset(&queue_list, 0, sizeof(queue_list));
        queue_list.pool = pool;
        
        sql = switch_mprintf("SELECT queue, level, position FROM opgroup WHERE operator = '%q'", operators);
        cc_execute_sql_callback(NULL, NULL, sql, info_list_callback, &queue_list );
        switch_safe_free(sql);
        
        for (int i=0; i<queue_list.num; i++) {
            cc_tier_add(queue_list.info[i].info1, agent, cc_tier_state2str(CC_TIER_STATE_READY), atoi(queue_list.info[i].info2), atoi(queue_list.info[i].info3));
            
            // send out event per each queue
            {
                switch_event_t *event;

                if (switch_event_create_subclass(&event, SWITCH_EVENT_CUSTOM, CALLCENTER_EVENT) == SWITCH_STATUS_SUCCESS) {
                    switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Operator", operators);
                    switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Queue", queue_list.info[i].info1);
                    switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent", agent);
                    switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Action", "agent-status-change");
                    switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent-Status", cc_agent_status2str(CC_AGENT_STATUS_LOGGED_ON));
                    switch_event_fire(&event);
                }
            }
    
        }
        
        switch_core_destroy_memory_pool(&pool);
    }

done:
	if (result == CC_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Updated Operator %s LoggedOn\n", operators);
	}

	return result;
}

cc_status_t cc_operator_logout(const char *operators, const char *agent)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
	char res[256];
    char binded_agent[256];
    
	/* Check to see if operator already exist */
	sql = switch_mprintf("SELECT count(*) FROM operators WHERE name = '%q'", operators);
	cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
	switch_safe_free(sql);

	if (atoi(res) == 0) {
		result = CC_STATUS_OPERATOR_NOT_FOUND;
		goto done;
	}
    
    // ??? can be removed later... ???
    /* Check to see if agent already exist */
    sql = switch_mprintf("SELECT count(*) FROM agents WHERE name = '%q'", agent);
    cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
    switch_safe_free(sql);

    if (atoi(res) == 0) {
        result = CC_STATUS_AGENT_NOT_FOUND;
        goto done;
    }
    
    // check to see if operator ALREADY unbound agent
    sql = switch_mprintf("SELECT agent FROM operators WHERE name = '%q'", operators);
    cc_execute_sql2str(NULL, NULL, sql, binded_agent, sizeof(binded_agent));
    switch_safe_free(sql);
    
    if (!zstr(binded_agent)) {
    // unBound agent
        sql = switch_mprintf("UPDATE operators SET agent = '' WHERE name = '%q';"
                "UPDATE agents SET status = '%q', last_status_change = '%" SWITCH_TIME_T_FMT "' WHERE name = '%q'",
                operators,
                cc_agent_status2str(CC_AGENT_STATUS_LOGGED_OUT), local_epoch_time_now(NULL), binded_agent);
        cc_execute_sql(NULL, sql, NULL);
        switch_safe_free(sql);
        
        // Del ALL entry in tiers table
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Deleted ALL tiers of Agent %s\n", binded_agent);
        sql = switch_mprintf("DELETE FROM tiers WHERE agent = '%q';", binded_agent);
        cc_execute_sql(NULL, sql, NULL);
        switch_safe_free(sql);

        /* Used to stop any active callback */
        sql = switch_mprintf("SELECT uuid FROM members WHERE serving_agent = '%q' AND NOT state = 'Answered'", binded_agent);
        cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
        switch_safe_free(sql);
        if (!switch_strlen_zero(res)) {
            //switch_core_session_hupall_matching_var("cc_member_pre_answer_uuid", res, SWITCH_CAUSE_ORIGINATOR_CANCEL);

			/*hmeng*/
			switch_core_session_hupall_matching_var_ans("cc_member_pre_answer_uuid", res, SWITCH_CAUSE_ORIGINATOR_CANCEL, (switch_hup_type_t) (SHT_UNANSWERED | SHT_ANSWERED));
        }
        
    }

    // send out event
    {
        switch_event_t *event;
        if (switch_event_create_subclass(&event, SWITCH_EVENT_CUSTOM, CALLCENTER_EVENT) == SWITCH_STATUS_SUCCESS) {
            switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Operator", operators);
            switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent", binded_agent);
            switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Action", "agent-status-change");
            switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "CC-Agent-Status", cc_agent_status2str(CC_AGENT_STATUS_LOGGED_OUT));
            switch_event_fire(&event);
        }
    }

done:
	if (result == CC_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Updated Operator %s LoggedOut\n", operators);
	}

	return result;
}

static switch_status_t load_operator(const char *operators)
{
	switch_xml_t x_operators, x_operator, cfg, xml;

	if (!(xml = switch_xml_open_cfg(global_cf, &cfg, NULL))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Open of %s failed\n", global_cf);
		return SWITCH_STATUS_FALSE;
	}
	if (!(x_operators = switch_xml_child(cfg, "operators"))) {
		goto end;
	}

	if ((x_operator = switch_xml_find_child(x_operators, "operator", "name", operators))) {
		const char *agent = switch_xml_attr(x_operator, "agent");
        
        //cc_status_t res = cc_operator_add(operator, agent);
        cc_operator_add(operators, agent);
        // For HA, only add new agent, DO NOT update or DELETE.
        /*
        if (res == CC_STATUS_OPERATOR_ALREADY_EXIST) {
            cc_operator_update("agent", agent, operator);
        }*/
	}

end:

	if (xml) {
		switch_xml_free(xml);
	}

	return SWITCH_STATUS_SUCCESS;
}

cc_status_t cc_vdn_add(const char *vdnname, const char *queuename)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
    char res[256] = "";
    cc_queue_t *queue = NULL;
    
    /* Check to see if vdn already exist */
    sql = switch_mprintf("SELECT count(*) FROM vdn WHERE name = '%q'", vdnname);
    cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
    switch_safe_free(sql);

    if (atoi(res) != 0) {
        result = CC_STATUS_VDN_ALREADY_EXIST;
        goto done;
    }
    
    /* Check to see if queue DOES exist */
    if (zstr(queuename) || !(queue = get_queue(queuename))) {
        result = CC_STATUS_QUEUE_NOT_FOUND;
        goto done;
    } else {
        queue_rwunlock(queue);
    }
    
    /* Add Operator */
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Adding VDN [%s] with queue [%s]\n", vdnname, queuename);
    sql = switch_mprintf("INSERT INTO vdn (name, queue) VALUES('%q', '%q');", vdnname, queuename);
    cc_execute_sql(NULL, sql, NULL);
    switch_safe_free(sql);
    
done:		
	return result;
}

cc_status_t cc_vdn_del(const char *vdnname)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Deleted VDN %s\n", vdnname);

    sql = switch_mprintf("DELETE FROM vdn WHERE name = '%q';", vdnname);
	cc_execute_sql(NULL, sql, NULL);
	switch_safe_free(sql);
    
	return result;
}

cc_status_t cc_vdn_get(const char *key, const char *vdnname, char *ret_result, size_t ret_result_size)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
	char res[256];

	/* Check to see if vdn already exists */
	sql = switch_mprintf("SELECT count(*) FROM vdn WHERE name = '%q'", vdnname);
	cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
	switch_safe_free(sql);

	if (atoi(res) == 0) {
		result = CC_STATUS_VDN_NOT_FOUND;
		goto done;
	}

	if (!strcasecmp(key, "queue")) {
		sql = switch_mprintf("SELECT %q FROM vdn WHERE name = '%q'", key, vdnname);
		cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
		switch_safe_free(sql);
		switch_snprintf(ret_result, ret_result_size, "%s", res);
        
	} else {
		result = CC_STATUS_INVALID_KEY;
		goto done;
	}

done:   
	if (result == CC_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Get Info VDN %s %s = %s\n", vdnname, key, res);
	}

	return result;
}

cc_status_t cc_vdn_update(const char *key, const char *value, const char *vdnname)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
	char res[256];
    
	/* Check to see if VDN already exist */
	sql = switch_mprintf("SELECT count(*) FROM vdn WHERE name = '%q'", vdnname);
	cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
	switch_safe_free(sql);

	if (atoi(res) == 0) {
		result = CC_STATUS_VDN_NOT_FOUND;
		goto done;
	}
    
	if (!strcasecmp(key, "queue")) {
        if (zstr(value)) {
            result = CC_STATUS_QUEUE_NOT_FOUND;
            goto done;
        }
        
        // update vdn.queue
        sql = switch_mprintf("UPDATE vdn SET queue = '%q' WHERE name = '%q'", value, vdnname);
        cc_execute_sql(NULL, sql, NULL);
        switch_safe_free(sql);
        
	} else {
		result = CC_STATUS_INVALID_KEY;
		goto done;
	}

done:
	if (result == CC_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Updated VDN %s set %s = %s\n", vdnname, key, value);
	}

	return result;
}

static switch_status_t load_vdn(const char *vdnname)
{
	switch_xml_t x_vdns, x_vdn, cfg, xml;

	if (!(xml = switch_xml_open_cfg(global_cf, &cfg, NULL))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Open of %s failed\n", global_cf);
		return SWITCH_STATUS_FALSE;
	}
	if (!(x_vdns = switch_xml_child(cfg, "vdns"))) {
		goto end;
	}

	if ((x_vdn = switch_xml_find_child(x_vdns, "vdn", "name", vdnname))) {
		//const char *vdn   = switch_xml_attr(x_vdn, "name");
        const char *queue = switch_xml_attr(x_vdn, "queue");
        cc_status_t res = CC_STATUS_FALSE;
        
        /* Hack to check if a vdn already exist */
        if (cc_vdn_update("unknown", "unknown", vdnname) == CC_STATUS_VDN_NOT_FOUND) {
            res = cc_vdn_add(vdnname, queue);
        } else {
            res = cc_vdn_update("queue", queue, vdnname);
        }

        if (res != CC_STATUS_SUCCESS) {
            goto end;
        }
    }

end:

	if (xml) {
		switch_xml_free(xml);
	}

	return SWITCH_STATUS_SUCCESS;
}

cc_status_t cc_opgroup_item_add(const char *queue_name, const char *operators, int level, int position)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
    char res[256] = "";
    char agent[256] = {'\0'};
    
	cc_queue_t *queue = NULL;
	if (!(queue = get_queue(queue_name))) {
		result = CC_STATUS_QUEUE_NOT_FOUND;
		goto done;
	} else {
		queue_rwunlock(queue);
	}

	// * Check to see if operator DOES exist
	sql = switch_mprintf("SELECT count(*) FROM operators WHERE name = '%q'", operators);
	cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
	switch_safe_free(sql);

	if (atoi(res) == 0) {
		result = CC_STATUS_OPERATOR_NOT_FOUND;
		goto done;
	}

    // * Check to see if operator-tier already exist
    sql = switch_mprintf("SELECT count(*) FROM opgroup WHERE operator = '%q' AND queue = '%q'", operators, queue_name);
    cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
    switch_safe_free(sql);

    if (atoi(res) != 0) {
        result = CC_STATUS_OPERATORGROUP_ALREADY_EXIST;
        goto done;
    }
    
    // get agent
    sql = switch_mprintf("SELECT agent FROM operators WHERE name = '%q'", operators);
    cc_execute_sql2str(NULL, NULL, sql, agent, sizeof(agent));
    switch_safe_free(sql);

    // * Check to see if agent exist
    if (!zstr(agent)) {
        // NEED sync into tiers table.
        result = cc_tier_add(queue_name, agent, cc_tier_state2str(CC_TIER_STATE_READY), level, position);
        if (CC_STATUS_TIER_ALREADY_EXIST == result) {
            // something is wrong(like:FS core dump then restart, so rubbish data inside table-tiers), we need recover from this situation.
            // how about clean DB first?
            sql = switch_mprintf("UPDATE tiers SET level = '%d', position = '%d' WHERE queue = '%q' AND agent = '%q'", level, position, queue_name, agent);
            cc_execute_sql(NULL, sql, NULL);
            switch_safe_free(sql);
        }
    }

    // * Add opgroup_item
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Adding Operator %s on Queue %s, level %d, position %d\n", 
        operators, queue_name, level, position);
    sql = switch_mprintf("INSERT INTO opgroup (operator, queue, level, position) VALUES('%q', '%q', '%d', '%d');",
            operators, queue_name, level, position);
    cc_execute_sql(NULL, sql, NULL);
    switch_safe_free(sql);

    result = CC_STATUS_SUCCESS;

done:		
	return result;
}

cc_status_t cc_opgroup_item_update(const char *key, const char *value, const char *queue_name, const char *operators)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
	char res[256] = {0};
    char agent[256] = {'\0'};
    char level[256];
    char position[256];

	/* Check to see if opgroup_item DOES exist */
	sql = switch_mprintf("SELECT count(*) FROM opgroup WHERE queue = '%q' AND operator = '%q'", queue_name, operators);
	cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
	switch_safe_free(sql);

	if (atoi(res) == 0) {
		result = CC_STATUS_OPERATORGROUP_NOT_FOUND;
		goto done;
	}

    sql = switch_mprintf("SELECT agent FROM operators WHERE name = '%q'", operators);
    cc_execute_sql2str(NULL, NULL, sql, agent, sizeof(agent));
    switch_safe_free(sql);

    // get level/position
    if (!zstr(agent)) {
        sql = switch_mprintf("SELECT level FROM opgroup WHERE queue = '%q' AND operator = '%q'", queue_name, operators);
        cc_execute_sql2str(NULL, NULL, sql, level, sizeof(level));
        switch_safe_free(sql);

        sql = switch_mprintf("SELECT position FROM opgroup WHERE queue = '%q' AND operator = '%q'", queue_name, operators);
        cc_execute_sql2str(NULL, NULL, sql, position, sizeof(position));
        switch_safe_free(sql);
    }

	if (!strcasecmp(key, "queue")) {
        cc_queue_t *queue = NULL;
        if (!(queue = get_queue(value))) {
            result = CC_STATUS_QUEUE_NOT_FOUND;
            goto done;
        } else {
            queue_rwunlock(queue);
        }
        
        // update tier
        if (!zstr(agent)) {
            cc_tier_del(queue_name, agent);
            cc_tier_add(value, agent, cc_tier_state2str(CC_TIER_STATE_READY), atoi(level), atoi(position));
        }

        // update opgroup.queue
		sql = switch_mprintf("UPDATE opgroup SET queue = '%q' WHERE queue = '%q' AND operator = '%q'", value, queue_name, operators);
		cc_execute_sql(NULL, sql, NULL);
		switch_safe_free(sql);
        
	} else if (!strcasecmp(key, "level")) {
        if (!zstr(agent)) {
            cc_tier_update(key, value, queue_name, agent);
        }
        
        sql = switch_mprintf("UPDATE opgroup SET level = '%d' WHERE queue = '%q' AND operator = '%q'", atoi(value), queue_name, operators);
		cc_execute_sql(NULL, sql, NULL);
		switch_safe_free(sql);

	} else if (!strcasecmp(key, "position")) {
        if (!zstr(agent)) {
            cc_tier_update(key, value, queue_name, agent);
        }
        
        sql = switch_mprintf("UPDATE opgroup SET position = '%d' WHERE queue = '%q' AND operator = '%q'", atoi(value), queue_name, operators);
		cc_execute_sql(NULL, sql, NULL);
		switch_safe_free(sql);

	} else {
		result = CC_STATUS_INVALID_KEY;
		goto done;
	}	
done:
	if (result == CC_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Updated opgroup: operator %s set %s = %s\n", operators, key, value);
	}
	return result;
}

cc_status_t cc_opgroup_item_get(const char *key, const char *operators, char *ret_result, size_t ret_result_size)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
	char res[256] = {0};

	/* Check to see if opgroup_item DOES exist */
	sql = switch_mprintf("SELECT count(*) FROM opgroup WHERE operator = '%q'", operators);
	cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
	switch_safe_free(sql);

	if (atoi(res) == 0) {
		result = CC_STATUS_OPERATORGROUP_NOT_FOUND;
		goto done;
	}

	if (!strcasecmp(key, "queue")) {
		sql = switch_mprintf("SELECT queue FROM opgroup WHERE operator = '%q'", operators);
        cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
        switch_safe_free(sql);
        switch_snprintf(ret_result, ret_result_size, "%s", res);
/*        
	} else if (!strcasecmp(key, "level")) {
		sql = switch_mprintf("SELECT level FROM opgroup WHERE operator = '%q'", operators);
        cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
        switch_safe_free(sql);
        switch_snprintf(ret_result, ret_result_size, "%s", res);
        
	} else if (!strcasecmp(key, "position")) {
		sql = switch_mprintf("SELECT queue FROM opgroup WHERE operator = '%q'", operators);
        cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
        switch_safe_free(sql);
        switch_snprintf(ret_result, ret_result_size, "%s", res);
*/
	} else {
		result = CC_STATUS_INVALID_KEY;
		goto done;
	}	
done:
	if (result == CC_STATUS_SUCCESS) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Get %s by operator[%s] %s\n", key, operators, res);
	}
	return result;
}

cc_status_t cc_opgroup_item_del(const char *queue_name, const char *operators)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
    char agent[256] = {'\0'};

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Deleted opgroup_item<%s, %s>.\n", queue_name, operators);
    
    sql = switch_mprintf("SELECT agent FROM operators WHERE name = '%q'", operators);
    cc_execute_sql2str(NULL, NULL, sql, agent, sizeof(agent));
    switch_safe_free(sql);
    
    if (!zstr(agent)) {
        // remove Tier
        cc_tier_del(queue_name, agent);
    }

    // del opgroup_item
    sql = switch_mprintf("DELETE FROM opgroup WHERE queue = '%q' AND operator = '%q';", queue_name, operators);
    cc_execute_sql(NULL, sql, NULL);
	switch_safe_free(sql);

	return result;
}



static switch_status_t load_opgroup_new_item(const char *operators, const char *queue, const char *level, const char *position)
{
    cc_status_t result = (cc_status_t)SWITCH_STATUS_FALSE;
    
	/* Hack to check if an tier already exist */
    if (level && position) {
        result = cc_opgroup_item_add(queue, operators, atoi(level), atoi(position));
    } else {
        /* default to level 1 and position 1 within the level */
        result = cc_opgroup_item_add(queue, operators, 0, 0);
    }
    
    // For HA, only insert new opgroup item
    if (CC_STATUS_OPERATORGROUP_ALREADY_EXIST == result) {
        return SWITCH_STATUS_SUCCESS;
    }
    
    return SWITCH_STATUS_FALSE;
}

static switch_status_t load_opgroup(switch_bool_t load_all, const char *queue_name, const char *operator_name)
{
	switch_xml_t x_opgroup, x_opgroup_item, cfg, xml;
	switch_status_t result = SWITCH_STATUS_FALSE;

	if (!(xml = switch_xml_open_cfg(global_cf, &cfg, NULL))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Open of %s failed\n", global_cf);
		return SWITCH_STATUS_FALSE;
	}

	if (!(x_opgroup = switch_xml_child(cfg, "opgroup"))) {
		goto end;
	}

	/* Importing from XML config opgroup */
    /* For support HA, please call load_opgroup_new_item  */
	for (x_opgroup_item = switch_xml_child(x_opgroup, "tier"); x_opgroup_item; x_opgroup_item = x_opgroup_item->next) {
		const char *operators  = switch_xml_attr(x_opgroup_item, "operator");
		const char *queue = switch_xml_attr(x_opgroup_item, "queue");
		const char *level = switch_xml_attr(x_opgroup_item, "level");
		const char *position = switch_xml_attr(x_opgroup_item, "position");
		if (load_all == SWITCH_TRUE) {
			result = load_opgroup_new_item(operators, queue, level, position);
		} else if (!zstr(operator_name) && !zstr(queue_name) && !strcasecmp(operators, operator_name) && !strcasecmp(queue, queue_name)) {
			result = load_opgroup_new_item(operators, queue, level, position);
		} else if (zstr(operator_name) && !strcasecmp(queue, queue_name)) {
			result = load_opgroup_new_item(operators, queue, level, position);
		} else if (zstr(queue_name) && !strcasecmp(operators, operator_name)) {
			result = load_opgroup_new_item(operators, queue, level, position);
		}
	}

end:

	if (xml) {
		switch_xml_free(xml);
	}

	return result;
}

static switch_status_t cc_qcontrol_add(const char *queue_name, const char *control, const char *disp_num)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
    char res[256] = "";
    cc_queue_t *queue = NULL;
    
    // debug
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "inside cc_qcontrol_add %s %s %s\n", queue_name, control, disp_num);
    
    /* Check to see if queue DOES exist */
    if (zstr(queue_name) || !(queue = get_queue(queue_name))) {
        result = CC_STATUS_QUEUE_NOT_FOUND;
        goto done;
    } else {
        queue_rwunlock(queue);
    }
    
	// * Check to see if binding DOES exist
	sql = switch_mprintf("SELECT count(*) FROM qcontrol WHERE name = '%q'", queue_name);
	cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
	switch_safe_free(sql);

	if (atoi(res) != 0) {
		result = CC_STATUS_QUEUE_CONTROL_ALREADY_EXIST;
		goto done;
	}
    
    // insert tuple
    sql = switch_mprintf("INSERT INTO qcontrol (name, control, disp_num) VALUES('%q', '%q', '%q')", queue_name, control, disp_num);
    cc_execute_sql(NULL, sql, NULL);
    switch_safe_free(sql);

done:		
    return (switch_status_t)result;
}

static switch_status_t cc_qcontrol_del(const char *queue_name)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Deleted Queue control %s\n", queue_name);

    sql = switch_mprintf("DELETE FROM qcontrol WHERE name = '%q';", queue_name);
	cc_execute_sql(NULL, sql, NULL);
	switch_safe_free(sql);
    
	return (switch_status_t)result;
}

cc_status_t cc_qcontrol_update(const char *key, const char *value, const char *queue_name)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
	char res[256];
    
	/* Check to see if binding already exist */
	sql = switch_mprintf("SELECT count(*) FROM qcontrol WHERE name = '%q'", queue_name);
	cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
	switch_safe_free(sql);

	if (atoi(res) == 0) {
		result = CC_STATUS_QUEUE_CONTROL_NOT_FOUND;
		goto done;
	}
    
	if (!strcasecmp(key, "control")) {
        // update qcontrol.control
        sql = switch_mprintf("UPDATE qcontrol SET control = '%q' WHERE name = '%q'", value, queue_name);
        cc_execute_sql(NULL, sql, NULL);
        switch_safe_free(sql);
        
	} else if (!strcasecmp(key, "disp_num")) {
        // update qcontrol.disp_num
        sql = switch_mprintf("UPDATE qcontrol SET disp_num = '%q' WHERE name = '%q'", value, queue_name);
        cc_execute_sql(NULL, sql, NULL);
        switch_safe_free(sql);
        
	} else {
		result = CC_STATUS_INVALID_KEY;
		goto done;
	}

done:
	if (result == CC_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Updated qcontrol %s set %s = %s\n", queue_name, key, value);
	}

	return result;
}

cc_status_t cc_qcontrol_get(const char *key, const char *queue_name, char *ret_result, size_t ret_result_size)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
	char res[256];

	/* Check to see if vdn already exists */
	sql = switch_mprintf("SELECT count(*) FROM qcontrol WHERE name = '%q'", queue_name);
	cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
	switch_safe_free(sql);

	if (atoi(res) == 0) {
		result = CC_STATUS_QUEUE_CONTROL_NOT_FOUND;
		goto done;
	}

	if (!strcasecmp(key, "control") || !strcasecmp(key, "disp_num")) {
		sql = switch_mprintf("SELECT %q FROM qcontrol WHERE name = '%q'", key, queue_name);
		cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
		switch_safe_free(sql);
		switch_snprintf(ret_result, ret_result_size, "%s", res);
        
	} else {
		result = CC_STATUS_INVALID_KEY;
		goto done;
	}

done:   
	if (result == CC_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Get Info QControl %s %s = %s\n", queue_name, key, res);
	}

	return result;
}

static switch_status_t cc_opcontrol_add(const char *operators, const char *control, const char *disp_num)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
    char res[256] = "";
    
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "inside cc_opcontrol_add %s %s\n", operators, control);
    
	// * Check to see if operator DOES exist
	sql = switch_mprintf("SELECT count(*) FROM operators WHERE name = '%q'", operators);
	cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
	switch_safe_free(sql);

	if (atoi(res) == 0) {
		result = CC_STATUS_OPERATOR_NOT_FOUND;
		goto done;
	}
    
	// * Check to see if binding DOES exist
	sql = switch_mprintf("SELECT count(*) FROM opcontrol WHERE name = '%q'", operators);
	cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
	switch_safe_free(sql);

	if (atoi(res) != 0) {
		result = CC_STATUS_OPERATOR_CONTROL_ALREADY_EXIST;
		goto done;
	}
    
    // insert tuple
    sql = switch_mprintf("INSERT INTO opcontrol (name, control, disp_num) VALUES('%q', '%q', '%q')", operators, control, disp_num);
    cc_execute_sql(NULL, sql, NULL);
    switch_safe_free(sql);

done:		
    return (switch_status_t)result;
}

static switch_status_t cc_opcontrol_del(const char *operators)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Deleted Operator control %s\n", operators);

    sql = switch_mprintf("DELETE FROM opcontrol WHERE name = '%q';", operators);
	cc_execute_sql(NULL, sql, NULL);
	switch_safe_free(sql);
    
	return (switch_status_t)result;
}

cc_status_t cc_opcontrol_update(const char *key, const char *value, const char *operators)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
	char res[256];
    
	/* Check to see if binding already exist */
	sql = switch_mprintf("SELECT count(*) FROM opcontrol WHERE name = '%q'", operators);
	cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
	switch_safe_free(sql);

	if (atoi(res) == 0) {
		result = CC_STATUS_OPERATOR_CONTROL_NOT_FOUND;
		goto done;
	}
    
	if (!strcasecmp(key, "control")) {
        // update qcontrol.control
        sql = switch_mprintf("UPDATE opcontrol SET control = '%q' WHERE name = '%q'", value, operators);
        cc_execute_sql(NULL, sql, NULL);
        switch_safe_free(sql);
        
	} else if (!strcasecmp(key, "disp_num")) {
        // update qcontrol.control
        sql = switch_mprintf("UPDATE opcontrol SET disp_num = '%q' WHERE name = '%q'", value, operators);
        cc_execute_sql(NULL, sql, NULL);
        switch_safe_free(sql);
        
	} else {
		result = CC_STATUS_INVALID_KEY;
		goto done;
	}

done:
	if (result == CC_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Updated Opcontrol %s set %s = %s\n", operators, key, value);
	}

	return result;
}

cc_status_t cc_opcontrol_get(const char *key, const char *operators, char *ret_result, size_t ret_result_size)
{
	cc_status_t result = CC_STATUS_SUCCESS;
	char *sql;
	char res[256];

	/* Check to see if vdn already exists */
	sql = switch_mprintf("SELECT count(*) FROM opcontrol WHERE name = '%q'", operators);
	cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
	switch_safe_free(sql);

	if (atoi(res) == 0) {
		result = CC_STATUS_OPERATORGROUP_NOT_FOUND;
		goto done;
	}

	if (!strcasecmp(key, "control") || !strcasecmp(key, "disp_num")) {
		sql = switch_mprintf("SELECT %q FROM opcontrol WHERE name = '%q'", key, operators);
		cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
		switch_safe_free(sql);
		switch_snprintf(ret_result, ret_result_size, "%s", res);
        
	} else {
		result = CC_STATUS_INVALID_KEY;
		goto done;
	}

done:   
	if (result == CC_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Get Info OpControl %s %s = %s\n", operators, key, res);
	}

	return result;
}






/////////////////////////////////////////////////////////////



static char agents_sql[] =
"CREATE TABLE agents (\n"
"   name      VARCHAR(255),\n"
"   system    VARCHAR(255),\n"
"   uuid      VARCHAR(255),\n"
"   type      VARCHAR(255),\n" /* Callback , Dial in...*/
"   contact   VARCHAR(255),\n"
"   status    VARCHAR(255),\n"
/*User Personal Status
  Available
  On Break
  Logged Out
 */
"   state   VARCHAR(255),\n"
/* User Personal State
   Waiting
   Receiving
   In a queue call
 */

"   max_no_answer INTEGER NOT NULL DEFAULT 0,\n"
"   wrap_up_time INTEGER NOT NULL DEFAULT 0,\n"
"   reject_delay_time INTEGER NOT NULL DEFAULT 0,\n"
"   busy_delay_time INTEGER NOT NULL DEFAULT 0,\n"
"   no_answer_delay_time INTEGER NOT NULL DEFAULT 0,\n"
"   last_bridge_start INTEGER NOT NULL DEFAULT 0,\n"
"   last_bridge_end INTEGER NOT NULL DEFAULT 0,\n"
"   last_offered_call INTEGER NOT NULL DEFAULT 0,\n" 
"   last_status_change INTEGER NOT NULL DEFAULT 0,\n"
"   no_answer_count INTEGER NOT NULL DEFAULT 0,\n"
"   calls_answered  INTEGER NOT NULL DEFAULT 0,\n"
"   talk_time  INTEGER NOT NULL DEFAULT 0,\n"
"   ready_time INTEGER NOT NULL DEFAULT 0\n"
");\n";


static char tiers_sql[] =
"CREATE TABLE tiers (\n"
"   queue    VARCHAR(255),\n"
"   agent    VARCHAR(255),\n"
"   state    VARCHAR(255),\n"
/*
   Agent State: 
   Ready
   Active inbound
   Wrap-up inbound
   Standby
   No Answer
   Offering
 */
"   level    INTEGER NOT NULL DEFAULT 1,\n"
"   position INTEGER NOT NULL DEFAULT 1\n" ");\n";



static char members_sql[] =
"CREATE TABLE members (\n"
"   queue	     VARCHAR(255),\n"
"   system	     VARCHAR(255),\n"
"   uuid	     VARCHAR(255) NOT NULL DEFAULT '',\n"
"   session_uuid     VARCHAR(255) NOT NULL DEFAULT '',\n"
"   cid_number	     VARCHAR(255),\n"
"   cid_name	     VARCHAR(255),\n"
"   system_epoch     INTEGER NOT NULL DEFAULT 0,\n"
"   joined_epoch     INTEGER NOT NULL DEFAULT 0,\n"
"   rejoined_epoch   INTEGER NOT NULL DEFAULT 0,\n"
"   bridge_epoch     INTEGER NOT NULL DEFAULT 0,\n"
"   abandoned_epoch  INTEGER NOT NULL DEFAULT 0,\n"
"   base_score       INTEGER NOT NULL DEFAULT 0,\n"
"   skill_score      INTEGER NOT NULL DEFAULT 0,\n"
"   serving_agent    VARCHAR(255),\n"
"   serving_system   VARCHAR(255),\n"
"   state	     VARCHAR(255)\n" ");\n";
/* Member State 
   Waiting
   Answered
 */

int list_result_callback(void *pArg, int argc, char **argv, char **columnNames)
{
	struct list_result *cbt = (struct list_result *) pArg;
	int i = 0;

	cbt->row_process++;

	if (cbt->row_process == 1) {
		for ( i = 0; i < argc; i++) {
			cbt->stream->write_function(cbt->stream,"%s", columnNames[i]);
			if (i < argc - 1) {
				cbt->stream->write_function(cbt->stream,"|");
			}
		}  
		cbt->stream->write_function(cbt->stream,"\n");

	}
	for ( i = 0; i < argc; i++) {
		cbt->stream->write_function(cbt->stream,"%s", argv[i]);
		if (i < argc - 1) {
			cbt->stream->write_function(cbt->stream,"|");
		}
	}
	cbt->stream->write_function(cbt->stream,"\n");
	return 0;
}

switch_status_t load_config(void)
{
	switch_status_t status = SWITCH_STATUS_SUCCESS;
	switch_xml_t cfg, xml, settings, param, x_queues, x_queue, x_agents, x_agent, x_operators, x_operator, x_vdns, x_vdn;
	switch_cache_db_handle_t *dbh = NULL;
	char *sql = NULL;
	
	if (!(xml = switch_xml_open_cfg(global_cf, &cfg, NULL))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Open of %s failed\n", global_cf);
		status = SWITCH_STATUS_TERM;
		goto end;
	}

	switch_mutex_lock(globals.mutex);
	if ((settings = switch_xml_child(cfg, "settings"))) {
		for (param = switch_xml_child(settings, "param"); param; param = param->next) {
			char *var = (char *) switch_xml_attr_soft(param, "name");
			char *val = (char *) switch_xml_attr_soft(param, "value");

			if (!strcasecmp(var, "debug")) {
				globals.debug = atoi(val);
			} else if (!strcasecmp(var, "dbname")) {
				globals.dbname = strdup(val);
			} else if (!strcasecmp(var, "odbc-dsn")) {
				globals.odbc_dsn = strdup(val);
			}
		}
	}
	if (!globals.dbname) {
		globals.dbname = strdup(CC_SQLITE_DB_NAME);
	}

	/* Initialize database */
	if (!(dbh = cc_get_db_handle())) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CRIT, "Cannot open DB!\n");
		status = SWITCH_STATUS_TERM;
		goto end;
	}
	switch_cache_db_test_reactive(dbh, "select count(session_uuid) from members", "drop table members", members_sql);
	switch_cache_db_test_reactive(dbh, "select count(ready_time) from agents", NULL, "alter table agents add ready_time integer not null default 0;"
									   "alter table agents add reject_delay_time integer not null default 0;"
									   "alter table agents add busy_delay_time  integer not null default 0;");
	switch_cache_db_test_reactive(dbh, "select count(no_answer_delay_time) from agents", NULL, "alter table agents add no_answer_delay_time integer not null default 0;");
	switch_cache_db_test_reactive(dbh, "select count(ready_time) from agents", "drop table agents", agents_sql);
	switch_cache_db_test_reactive(dbh, "select count(queue) from tiers", "drop table tiers" , tiers_sql);
    
    switch_cache_db_test_reactive(dbh, "select count(name) from operators", "drop table operators" , operators_sql);
    switch_cache_db_test_reactive(dbh, "select count(queue) from opgroup", "drop table opgroup" , opgroup_sql);
    switch_cache_db_test_reactive(dbh, "select count(name) from vdn", "drop table vdn" , vdn_sql);
    switch_cache_db_test_reactive(dbh, "select count(name) from qcontrol", "drop table qcontrol" , qcontrol_sql);
    switch_cache_db_test_reactive(dbh, "select count(name) from opcontrol", "drop table opcontrol" , opcontrol_sql);
    switch_cache_db_test_reactive(dbh, "select count(name) from cc_flags", "drop table cc_flags" , ccflags_sql);

	switch_cache_db_release_db_handle(&dbh);

	/* Reset a unclean shutdown */
	sql = switch_mprintf("update agents set state = 'Waiting', uuid = '' where system = 'single_box';"
						 "update tiers set state = 'Ready' where agent IN (select name from agents where system = 'single_box');"
						 "update members set state = '%q', session_uuid = '' where system = 'single_box';",
						 cc_member_state2str(CC_MEMBER_STATE_ABANDONED));
	cc_execute_sql(NULL, sql, NULL);
	switch_safe_free(sql);

	/* Loading queue into memory struct */
	if ((x_queues = switch_xml_child(cfg, "queues"))) {
		for (x_queue = switch_xml_child(x_queues, "queue"); x_queue; x_queue = x_queue->next) {
			load_queue(switch_xml_attr_soft(x_queue, "name"));
		}
	}

	/* Importing from XML config Agents */
	if ((x_agents = switch_xml_child(cfg, "agents"))) {
		for (x_agent = switch_xml_child(x_agents, "agent"); x_agent; x_agent = x_agent->next) {
			const char *agent = switch_xml_attr(x_agent, "name");
			if (agent) {
				load_agent(agent);
			}
		}
	}

	/* Importing from XML config Operators */
    if ((x_operators = switch_xml_child(cfg, "operators"))) {
		for (x_operator = switch_xml_child(x_operators, "operator"); x_operator; x_operator = x_operator->next) {
			const char *operators = switch_xml_attr(x_operator, "name");
			//const char *agent = switch_xml_attr(x_operator, "agent");
			if (operators) {
				load_operator(operators);
			}
		}
	}
    
    /* Importing from XML config opgroup */
	load_opgroup(SWITCH_TRUE, NULL, NULL);

    /* Importing from XML config vdn */
    if ((x_vdns = switch_xml_child(cfg, "vdns"))) {
		for (x_vdn = switch_xml_child(x_vdns, "vdn"); x_vdn; x_vdn = x_vdn->next) {
			const char *vdnname = switch_xml_attr(x_vdn, "name");
            const char *queue = switch_xml_attr(x_vdn, "queue");
            
            // for HA, only add new iterms
			if (vdnname) {
                cc_vdn_add(vdnname, queue);
			}
		}
	}
    
    // Importing operator-control settings from XML conf file.
    {
        switch_xml_t x_ctrl;
        
        if ((x_ctrl = switch_xml_child(cfg, "callcontrol"))) {
            switch_xml_t x_qctrls, x_opctrls;
            
            // queue-control
            if ((x_qctrls = switch_xml_child(x_ctrl, "qcontrols"))) {
                switch_xml_t x_qctrl;
                
                for (x_qctrl = switch_xml_child(x_qctrls, "qcontrol"); x_qctrl; x_qctrl = x_qctrl->next) {
                    const char *queue   = switch_xml_attr(x_qctrl, "queue");
                    const char *control = switch_xml_attr(x_qctrl, "control");
                    const char *disp_num= switch_xml_attr(x_qctrl, "disp_num");

                    cc_qcontrol_add(queue, control, disp_num);
                }
            }
            
            if ((x_opctrls = switch_xml_child(x_ctrl, "opcontrols"))) {
                switch_xml_t x_opctrl;
                
                for (x_opctrl = switch_xml_child(x_opctrls, "opcontrol"); x_opctrl; x_opctrl = x_opctrl->next) {
                    const char *operators = switch_xml_attr(x_opctrl, "operator");
                    const char *control  = switch_xml_attr(x_opctrl, "control");
                    const char *disp_num = switch_xml_attr(x_opctrl, "disp_num");
                    
                    cc_opcontrol_add(operators, control, disp_num);
                }
            }
        }
        
    }
    
    
end:
	switch_mutex_unlock(globals.mutex);

	if (xml) {
		switch_xml_free(xml);
	}

	return status;
}


SWITCH_STANDARD_API(cc_config_api_function)
{
	char *mydata = NULL, *argv[8] = { 0 };
	const char *section = NULL;
	const char *action = NULL;
	char *sql;
	int initial_argc = 2;

	int argc;
	if (!globals.running) {
		return SWITCH_STATUS_FALSE;
	}
	if (zstr(cmd)) {
		stream->write_function(stream, "-USAGE: \n%s\n", CC_CONFIG_API_SYNTAX);
		return SWITCH_STATUS_SUCCESS;
	}

	mydata = strdup(cmd);
	switch_assert(mydata);

	argc = switch_separate_string(mydata, ' ', argv, (sizeof(argv) / sizeof(argv[0])));

	if (argc < 2) {
		stream->write_function(stream, "%s", "-ERR Invalid!\n");
		goto done;
	}

	section = argv[0];
	action = argv[1];

	if (section && !strcasecmp(section, "agent")) {
		if (action && !strcasecmp(action, "add")) {
			if (argc-initial_argc < 2) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *name = argv[0 + initial_argc];
				const char *type = argv[1 + initial_argc];
				switch (cc_agent_add(name, type)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					case CC_STATUS_AGENT_ALREADY_EXIST:
						stream->write_function(stream, "%s", "-ERR Agent already exist!\n");
						goto done;
					case CC_STATUS_AGENT_INVALID_TYPE:
						stream->write_function(stream, "%s", "-ERR Agent type invalid!\n");
						goto done;

					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;

				}
			}

		} else if (action && !strcasecmp(action, "del")) {
			if (argc-initial_argc < 1) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *agent = argv[0 + initial_argc];
				switch (cc_agent_del(agent)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "set")) {
			if (argc-initial_argc < 3) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *key = argv[0 + initial_argc];
				const char *agent = argv[1 + initial_argc];
				const char *value = argv[2 + initial_argc];

				switch (cc_agent_update(key, value, agent)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					case CC_STATUS_AGENT_INVALID_STATUS:
						stream->write_function(stream, "%s", "-ERR Invalid Agent Status!\n");
						goto done;
					case CC_STATUS_AGENT_INVALID_STATE:
						stream->write_function(stream, "%s", "-ERR Invalid Agent State!\n");
						goto done;
					case CC_STATUS_AGENT_INVALID_TYPE:
						stream->write_function(stream, "%s", "-ERR Invalid Agent Type!\n");
						goto done;
					case CC_STATUS_INVALID_KEY:
						stream->write_function(stream, "%s", "-ERR Invalid Agent Update KEY!\n");
						goto done;
					case CC_STATUS_AGENT_NOT_FOUND:	
						stream->write_function(stream, "%s", "-ERR Agent not found!\n");
						goto done;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}

			}

		} else if (action && !strcasecmp(action, "get")) {
			if (argc-initial_argc < 2) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *key = argv[0 + initial_argc];
				const char *agent = argv[1 + initial_argc];
				char ret[64];
				switch (cc_agent_get(key, agent, ret, sizeof(ret))) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", ret);
						break;
					case CC_STATUS_INVALID_KEY:
						stream->write_function(stream, "%s", "-ERR Invalid Agent Update KEY!\n");
						goto done;
					case CC_STATUS_AGENT_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR Agent not found!\n");
						goto done;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;


				}
			}

		} else if (action && !strcasecmp(action, "list")) {
			struct list_result cbt;
			cbt.row_process = 0;
			cbt.stream = stream;
			if ( argc-initial_argc > 1 ) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else if ( argc-initial_argc == 1 ) {
				sql = switch_mprintf("SELECT * FROM agents WHERE name='%q'", argv[0 + initial_argc]);
			} else {
				sql = switch_mprintf("SELECT * FROM agents");
			}
			cc_execute_sql_callback(NULL /* queue */, NULL /* mutex */, sql, list_result_callback, &cbt /* Call back variables */);
			switch_safe_free(sql);
			stream->write_function(stream, "%s", "+OK\n");
		}

	} else if (section && !strcasecmp(section, "tier")) {
		/* remove by Tom Liang
        if (action && !strcasecmp(action, "add")) {
			if (argc-initial_argc < 4) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *queue_name = argv[0 + initial_argc];
				const char *agent = argv[1 + initial_argc];
				const char *level = argv[2 + initial_argc];
				const char *position = argv[3 + initial_argc];

				switch(cc_tier_add(queue_name, agent, cc_tier_state2str(CC_TIER_STATE_READY), atoi(level), atoi(position))) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					case CC_STATUS_QUEUE_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR Queue not found!\n");
						goto done;
					case CC_STATUS_TIER_INVALID_STATE:
						stream->write_function(stream, "%s", "-ERR Invalid Tier State!\n");
						goto done;
					case CC_STATUS_AGENT_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR Agent not found!\n");
						goto done;
					case CC_STATUS_TIER_ALREADY_EXIST:
						stream->write_function(stream, "%s", "-ERR Tier already exist!\n");
						goto done;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "set")) {
			if (argc-initial_argc < 4) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *key = argv[0 + initial_argc];
				const char *queue_name = argv[1 + initial_argc];
				const char *agent = argv[2 + initial_argc];
				const char *value = argv[3 + initial_argc];

				switch(cc_tier_update(key, value, queue_name, agent)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					case CC_STATUS_AGENT_INVALID_STATUS:
						stream->write_function(stream, "%s", "-ERR Invalid Agent Status!\n");
						goto done;
					case CC_STATUS_TIER_INVALID_STATE:
						stream->write_function(stream, "%s", "-ERR Invalid Tier State!\n");
						goto done;
					case CC_STATUS_INVALID_KEY:
						stream->write_function(stream, "%s", "-ERR Invalid Tier Update KEY!\n");
						goto done;
					case CC_STATUS_AGENT_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR Agent not found!\n");
						goto done;
					case CC_STATUS_QUEUE_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR Agent not found!\n");
						goto done;

					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "del")) {
			if (argc-initial_argc < 2) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done; 
			} else {
				const char *queue = argv[0 + initial_argc];
				const char *agent = argv[1 + initial_argc];
				switch (cc_tier_del(queue, agent)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;

				}
			}

        } else */ 
        if (action && !strcasecmp(action, "list")) {
			struct list_result cbt;
			cbt.row_process = 0;
			cbt.stream = stream;
			sql = switch_mprintf("SELECT * FROM tiers ORDER BY level, position");
			cc_execute_sql_callback(NULL /* queue */, NULL /* mutex */, sql, list_result_callback, &cbt /* Call back variables */);
			switch_safe_free(sql);
			stream->write_function(stream, "%s", "+OK\n");
		}
	} else if (section && !strcasecmp(section, "queue")) {
		if (action && !strcasecmp(action, "load")) {
			if (argc-initial_argc < 1) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *queue_name = argv[0 + initial_argc];
				cc_queue_t *queue = NULL;
				if ((queue = get_queue(queue_name))) {
					queue_rwunlock(queue);
					stream->write_function(stream, "%s", "+OK\n");
				} else {
					stream->write_function(stream, "%s", "-ERR Invalid Queue not found!\n");
				}
			}

		} else if (action && !strcasecmp(action, "unload")) {
			if (argc-initial_argc < 1) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *queue_name = argv[0 + initial_argc];
				destroy_queue(queue_name, SWITCH_FALSE);
				stream->write_function(stream, "%s", "+OK\n");

			}

		} else if (action && !strcasecmp(action, "reload")) {
			if (argc-initial_argc < 1) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *queue_name = argv[0 + initial_argc];
				cc_queue_t *queue = NULL;
				destroy_queue(queue_name, SWITCH_FALSE);
				if ((queue = get_queue(queue_name))) {
					queue_rwunlock(queue);
					stream->write_function(stream, "%s", "+OK\n");
				} else {
					stream->write_function(stream, "%s", "-ERR Invalid Queue not found!\n");
				}
			}

		} else if (action && !strcasecmp(action, "list")) {
			/* queue list */
			if (argc-initial_argc < 1) {
				switch_hash_index_t *hi;
				stream->write_function(stream, "%s", "name|strategy|moh_sound|time_base_score|tier_rules_apply|tier_rule_wait_second|tier_rule_wait_multiply_level|tier_rule_no_agent_no_wait|discard_abandoned_after|abandoned_resume_allowed|max_wait_time|max_wait_time_with_no_agent|max_wait_time_with_no_agent_time_reached|record_template\n");
				switch_mutex_lock(globals.mutex);
				for (hi = switch_hash_first(NULL, globals.queue_hash); hi; hi = switch_hash_next(hi)) {
					void *val = NULL;
					const void *key;
					switch_ssize_t keylen;
					cc_queue_t *queue;
					switch_hash_this(hi, &key, &keylen, &val);
					queue = (cc_queue_t *) val;
					stream->write_function(stream, "%s|%s|%s|%s|%s|%d|%s|%s|%d|%s|%d|%d|%d|%s\n", queue->name, queue->strategy, queue->moh, queue->time_base_score, (queue->tier_rules_apply?"true":"false"), queue->tier_rule_wait_second, (queue->tier_rule_wait_multiply_level?"true":"false"), (queue->tier_rule_no_agent_no_wait?"true":"false"), queue->discard_abandoned_after, (queue->abandoned_resume_allowed?"true":"false"), queue->max_wait_time, queue->max_wait_time_with_no_agent, queue->max_wait_time_with_no_agent_time_reached, queue->record_template);
					queue = NULL;
				}
				switch_mutex_unlock(globals.mutex);
				stream->write_function(stream, "%s", "+OK\n");
				goto done;
			} else {
				const char *sub_action = argv[0 + initial_argc];
				const char *queue_name = argv[1 + initial_argc];
				const char *status = NULL;
				struct list_result cbt;

				/* queue list agents */
				if (sub_action && !strcasecmp(sub_action, "agents")) {
					if (argc-initial_argc > 2) {
						status = argv[2 + initial_argc];
					}
					if (status)	{
						sql = switch_mprintf("SELECT agents.* FROM agents,tiers WHERE tiers.agent = agents.name AND tiers.queue = '%q' AND agents.status = '%q'", queue_name, status);
					} else {
						sql = switch_mprintf("SELECT agents.* FROM agents,tiers WHERE tiers.agent = agents.name AND tiers.queue = '%q'", queue_name);
					}
				/* queue list members */
				} else if (sub_action && !strcasecmp(sub_action, "members")) {
					sql = switch_mprintf("SELECT * FROM members WHERE queue = '%q';", queue_name);
				/* queue list tiers */
				} else if (sub_action && !strcasecmp(sub_action, "tiers")) {
					sql = switch_mprintf("SELECT * FROM tiers WHERE queue = '%q';", queue_name);
				} else {
					stream->write_function(stream, "%s", "-ERR Invalid!\n");
					goto done;
				}

				cbt.row_process = 0;
				cbt.stream = stream;
				cc_execute_sql_callback(NULL /* queue */, NULL /* mutex */, sql, list_result_callback, &cbt /* Call back variables */);
				switch_safe_free(sql);
				stream->write_function(stream, "%s", "+OK\n");
			}

		} else if (action && !strcasecmp(action, "count")) {
			/* queue count */
			if (argc-initial_argc < 1) {
				switch_hash_index_t *hi;
				int queue_count = 0;
				switch_mutex_lock(globals.mutex);
				for (hi = switch_hash_first(NULL, globals.queue_hash); hi; hi = switch_hash_next(hi)) {
					queue_count++;
				}
				switch_mutex_unlock(globals.mutex);
				stream->write_function(stream, "%d\n", queue_count);
				goto done;
			} else {
				const char *sub_action = argv[0 + initial_argc];
				const char *queue_name = argv[1 + initial_argc];
				const char *status = NULL;
				char res[256] = "";

				/* queue count agents */
				if (sub_action && !strcasecmp(sub_action, "agents")) {
					if (argc-initial_argc > 2) {
						status = argv[2 + initial_argc];
					}
					if (status)	{
						sql = switch_mprintf("SELECT count(*) FROM agents,tiers WHERE tiers.agent = agents.name AND tiers.queue = '%q' AND agents.status = '%q'", queue_name, status);
					} else {
						sql = switch_mprintf("SELECT count(*) FROM agents,tiers WHERE tiers.agent = agents.name AND tiers.queue = '%q'", queue_name);
					}
				/* queue count members */
				} else if (sub_action && !strcasecmp(sub_action, "members")) {
					sql = switch_mprintf("SELECT count(*) FROM members WHERE queue = '%q';", queue_name);
				/* queue count tiers */
				} else if (sub_action && !strcasecmp(sub_action, "tiers")) {
					sql = switch_mprintf("SELECT count(*) FROM tiers WHERE queue = '%q';", queue_name);
				} else {
					stream->write_function(stream, "%s", "-ERR Invalid!\n");
					goto done;
				}

				cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));
				switch_safe_free(sql);
				stream->write_function(stream, "%d\n", atoi(res));
			}
		}
	} else if (section && !strcasecmp(section, "operator")) {
		if (action && !strcasecmp(action, "add")) {
			if (argc-initial_argc < 1) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
                const char *name = argv[0 + initial_argc];
                const char *agent = argv[1 + initial_argc];
                
                if (argc-initial_argc == 1) {
                    agent = "";
                }
                
				switch (cc_operator_add(name, agent)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					case CC_STATUS_OPERATOR_ALREADY_EXIST:
						stream->write_function(stream, "%s", "-ERR Operator already exist!\n");
						goto done;
					case CC_STATUS_AGENT_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR Agent not found!\n");
						goto done;
					case CC_STATUS_AGENT_ALREADY_BIND:
						stream->write_function(stream, "%s", "-ERR Agent already bind to operator!\n");
						goto done;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;

				}
			}

		} else if (action && !strcasecmp(action, "del")) {
			if (argc-initial_argc < 1) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *operators = argv[0 + initial_argc];
				switch (cc_operator_del(operators)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "reload")) {
			if (argc-initial_argc < 1) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *operators = argv[0 + initial_argc];
				switch (load_operator(operators)) {
					case SWITCH_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "set")) {
			if (argc-initial_argc < 2) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *key = argv[0 + initial_argc];
				const char *operators = argv[1 + initial_argc];
				const char *value = argv[2 + initial_argc];
                
                if (argc-initial_argc == 2) {
                    value = "";
                }

				switch (cc_operator_update(key, value, operators)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					case CC_STATUS_OPERATOR_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR Invalid Operator not found!\n");
						goto done;
					case CC_STATUS_AGENT_ALREADY_BIND:
						stream->write_function(stream, "%s", "-ERR Invalid Agent already bind!\n");
						goto done;
					case CC_STATUS_INVALID_KEY:
						stream->write_function(stream, "%s", "-ERR Invalid Operator Update KEY!\n");
						goto done;
					case CC_STATUS_AGENT_NOT_FOUND:	
						stream->write_function(stream, "%s", "-ERR Agent not found!\n");
						goto done;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "get")) {
			if (argc-initial_argc < 2) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *key = argv[0 + initial_argc];
				const char *name = argv[1 + initial_argc];
				char ret[64];
                
                if (!strcasecmp(key, "operator")) {
                    switch (cc_operator_get_operator(name, ret, sizeof(ret))) {
                        case CC_STATUS_SUCCESS:
                            stream->write_function(stream, "%s", ret);
                            break;
                        default:
                            stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
                            goto done;
                    }
                } else {
                    switch (cc_operator_get(key, name, ret, sizeof(ret))) {
                        case CC_STATUS_SUCCESS:
                            stream->write_function(stream, "%s", ret);
                            break;
						case CC_STATUS_AGENT_NOT_FOUND:
							stream->write_function(stream, "%s", "-ERR Agent not bound!\n");
							goto done;
						case CC_STATUS_INVALID_KEY:
                            stream->write_function(stream, "%s", "-ERR Invalid Operator Update KEY!\n");
                            goto done;
                        case CC_STATUS_OPERATOR_NOT_FOUND:
                            stream->write_function(stream, "%s", "-ERR Operator not found!\n");
                            goto done;
                        default:
                            stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
                            goto done;
                    }
                }
			}

		} else if (action && !strcasecmp(action, "list")) {
			struct list_result cbt;
			cbt.row_process = 0;
			cbt.stream = stream;
			if ( argc-initial_argc > 1 ) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else if ( argc-initial_argc == 1 ) {
				sql = switch_mprintf("SELECT * FROM operators WHERE name='%q'", argv[0 + initial_argc]);
			} else {
				sql = switch_mprintf("SELECT * FROM operators");
			}
			cc_execute_sql_callback(NULL /* queue */, NULL /* mutex */, sql, list_result_callback, &cbt /* Call back variables */);
			switch_safe_free(sql);
			stream->write_function(stream, "%s", "+OK\n");
            
		} else if (action && !strcasecmp(action, "status")) {
			if (argc-initial_argc < 3) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *operators = argv[0 + initial_argc];
				const char *agent  = argv[1 + initial_argc];
                const char *status = argv[2 + initial_argc];
                cc_status_t result = CC_STATUS_SUCCESS;

                switch_bool_t blLogOn  = (switch_bool_t)!strcasecmp(status, cc_agent_status2str(CC_AGENT_STATUS_LOGGED_ON));
                switch_bool_t blLogOut = (switch_bool_t)!strcasecmp(status, cc_agent_status2str(CC_AGENT_STATUS_LOGGED_OUT));
                
                if (blLogOn) {
                    result = cc_operator_logon(operators, agent);
                } else if (blLogOut) {
                    result = cc_operator_logout(operators, agent);
                } else {
                // normal status change
                    result = cc_agent_update("status", status, agent);
                }
                
                switch (result) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
                    case CC_STATUS_OPERATOR_NOT_FOUND:
                        stream->write_function(stream, "%s", "-ERR Operator not found!\n");
                        goto done;
                    case CC_STATUS_OPERATOR_ALREADY_BIND:
                        stream->write_function(stream, "%s", "-ERR Operator already found!\n");
                        goto done;
                        
					case CC_STATUS_AGENT_INVALID_STATUS:
						stream->write_function(stream, "%s", "-ERR Invalid Agent Status!\n");
						goto done;
					case CC_STATUS_AGENT_INVALID_STATE:
						stream->write_function(stream, "%s", "-ERR Invalid Agent State!\n");
						goto done;
					case CC_STATUS_AGENT_INVALID_TYPE:
						stream->write_function(stream, "%s", "-ERR Invalid Agent Type!\n");
						goto done;
					case CC_STATUS_AGENT_NOT_FOUND:	
						stream->write_function(stream, "%s", "-ERR Agent not found!\n");
						goto done;
                    case CC_STATUS_AGENT_ALREADY_BIND:
                        stream->write_function(stream, "%s", "-ERR Operator: Agent already bind!\n");
                        goto done;
                        
					case CC_STATUS_INVALID_KEY:
						stream->write_function(stream, "%s", "-ERR Invalid Agent Update KEY!\n");
						goto done;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
                }
                
                /*
                if (blLogOut) {
                    agent = "";
                }
                
                // 1st: bound/unBound
                if (blLogOn || blLogOut) {
                    switch (cc_operator_update("agent", agent, operator)) {
                        case CC_STATUS_SUCCESS:
                            stream->write_function(stream, "%s", "+OK\n");
                            break;
                        case CC_STATUS_OPERATOR_NOT_FOUND:
                            stream->write_function(stream, "%s", "-ERR Operator: Operator not found!\n");
                            goto done;
                        case CC_STATUS_AGENT_ALREADY_BIND:
                            stream->write_function(stream, "%s", "-ERR Operator: Agent already bind!\n");
                            goto done;
                        case CC_STATUS_INVALID_KEY:
                            stream->write_function(stream, "%s", "-ERR Operator: Operator Update KEY!\n");
                            goto done;
                        case CC_STATUS_AGENT_NOT_FOUND:	
                            stream->write_function(stream, "%s", "-ERR Operator: Agent not found!\n");
                            goto done;
                        default:
                            stream->write_function(stream, "%s", "-ERR Operator: Unknown Error!\n");
                            goto done;
                    }
                }
                
                if (blLogOut) {
                    agent = argv[1 + initial_argc];
                }
                // 2nd: modify agent.status
				switch (cc_agent_update("status", status, agent)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					case CC_STATUS_AGENT_INVALID_STATUS:
						stream->write_function(stream, "%s", "-ERR Agent: Invalid Agent Status!\n");
						goto done;
					case CC_STATUS_AGENT_INVALID_STATE:
						stream->write_function(stream, "%s", "-ERR Agent: Invalid Agent State!\n");
						goto done;
					case CC_STATUS_AGENT_INVALID_TYPE:
						stream->write_function(stream, "%s", "-ERR Agent: Invalid Agent Type!\n");
						goto done;
					case CC_STATUS_INVALID_KEY:
						stream->write_function(stream, "%s", "-ERR Agent: Invalid Agent Update KEY!\n");
						goto done;
					case CC_STATUS_AGENT_NOT_FOUND:	
						stream->write_function(stream, "%s", "-ERR Agent: Agent not found!\n");
						goto done;
					default:
						stream->write_function(stream, "%s", "-ERR Agent: Unknown Error!\n");
						goto done;
				}
                */
			}

		} else if (action && !strcasecmp(action, "clean")) {
			if (argc-initial_argc < 1) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
                const char *operators = argv[0 + initial_argc];
                if (strcasecmp(operators, "all")) {
                // clean this operator
                    switch (cc_operator_del(operators)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
                    }
                } else {
                // clean table
                    switch_xml_t cfg, xml, x_queues, x_queue, x_agents, x_agent, x_operators, x_operator, x_vdns, x_vdn;
                    
                    if (!(xml = switch_xml_open_cfg(global_cf, &cfg, NULL))) {
                        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Open of %s failed\n", global_cf);
                        goto done;
                    }
                    
                    sql = switch_mprintf("DELETE FROM members;"
                                         "DELETE FROM agents;"
                                         "DELETE FROM vdn;"
                                         "DELETE FROM tiers;"
                                         "DELETE FROM opgroup;"
                                         "DELETE FROM operators;");
                    cc_execute_sql(NULL, sql, NULL);
                    switch_safe_free(sql);
                    
                    // re-create table
                    /** can not drop/create table inside product environment.
                    cc_execute_sql(NULL, tiers_sql, NULL);
                    cc_execute_sql(NULL, opgroup_sql, NULL);
                    cc_execute_sql(NULL, operators_sql, NULL);
                    */
                    
                    // reload data
                    switch_mutex_lock(globals.mutex);
                    
                    /* Loading queue into memory struct */
                    if ((x_queues = switch_xml_child(cfg, "queues"))) {
                        for (x_queue = switch_xml_child(x_queues, "queue"); x_queue; x_queue = x_queue->next) {
                            load_queue(switch_xml_attr_soft(x_queue, "name"));
                        }
                    }
                    
                    /* Importing from XML config Agents */
                    if ((x_agents = switch_xml_child(cfg, "agents"))) {
                        for (x_agent = switch_xml_child(x_agents, "agent"); x_agent; x_agent = x_agent->next) {
                            const char *agent = switch_xml_attr(x_agent, "name");
                            if (agent) {
                                load_agent(agent);
                            }
                        }
                    }
                    
                    /* Importing from XML config Operators */
                    if ((x_operators = switch_xml_child(cfg, "operators"))) {
                        for (x_operator = switch_xml_child(x_operators, "operator"); x_operator; x_operator = x_operator->next) {
                            const char *operators = switch_xml_attr(x_operator, "name");
                            if (operators) {
                                load_operator(operators);
                            }
                        }
                    }
                    
                    /* Importing from XML config opgroup */
                    load_opgroup(SWITCH_TRUE, NULL, NULL);

                    /* Importing from XML config vdn */
                    if ((x_vdns = switch_xml_child(cfg, "vdns"))) {
                        for (x_vdn = switch_xml_child(x_vdns, "vdn"); x_vdn; x_vdn = x_vdn->next) {
                            const char *vdnname = switch_xml_attr(x_vdn, "name");
                            const char *queue = switch_xml_attr(x_vdn, "queue");
                            
                            // for HA, only add new iterms
                            if (vdnname) {
                                cc_vdn_add(vdnname, queue);
                            }
                        }
                    }
                    
                    switch_mutex_unlock(globals.mutex);

                    stream->write_function(stream, "%s", "+OK\n");
                }
            }
        }

    } else if (section && !strcasecmp(section, "qcontrol")) {
		if (action && !strcasecmp(action, "add")) {
			if (argc-initial_argc < 3) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
                const char *name = argv[0 + initial_argc];
                const char *control = argv[1 + initial_argc];
                const char *disp_num= argv[2 + initial_argc];
                
				switch (cc_qcontrol_add(name, control, disp_num)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					case CC_STATUS_QUEUE_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR Queue not found!\n");
						goto done;
					case CC_STATUS_QUEUE_CONTROL_ALREADY_EXIST:
						stream->write_function(stream, "%s", "-ERR Queue control already exist!\n");
						goto done;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;

				}
			}

		} else if (action && !strcasecmp(action, "del")) {
			if (argc-initial_argc < 1) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *queue = argv[0 + initial_argc];
                
				switch (cc_qcontrol_del(queue)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "set")) {
			if (argc-initial_argc < 3) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *key = argv[0 + initial_argc];
				const char *queue = argv[1 + initial_argc];
				const char *value = argv[2 + initial_argc];
                
				switch (cc_qcontrol_update(key, value, queue)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					case CC_STATUS_QUEUE_CONTROL_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR Invalid QControl not found!\n");
						goto done;
					case CC_STATUS_INVALID_KEY:
						stream->write_function(stream, "%s", "-ERR Invalid QControl Update KEY!\n");
						goto done;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "get")) {
			if (argc-initial_argc < 2) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *key = argv[0 + initial_argc];
				const char *queue_name = argv[1 + initial_argc];
				char ret[64];
                
				switch (cc_qcontrol_get(key, queue_name, ret, sizeof(ret))) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", ret);
						break;
					case CC_STATUS_QUEUE_CONTROL_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR QControl not found!\n");
						goto done;
					case CC_STATUS_INVALID_KEY:
						stream->write_function(stream, "%s", "-ERR Invalid QControl KEY!\n");
						goto done;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "list")) {
			struct list_result cbt;
			cbt.row_process = 0;
			cbt.stream = stream;
            
			if ( argc-initial_argc > 1 ) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else if ( argc-initial_argc == 1 ) {
				sql = switch_mprintf("SELECT * FROM qcontrol WHERE name='%q'", argv[0 + initial_argc]);
			} else {
				sql = switch_mprintf("SELECT * FROM qcontrol");
			}
			cc_execute_sql_callback(NULL /* queue */, NULL /* mutex */, sql, list_result_callback, &cbt /* Call back variables */);
			switch_safe_free(sql);
			stream->write_function(stream, "%s", "+OK\n");
            
		}

    } else if (section && !strcasecmp(section, "opcontrol")) {
		if (action && !strcasecmp(action, "add")) {
			if (argc-initial_argc < 3) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
                const char *name = argv[0 + initial_argc];
                const char *control = argv[1 + initial_argc];
                const char *disp_num= argv[2 + initial_argc];
                
				switch (cc_opcontrol_add(name, control, disp_num)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					case CC_STATUS_OPERATOR_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR Operator not found!\n");
						goto done;
					case CC_STATUS_OPERATOR_CONTROL_ALREADY_EXIST:
						stream->write_function(stream, "%s", "-ERR Operator control already exist!\n");
						goto done;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;

				}
			}

		} else if (action && !strcasecmp(action, "del")) {
			if (argc-initial_argc < 1) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *operators = argv[0 + initial_argc];
                
				switch (cc_opcontrol_del(operators)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "set")) {
			if (argc-initial_argc < 3) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *key = argv[0 + initial_argc];
				const char *operators = argv[1 + initial_argc];
				const char *value = argv[2 + initial_argc];
                
				switch (cc_opcontrol_update(key, value, operators)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					case CC_STATUS_OPERATOR_CONTROL_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR Invalid OpControl not found!\n");
						goto done;
					case CC_STATUS_INVALID_KEY:
						stream->write_function(stream, "%s", "-ERR Invalid OpControl Update KEY!\n");
						goto done;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "get")) {
			if (argc-initial_argc < 2) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *key = argv[0 + initial_argc];
				const char *operators = argv[1 + initial_argc];
				char ret[64];
                
				switch (cc_opcontrol_get(key, operators, ret, sizeof(ret))) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", ret);
						break;
					case CC_STATUS_OPERATOR_CONTROL_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR OpControl not found!\n");
						goto done;
					case CC_STATUS_INVALID_KEY:
						stream->write_function(stream, "%s", "-ERR Invalid QControl KEY!\n");
						goto done;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "list")) {
			struct list_result cbt;
			cbt.row_process = 0;
			cbt.stream = stream;
			if ( argc-initial_argc > 1 ) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else if ( argc-initial_argc == 1 ) {
				sql = switch_mprintf("SELECT * FROM opcontrol WHERE name='%q'", argv[0 + initial_argc]);
			} else {
				sql = switch_mprintf("SELECT * FROM opcontrol");
			}
			cc_execute_sql_callback(NULL /* queue */, NULL /* mutex */, sql, list_result_callback, &cbt /* Call back variables */);
			switch_safe_free(sql);
			stream->write_function(stream, "%s", "+OK\n");
            
		}

    } else if (section && !strcasecmp(section, "opgroup")) {
		if (action && !strcasecmp(action, "add")) {
			if (argc-initial_argc < 4) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *queue_name = argv[0 + initial_argc];
				const char *operators = argv[1 + initial_argc];
				const char *level = argv[2 + initial_argc];
				const char *position = argv[3 + initial_argc];

				switch(cc_opgroup_item_add(queue_name, operators, atoi(level), atoi(position))) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					case CC_STATUS_QUEUE_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR Queue not found!\n");
						goto done;
					case CC_STATUS_OPERATOR_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR Operator not found!\n");
						goto done;
					case CC_STATUS_OPERATORGROUP_ALREADY_EXIST:
						stream->write_function(stream, "%s", "-ERR Operator queue already exist!\n");
						goto done;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "set")) {
			if (argc-initial_argc < 4) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *key = argv[0 + initial_argc];
				const char *queue_name = argv[1 + initial_argc];
				const char *operators = argv[2 + initial_argc];
				const char *value = argv[3 + initial_argc];

				switch(cc_opgroup_item_update(key, value, queue_name, operators)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					case CC_STATUS_OPERATORGROUP_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR Operator not found!\n");
						goto done;
					case CC_STATUS_QUEUE_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR Queue not found!\n");
						goto done;
					case CC_STATUS_INVALID_KEY:
						stream->write_function(stream, "%s", "-ERR Invalid Tier Update KEY!\n");
						goto done;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "get")) {
			if (argc-initial_argc < 2) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *key = argv[0 + initial_argc];
				const char *operators = argv[1 + initial_argc];
				char ret[64];

				switch(cc_opgroup_item_get(key, operators, ret, sizeof(ret))) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", ret);
						break;
					case CC_STATUS_INVALID_KEY:
						stream->write_function(stream, "%s", "-ERR Invalid Tier KEY!\n");
						goto done;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "del")) {
			if (argc-initial_argc < 2) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done; 
			} else {
				const char *queue = argv[0 + initial_argc];
				const char *operators = argv[1 + initial_argc];
				switch (cc_opgroup_item_del(queue, operators)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;

				}
			}

		} else if (action && !strcasecmp(action, "reload")) {
			if (argc-initial_argc < 1) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *queue = argv[0 + initial_argc];
				const char *operators = argv[1 + initial_argc];
				switch_bool_t load_all = SWITCH_FALSE;
				if (!strcasecmp(queue, "all")) {
					load_all = SWITCH_TRUE;
				}
				switch (load_opgroup(load_all, queue, operators)) {
					case SWITCH_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;

				}
			}

		} else if (action && !strcasecmp(action, "list")) {
			struct list_result cbt;
			cbt.row_process = 0;
			cbt.stream = stream;
			sql = switch_mprintf("SELECT * FROM opgroup ORDER BY level, position");
			cc_execute_sql_callback(NULL /* queue */, NULL /* mutex */, sql, list_result_callback, &cbt /* Call back variables */);
			switch_safe_free(sql);
			stream->write_function(stream, "%s", "+OK\n");
		}
        
	} else if (section && !strcasecmp(section, "vdn")) {
		if (action && !strcasecmp(action, "add")) {
			if (argc-initial_argc < 2) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
                const char *name = argv[0 + initial_argc];
                const char *queue = argv[1 + initial_argc];
                
				switch (cc_vdn_add(name, queue)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					case CC_STATUS_VDN_ALREADY_EXIST:
						stream->write_function(stream, "%s", "-ERR VDN already exist!\n");
						goto done;
					case CC_STATUS_QUEUE_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR Queue not found!\n");
						goto done;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;

				}
			}

		} else if (action && !strcasecmp(action, "del")) {
			if (argc-initial_argc < 1) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *vdn = argv[0 + initial_argc];
				switch (cc_vdn_del(vdn)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "reload")) {
			if (argc-initial_argc < 1) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *vdn = argv[0 + initial_argc];
                
                if (zstr(vdn)) {
                    goto done;
                }
                
				switch (load_vdn(vdn)) {
					case SWITCH_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "set")) {
			if (argc-initial_argc < 3) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *key = argv[0 + initial_argc];
				const char *vdn = argv[1 + initial_argc];
				const char *value = argv[2 + initial_argc];
                
				switch (cc_vdn_update(key, value, vdn)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					case CC_STATUS_VDN_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR Invalid VDN not found!\n");
						goto done;
					case CC_STATUS_QUEUE_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR Invalid Queue not found!\n");
						goto done;
					case CC_STATUS_INVALID_KEY:
						stream->write_function(stream, "%s", "-ERR Invalid VDN Update KEY!\n");
						goto done;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "get")) {
			if (argc-initial_argc < 2) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *key = argv[0 + initial_argc];
				const char *vdn = argv[1 + initial_argc];
				char ret[64];
                
				switch (cc_vdn_get(key, vdn, ret, sizeof(ret))) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", ret);
						break;
					case CC_STATUS_VDN_NOT_FOUND:
						stream->write_function(stream, "%s", "-ERR VDN not found!\n");
						goto done;
					case CC_STATUS_INVALID_KEY:
						stream->write_function(stream, "%s", "-ERR Invalid VDN KEY!\n");
						goto done;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "list")) {
			struct list_result cbt;
			cbt.row_process = 0;
			cbt.stream = stream;
			if ( argc-initial_argc > 1 ) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else if ( argc-initial_argc == 1 ) {
				sql = switch_mprintf("SELECT * FROM vdn WHERE name='%q'", argv[0 + initial_argc]);
			} else {
				sql = switch_mprintf("SELECT * FROM vdn");
			}
			cc_execute_sql_callback(NULL /* queue */, NULL /* mutex */, sql, list_result_callback, &cbt /* Call back variables */);
			switch_safe_free(sql);
			stream->write_function(stream, "%s", "+OK\n");
            
        }

    } else if (section && !strcasecmp(section, "ha")) {
        if (action && !strcasecmp(action, "set")) {
			if (argc-initial_argc < 2) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *key = argv[0 + initial_argc];
				const char *value = argv[1 + initial_argc];
                
				switch (cc_ha_set(key, value)) {
					case CC_STATUS_SUCCESS:
						stream->write_function(stream, "%s", "+OK\n");
						break;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "get")) {
			if (argc-initial_argc < 1) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *key = argv[0 + initial_argc];
				char ret[64];
                
				switch (cc_ha_get(key, ret, sizeof(ret))) {
					case CC_STATUS_SUCCESS:
                        stream->write_function(stream, "%s", ret);
						break;
					default:
						stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
						goto done;
				}
			}

		} else if (action && !strcasecmp(action, "recover")) {
            switch (cc_ha_recover()) {
                case CC_STATUS_SUCCESS:
                    stream->write_function(stream, "%s", "+OK\n");
                    break;
                default:
                    stream->write_function(stream, "%s", "-ERR Unknown Error!\n");
                    goto done;
            }

		} else if (action && !strcasecmp(action, "show")) {
			if (argc-initial_argc < 1) {
				stream->write_function(stream, "%s", "-ERR Invalid!\n");
				goto done;
			} else {
				const char *key = argv[0 + initial_argc];
                
                if (!strcasecmp(key, "master")) {
                    if (argc-initial_argc == 2) {
                        get_queue_context_cli(stream, argv[1 + initial_argc]);
                    } else {
                        get_queue_context_cli(stream, (char *)"");
                    }
                }
                
                stream->write_function(stream, "%s", "+OK\n");
			}

		}
    }

	goto done;
done:

	free(mydata);

	return SWITCH_STATUS_SUCCESS;
}

int acd_config_init(switch_loadable_module_interface_t **module_interface, switch_api_interface_t *api_interface) {
	SWITCH_ADD_API(api_interface, "callcenter_config", "Config of callcenter", cc_config_api_function, CC_CONFIG_API_SYNTAX);

	switch_console_set_complete("add callcenter_config agent add");
	switch_console_set_complete("add callcenter_config agent del");
	switch_console_set_complete("add callcenter_config agent set status");
	switch_console_set_complete("add callcenter_config agent set state");
	switch_console_set_complete("add callcenter_config agent set uuid");
	switch_console_set_complete("add callcenter_config agent set contact");
	switch_console_set_complete("add callcenter_config agent set ready_time");
	switch_console_set_complete("add callcenter_config agent set reject_delay_time");
	switch_console_set_complete("add callcenter_config agent set busy_delay_time");
	switch_console_set_complete("add callcenter_config agent set no_answer_delay_time");
	switch_console_set_complete("add callcenter_config agent get status");
	switch_console_set_complete("add callcenter_config agent list");
/*
	switch_console_set_complete("add callcenter_config tier add");
	switch_console_set_complete("add callcenter_config tier del");
	switch_console_set_complete("add callcenter_config tier set state");
	switch_console_set_complete("add callcenter_config tier set level");
	switch_console_set_complete("add callcenter_config tier set position");
*/
	switch_console_set_complete("add callcenter_config tier list");

	switch_console_set_complete("add callcenter_config operator add");
	switch_console_set_complete("add callcenter_config operator del");
	switch_console_set_complete("add callcenter_config operator get agent");
	switch_console_set_complete("add callcenter_config operator set agent");
	switch_console_set_complete("add callcenter_config operator reload");
	switch_console_set_complete("add callcenter_config operator list");
	switch_console_set_complete("add callcenter_config operator status");
    
	switch_console_set_complete("add callcenter_config opgroup add");
	switch_console_set_complete("add callcenter_config opgroup del");
	switch_console_set_complete("add callcenter_config opgroup set level");
	switch_console_set_complete("add callcenter_config opgroup set position");
	switch_console_set_complete("add callcenter_config opgroup reload");
	switch_console_set_complete("add callcenter_config opgroup list");
    
	switch_console_set_complete("add callcenter_config vdn add");
	switch_console_set_complete("add callcenter_config vdn del");
	switch_console_set_complete("add callcenter_config vdn get queue");
	switch_console_set_complete("add callcenter_config vdn set queue");
	switch_console_set_complete("add callcenter_config vdn reload");
	switch_console_set_complete("add callcenter_config vdn list");
    
	switch_console_set_complete("add callcenter_config qcontrol add");
	switch_console_set_complete("add callcenter_config qcontrol del");
	switch_console_set_complete("add callcenter_config qcontrol get control");
	switch_console_set_complete("add callcenter_config qcontrol get disp_num");
	switch_console_set_complete("add callcenter_config qcontrol set control");
	switch_console_set_complete("add callcenter_config qcontrol set disp_num");
	switch_console_set_complete("add callcenter_config qcontrol list");
    
	switch_console_set_complete("add callcenter_config opcontrol add");
	switch_console_set_complete("add callcenter_config opcontrol del");
	switch_console_set_complete("add callcenter_config opcontrol get control");
	switch_console_set_complete("add callcenter_config opcontrol get disp_num");
	switch_console_set_complete("add callcenter_config opcontrol set control");
	switch_console_set_complete("add callcenter_config opcontrol set disp_num");
	switch_console_set_complete("add callcenter_config opcontrol list");
   
	switch_console_set_complete("add callcenter_config queue load");
	switch_console_set_complete("add callcenter_config queue unload");
	switch_console_set_complete("add callcenter_config queue reload");
	switch_console_set_complete("add callcenter_config queue list");
	switch_console_set_complete("add callcenter_config queue list agents");
	switch_console_set_complete("add callcenter_config queue list members");
	switch_console_set_complete("add callcenter_config queue list tiers");
	switch_console_set_complete("add callcenter_config queue count");
	switch_console_set_complete("add callcenter_config queue count agents");
	switch_console_set_complete("add callcenter_config queue count members");
	switch_console_set_complete("add callcenter_config queue count tiers");


}
