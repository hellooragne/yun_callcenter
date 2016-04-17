#include "acd_tools.h"


/* TODO This is temporary until we either move it to the core, or use it differently in the module */
switch_time_t local_epoch_time_now(switch_time_t *t)
{
	switch_time_t now = switch_micro_time_now() / 1000000; /* APR_USEC_PER_SEC */
	if (t) {
		*t = now;
	}
	return now;
}

switch_cache_db_handle_t *cc_get_db_handle(void)
{
	switch_cache_db_handle_t *dbh = NULL;
	char *dsn;
	
	if (!zstr(globals.odbc_dsn)) {
		dsn = globals.odbc_dsn;
	} else {
		dsn = globals.dbname;
	}

	if (switch_cache_db_get_db_handle_dsn(&dbh, dsn) != SWITCH_STATUS_SUCCESS) {
		dbh = NULL;
	}
	
	return dbh;

}

void playback_array(switch_core_session_t *session, const char *str) {
	if (str && !strncmp(str, "ARRAY::", 7)) {
		char *i = (char*) str + 7, *j = i;
		while (1) {
			if ((j = strstr(i, "::"))) {
				*j = 0;
			}
			switch_ivr_play_file(session, NULL, i, NULL);
			if (!j) break;
			i = j + 2;
		}
	} else {
		switch_ivr_play_file(session, NULL, str, NULL);
	}
}

char *cc_execute_sql2str(cc_queue_t *queue, switch_mutex_t *mutex, char *sql, char *resbuf, size_t len)
{
	char *ret = NULL;

	switch_cache_db_handle_t *dbh = NULL;

	if (mutex) {
		switch_mutex_lock(mutex);
	} else {
		switch_mutex_lock(globals.mutex);
	}

	if (!(dbh = cc_get_db_handle())) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Error Opening DB\n");
		goto end;
	}

	ret = switch_cache_db_execute_sql2str(dbh, sql, resbuf, len, NULL);

end:
	switch_cache_db_release_db_handle(&dbh);

	if (mutex) {
		switch_mutex_unlock(mutex);
	} else {
		switch_mutex_unlock(globals.mutex);
	}

	return ret;
}


switch_status_t cc_execute_sql(cc_queue_t *queue, char *sql, switch_mutex_t *mutex)
{
	switch_cache_db_handle_t *dbh = NULL;
	switch_status_t status = SWITCH_STATUS_FALSE;

	if (mutex) {
		switch_mutex_lock(mutex);
	} else {
		switch_mutex_lock(globals.mutex);
	}

	if (!(dbh = cc_get_db_handle())) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Error Opening DB\n");
		goto end;
	}

	status = switch_cache_db_execute_sql(dbh, sql, NULL);

end:

	switch_cache_db_release_db_handle(&dbh);

	if (mutex) {
		switch_mutex_unlock(mutex);
	} else {
		switch_mutex_unlock(globals.mutex);
	}

	return status;
}


switch_bool_t cc_execute_sql_callback(cc_queue_t *queue, switch_mutex_t *mutex, char *sql, switch_core_db_callback_func_t callback, void *pdata)
{
	switch_bool_t ret = SWITCH_FALSE;
	char *errmsg = NULL;
	switch_cache_db_handle_t *dbh = NULL;

	if (mutex) {
		switch_mutex_lock(mutex);
	} else {
		switch_mutex_lock(globals.mutex);
	}

	if (!(dbh = cc_get_db_handle())) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Error Opening DB\n");
		goto end;
	}

	switch_cache_db_execute_sql_callback(dbh, sql, callback, pdata, &errmsg);

	if (errmsg) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "SQL ERR: [%s] %s\n", sql, errmsg);
		free(errmsg);
	}

end:

	switch_cache_db_release_db_handle(&dbh);

	if (mutex) {
		switch_mutex_unlock(mutex);
	} else {
		switch_mutex_unlock(globals.mutex);
	}

	return ret;
}


// DO NOT use this function. This is for checking data inside coredb ONLY. Add by Tom LIANG.
char *cc_coredb_execute_sql2str(switch_mutex_t *mutex, char *sql, char *resbuf, size_t len)
{
	char *ret = NULL;

	switch_cache_db_handle_t *dbh = NULL;

	if (mutex) {
		switch_mutex_lock(mutex);
	} else {
		switch_mutex_lock(globals.mutex);
	}

    if (switch_core_db_handle(&dbh) != SWITCH_STATUS_SUCCESS) { 
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "-ERR Error Opening CoreDB!\n"); 
        goto end; 
    }

    //char *sql = switch_mprintf("select url from registrations where reg_user='%q'", m_sLocalId); 
    //char ret[1024]; 
	ret = switch_cache_db_execute_sql2str(dbh, sql, resbuf, len, NULL);

end:
	switch_cache_db_release_db_handle(&dbh);

	if (mutex) {
		switch_mutex_unlock(mutex);
	} else {
		switch_mutex_unlock(globals.mutex);
	}

	return ret;
}


//add by djxie
unsigned int get_waiting_members_count(const char *queue_name)
{
	char res[256] = {0};
	char *sql;

	if (!queue_name)
	{
		return 0;
	}

	sql = switch_mprintf("SELECT count(*) FROM members WHERE queue = '%q' AND state = 'Waiting'", queue_name);

	cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));

	switch_safe_free(sql);

	return atoi(res);
}

unsigned int get_trying_members_count(const char *queue_name)
{
	char res[256] = {0};
	char *sql;

	if (!queue_name)
	{
		return 0;
	}

	sql = switch_mprintf("SELECT count(*) FROM members WHERE queue = '%q' AND state = 'Trying'", queue_name);

	cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));

	switch_safe_free(sql);

	return atoi(res);
}

unsigned int get_answered_members_count(const char *queue_name)
{
	char res[256] = {0};
	char *sql;

	if (!queue_name)
	{
		return 0;
	}

	sql = switch_mprintf("SELECT count(*) FROM members WHERE queue = '%q' AND state = 'Answered'", queue_name);

	cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));

	switch_safe_free(sql);

	return atoi(res);
}

unsigned int get_available_agents_count(const char *queue_name)
{
	char res[256] = {0};
	char *sql;

	if (!queue_name)
	{
		return 0;
	}

	sql = switch_mprintf("SELECT count(*) FROM agents, tiers" 
			" WHERE tiers.agent = agents.name"
			" AND queue = '%q'"
			" AND agents.status = 'Available'"
			" AND agents.state = 'Waiting'"
			" And tiers.state = 'Ready'",
			queue_name);

	cc_execute_sql2str(NULL, NULL, sql, res, sizeof(res));

	switch_safe_free(sql);

	return atoi(res);	
}


void get_queue_context()
{
	switch_hash_index_t *hi;

	switch_mutex_lock(globals.mutex);
    
    //switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "-----------------------------------------\n");
    
	for (hi = switch_hash_first(NULL, globals.queue_hash); hi; hi = switch_hash_next(hi)) 
	{
		void *val = NULL;
		unsigned int waiting_members_count;
		unsigned int trying_members_count;
		unsigned int answered_members_count;
		unsigned int available_agents_count;
		const void *key;
		switch_ssize_t keylen;
		cc_queue_t *queue;
		switch_hash_this(hi, &key, &keylen, &val);
		queue = (cc_queue_t *) val;
        
		waiting_members_count = get_waiting_members_count(queue->name);
		trying_members_count = get_trying_members_count(queue->name);
		answered_members_count = get_answered_members_count(queue->name);
		available_agents_count = get_available_agents_count(queue->name);
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "queue: %s, waiting_members: %d, trying_members: %d, answered_members: %d, available_agents: %d\n", 
				queue->name, 
				waiting_members_count, 
				trying_members_count,
				answered_members_count,
				available_agents_count
				);
	}
	switch_mutex_unlock(globals.mutex);
}

void get_queue_context_cli(switch_stream_handle_t *stream, char *queuename)
{
	switch_hash_index_t *hi;
    switch_bool_t  blQueue = SWITCH_FALSE;
    
    if (!switch_strlen_zero(queuename)) {
        blQueue = SWITCH_TRUE;
    }

	switch_mutex_lock(globals.mutex);
    
    stream->write_function(stream, "-----------------------------------------\n");
    
	for (hi = switch_hash_first(NULL, globals.queue_hash); hi; hi = switch_hash_next(hi)) 
	{
		void *val = NULL;
		unsigned int waiting_members_count;
		unsigned int trying_members_count;
		unsigned int answered_members_count;
		unsigned int available_agents_count;
		const void *key;
		switch_ssize_t keylen;
		cc_queue_t *queue;
		switch_hash_this(hi, &key, &keylen, &val);
		queue = (cc_queue_t *) val;
        
        if (blQueue && strcasecmp(queuename, queue->name)) {
            continue;
        }
        
		waiting_members_count = get_waiting_members_count(queue->name);
		trying_members_count = get_trying_members_count(queue->name);
		answered_members_count = get_answered_members_count(queue->name);
		available_agents_count = get_available_agents_count(queue->name);
        //stream->write_function(stream, "%s", "+OK\n");
        stream->write_function(stream, "queue: %s, waiting_members: %d, trying_members: %d, answered_members: %d, available_agents: %d\n", 
				queue->name, 
				waiting_members_count, 
				trying_members_count,
				answered_members_count,
				available_agents_count
				);
		/*
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "queue: %s, waiting_members: %d, trying_members: %d, answered_members: %d, available_agents: %d\n", 
				queue->name, 
				waiting_members_count, 
				trying_members_count,
				answered_members_count,
				available_agents_count
				);*/
	}
	switch_mutex_unlock(globals.mutex);
}
//end add by djxie
