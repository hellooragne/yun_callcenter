#ifndef __ACD_TOOLS__
#define __ACD_TOOLS__

#include <switch.h>

struct globals_type{
	switch_hash_t *queue_hash;
	int debug;
	char *odbc_dsn;
	char *dbname;
	int32_t threads;
	int32_t running;
	switch_mutex_t *mutex;
	switch_memory_pool_t *pool;
};

extern struct globals_type globals;
switch_time_t local_epoch_time_now(switch_time_t *t);

switch_cache_db_handle_t *cc_get_db_handle(void);

void playback_array(switch_core_session_t *session, const char *str);

char *cc_execute_sql2str(switch_mutex_t *mutex, char *sql, char *resbuf, size_t len);

switch_status_t cc_execute_sql(char *sql, switch_mutex_t *mutex);

switch_bool_t cc_execute_sql_callback(switch_mutex_t *mutex, char *sql, switch_core_db_callback_func_t callback, void *pdata);
#endif
