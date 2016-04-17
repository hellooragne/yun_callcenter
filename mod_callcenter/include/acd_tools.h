#ifndef __ACD_TOOLS__
#define __ACD_TOOLS__

#include <switch.h>
#include "acd_queue.h"

struct globals_type{
	switch_hash_t *queue_hash;
	int debug;
	char *odbc_dsn;
	char *dbname;
	int32_t threads;
	int32_t running;
	switch_mutex_t *mutex;
	switch_memory_pool_t *pool;
    switch_bool_t blMaster;
};

extern struct globals_type globals;
switch_time_t local_epoch_time_now(switch_time_t *t);

switch_cache_db_handle_t *cc_get_db_handle(void);

void playback_array(switch_core_session_t *session, const char *str);

char *cc_execute_sql2str(cc_queue_t *queue, switch_mutex_t *mutex, char *sql, char *resbuf, size_t len);

switch_status_t cc_execute_sql(cc_queue_t *queue, char *sql, switch_mutex_t *mutex);

switch_bool_t cc_execute_sql_callback(cc_queue_t *queue, switch_mutex_t *mutex, char *sql, switch_core_db_callback_func_t callback, void *pdata);


char *cc_coredb_execute_sql2str(switch_mutex_t *mutex, char *sql, char *resbuf, size_t len);

unsigned int get_waiting_members_count(const char *queue_name);

unsigned int get_trying_members_count(const char *queue_name);

unsigned int get_answered_members_count(const char *queue_name);


unsigned int get_available_agents_count(const char *queue_name);


void get_queue_context();

void get_queue_context_cli(switch_stream_handle_t *stream, char *queuename);
#endif
