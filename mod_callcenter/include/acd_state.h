#ifndef __ACD__H_
#define __ACD__H_

#include "acd_tools.h"

struct cc_status_table {
	const char *name;
	int status;
};

struct cc_state_table {
	const char *name;
	int state;
};

typedef enum {
	CC_STATUS_SUCCESS,
	CC_STATUS_FALSE,
	CC_STATUS_AGENT_NOT_FOUND,
	CC_STATUS_QUEUE_NOT_FOUND,
	CC_STATUS_AGENT_ALREADY_EXIST,
	CC_STATUS_AGENT_INVALID_TYPE,
	CC_STATUS_AGENT_INVALID_STATUS,
	CC_STATUS_AGENT_INVALID_STATE,
    CC_STATUS_AGENT_ALREADY_BIND,
	CC_STATUS_TIER_ALREADY_EXIST,
	CC_STATUS_TIER_NOT_FOUND,
	CC_STATUS_TIER_INVALID_STATE,
	CC_STATUS_OPERATOR_ALREADY_EXIST,
	CC_STATUS_OPERATOR_ALREADY_BIND,
    CC_STATUS_OPERATOR_ALREADY_UNBIND,
	CC_STATUS_OPERATOR_NOT_FOUND,
    CC_STATUS_OPERATORGROUP_ALREADY_EXIST,
    CC_STATUS_OPERATORGROUP_NOT_FOUND,
    CC_STATUS_VDN_ALREADY_EXIST,
    CC_STATUS_VDN_NOT_FOUND,
    CC_STATUS_QUEUE_CONTROL_ALREADY_EXIST,
    CC_STATUS_QUEUE_CONTROL_NOT_FOUND,
    CC_STATUS_OPERATOR_CONTROL_ALREADY_EXIST,
    CC_STATUS_OPERATOR_CONTROL_NOT_FOUND,
	CC_STATUS_INVALID_KEY
} cc_status_t;

typedef enum {
	CC_TIER_STATE_UNKNOWN = 0,
	CC_TIER_STATE_NO_ANSWER = 1,
	CC_TIER_STATE_READY = 2,
	CC_TIER_STATE_OFFERING = 3,
	CC_TIER_STATE_ACTIVE_INBOUND = 4,
	CC_TIER_STATE_STANDBY = 5
} cc_tier_state_t;

typedef enum {
	CC_AGENT_STATUS_UNKNOWN = 0,
	CC_AGENT_STATUS_LOGGED_OUT,
	CC_AGENT_STATUS_LOGGED_ON,
	CC_AGENT_STATUS_AVAILABLE,
	CC_AGENT_STATUS_AVAILABLE_ON_DEMAND,
	CC_AGENT_STATUS_ON_BREAK
} cc_agent_status_t;

typedef enum {
	CC_AGENT_STATE_UNKNOWN = 0,
	CC_AGENT_STATE_WAITING = 1,
	CC_AGENT_STATE_RECEIVING = 2,
	CC_AGENT_STATE_IN_A_QUEUE_CALL = 3,
    CC_AGENT_STATE_IN_A_DIRECT_CALL = 4,
	CC_AGENT_STATE_IDLE = 5
} cc_agent_state_t;

typedef enum {
	CC_MEMBER_STATE_UNKNOWN = 0,
	CC_MEMBER_STATE_WAITING = 1,
	CC_MEMBER_STATE_TRYING = 2,
	CC_MEMBER_STATE_ANSWERED = 3,
	CC_MEMBER_STATE_ABANDONED = 4
} cc_member_state_t;

typedef enum {
	CC_MEMBER_CANCEL_REASON_NONE,
	CC_MEMBER_CANCEL_REASON_TIMEOUT,
	CC_MEMBER_CANCEL_REASON_NO_AGENT_TIMEOUT,
	CC_MEMBER_CANCEL_REASON_BREAK_OUT
} cc_member_cancel_reason_t;

typedef enum {
	PFLAG_DESTROY = 1 << 0
} cc_flags_t;

struct cc_member_cancel_reason_table {
	const char *name;
	int reason;
};

const char * cc_tier_state2str(cc_tier_state_t state);
cc_tier_state_t cc_tier_str2state(const char *str);
const char * cc_agent_status2str(cc_agent_status_t status);
cc_agent_status_t cc_agent_str2status(const char *str);
const char * cc_agent_state2str(cc_agent_state_t state);
cc_agent_state_t cc_agent_str2state(const char *str);
const char * cc_member_state2str(cc_member_state_t state);
cc_member_state_t cc_member_str2state(const char *str);
const char * cc_member_cancel_reason2str(cc_member_cancel_reason_t reason);
cc_member_cancel_reason_t cc_member_cancel_str2reason(const char *str);

#endif
