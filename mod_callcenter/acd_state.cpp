#include "acd_state.h"

static struct cc_state_table STATE_CHART[] = {
	{"Unknown", CC_TIER_STATE_UNKNOWN},
	{"No Answer", CC_TIER_STATE_NO_ANSWER},
	{"Ready", CC_TIER_STATE_READY},
	{"Offering", CC_TIER_STATE_OFFERING},
	{"Active Inbound", CC_TIER_STATE_ACTIVE_INBOUND},
	{"Standby", CC_TIER_STATE_STANDBY},
	{NULL, 0}

};


static struct cc_status_table AGENT_STATUS_CHART[] = {
	{"Unknown", CC_AGENT_STATUS_UNKNOWN},
	{"Logged Out", CC_AGENT_STATUS_LOGGED_OUT},
	{"Available", CC_AGENT_STATUS_AVAILABLE},
	{"Available (On Demand)", CC_AGENT_STATUS_AVAILABLE_ON_DEMAND},
	{"On Break", CC_AGENT_STATUS_ON_BREAK},
	{NULL, 0}

};

static struct cc_state_table AGENT_STATE_CHART[] = {
	{"Unknown", CC_AGENT_STATE_UNKNOWN},
	{"Waiting", CC_AGENT_STATE_WAITING},
	{"Receiving", CC_AGENT_STATE_RECEIVING},
	{"In a queue call", CC_AGENT_STATE_IN_A_QUEUE_CALL},
	{"Idle", CC_AGENT_STATE_IDLE},
	{NULL, 0}

};


static struct cc_state_table MEMBER_STATE_CHART[] = {
	{"Unknown", CC_MEMBER_STATE_UNKNOWN},
	{"Waiting", CC_MEMBER_STATE_WAITING},
	{"Trying", CC_MEMBER_STATE_TRYING},
	{"Answered", CC_MEMBER_STATE_ANSWERED},
	{"Abandoned", CC_MEMBER_STATE_ABANDONED},
	{NULL, 0}

};


static struct cc_member_cancel_reason_table MEMBER_CANCEL_REASON_CHART[] = {
	{"NONE", CC_MEMBER_CANCEL_REASON_NONE},
	{"TIMEOUT", CC_MEMBER_CANCEL_REASON_TIMEOUT},
	{"NO_AGENT_TIMEOUT", CC_MEMBER_CANCEL_REASON_NO_AGENT_TIMEOUT},
	{"BREAK_OUT", CC_MEMBER_CANCEL_REASON_BREAK_OUT},
	{NULL, 0}
};


/*tier status */
const char * cc_tier_state2str(cc_tier_state_t state)
{
	uint8_t x;
	const char *str = "Unknown";

	for (x = 0; x < (sizeof(STATE_CHART) / sizeof(struct cc_state_table)) - 1; x++) {
		if (STATE_CHART[x].state == state) {
			str = STATE_CHART[x].name;
			break;
		}
	}

	return str;
}


cc_tier_state_t cc_tier_str2state(const char *str)
{
	uint8_t x;
	cc_tier_state_t state = CC_TIER_STATE_UNKNOWN;

	for (x = 0; x < (sizeof(STATE_CHART) / sizeof(struct cc_state_table)) - 1 && STATE_CHART[x].name; x++) {
		if (!strcasecmp(STATE_CHART[x].name, str)) {
			state = (cc_tier_state_t)STATE_CHART[x].state;
			break;
		}
	}
	return state;
}

/*agent status */
const char * cc_agent_status2str(cc_agent_status_t status)
{
	uint8_t x;
	const char *str = "Unknown";

	for (x = 0; x < (sizeof(AGENT_STATUS_CHART) / sizeof(struct cc_status_table)) - 1; x++) {
		if (AGENT_STATUS_CHART[x].status == status) {
			str = AGENT_STATUS_CHART[x].name;
			break;
		}
	}

	return str;
}

/*agent state*/
cc_agent_status_t cc_agent_str2status(const char *str)
{
	uint8_t x;
	cc_agent_status_t status = CC_AGENT_STATUS_UNKNOWN;

	for (x = 0; x < (sizeof(AGENT_STATUS_CHART) / sizeof(struct cc_status_table)) - 1 && AGENT_STATUS_CHART[x].name; x++) {
		if (!strcasecmp(AGENT_STATUS_CHART[x].name, str)) {
			status = (cc_agent_status_t)AGENT_STATUS_CHART[x].status;
			break;
		}
	}
	return status;
}


const char * cc_agent_state2str(cc_agent_state_t state)
{
	uint8_t x;
	const char *str = "Unknown";

	for (x = 0; x < (sizeof(AGENT_STATE_CHART) / sizeof(struct cc_state_table)) - 1; x++) {
		if (AGENT_STATE_CHART[x].state == state) {
			str = AGENT_STATE_CHART[x].name;
			break;
		}
	}

	return str;
}

cc_agent_state_t cc_agent_str2state(const char *str)
{
	uint8_t x;
	cc_agent_state_t state = CC_AGENT_STATE_UNKNOWN;

	for (x = 0; x < (sizeof(AGENT_STATE_CHART) / sizeof(struct cc_state_table)) - 1 && AGENT_STATE_CHART[x].name; x++) {
		if (!strcasecmp(AGENT_STATE_CHART[x].name, str)) {
			state = (cc_agent_state_t)AGENT_STATE_CHART[x].state;
			break;
		}
	}
	return state;
}

/*member state*/
const char * cc_member_state2str(cc_member_state_t state)
{
	uint8_t x;
	const char *str = "Unknown";

	for (x = 0; x < (sizeof(MEMBER_STATE_CHART) / sizeof(struct cc_state_table)) - 1; x++) {
		if (MEMBER_STATE_CHART[x].state == state) {
			str = MEMBER_STATE_CHART[x].name;
			break;
		}
	}

	return str;
}

cc_member_state_t cc_member_str2state(const char *str)
{
	uint8_t x;
	cc_member_state_t state = CC_MEMBER_STATE_UNKNOWN;

	for (x = 0; x < (sizeof(MEMBER_STATE_CHART) / sizeof(struct cc_state_table)) - 1 && MEMBER_STATE_CHART[x].name; x++) {
		if (!strcasecmp(MEMBER_STATE_CHART[x].name, str)) {
			state = (cc_member_state_t)MEMBER_STATE_CHART[x].state;
			break;
		}
	}
	return state;
}


const char * cc_member_cancel_reason2str(cc_member_cancel_reason_t reason)
{
	uint8_t x;
	const char *str = "NONE";

	for (x = 0; x < (sizeof(MEMBER_CANCEL_REASON_CHART) / sizeof(struct cc_member_cancel_reason_table)) - 1; x++) {
		if (MEMBER_CANCEL_REASON_CHART[x].reason == reason) {
			str = MEMBER_CANCEL_REASON_CHART[x].name;
			break;
		}
	}

	return str;
}

cc_member_cancel_reason_t cc_member_cancel_str2reason(const char *str)
{
	uint8_t x;
	cc_member_cancel_reason_t reason = CC_MEMBER_CANCEL_REASON_NONE;

	for (x = 0; x < (sizeof(MEMBER_CANCEL_REASON_CHART) / sizeof(struct cc_member_cancel_reason_table)) - 1 && MEMBER_CANCEL_REASON_CHART[x].name; x++) {
		if (!strcasecmp(MEMBER_CANCEL_REASON_CHART[x].name, str)) {
			reason = (cc_member_cancel_reason_t)MEMBER_CANCEL_REASON_CHART[x].reason;
			break;
		}
	}
	return reason;
}


