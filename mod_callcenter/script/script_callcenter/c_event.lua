--[[
	"Event-Subclass":       "callcenter::info",
        "Event-Name":   "CUSTOM",
        "Core-UUID":    "311437d8-5546-11e5-9764-01212fceedc6",
        "FreeSWITCH-Hostname":  "SVR4936HW1288",
        "FreeSWITCH-Switchname":        "SVR4936HW1288",
        "FreeSWITCH-IPv4":      "10.253.31.1",
        "FreeSWITCH-IPv6":      "::1",
        "Event-Date-Local":     "2015-09-14 13:29:01",
        "Event-Date-GMT":       "Mon, 14 Sep 2015 05:29:01 GMT",
        "Event-Date-Timestamp": "1442208541956724",
        "Event-Calling-File":   "mod_callcenter.c",
        "Event-Calling-Function":       "cc_operator_logon",
        "Event-Calling-Line-Number":    "1923",
        "Event-Sequence":       "246266",
        "CC-Operator":  "221010",
        "CC-Queue":     "211450",
        "CC-Agent":     "231010",
        "CC-Action":    "agent-status-change",
        "CC-Agent-Status":      "LoggedOn"        
--]]

utils = require('c_utils')
local c_event = {}

function c_event.run(dbh)
	local cc_action = event:getHeader("CC-Action")
	if cc_action == 'agent-status-change' then
		local cc_status = event:getHeader("CC-Agent-Status")
		if cc_status == 'LoggedOn' then
			local cc_queue = event:getHeader("CC-Queue")
			local cc_phone = event:getHeader("CC-Agent")
			local cc_operator = event:getHeader("CC-Operator")

			if cc_queue ~= nil and cc_phone ~= nil and cc_operator ~= nil then
				sql = string.format("insert into callout_relation (queue_name, phone_name, operator_name) values ('%s', '%s', '%s')", cc_queue, cc_phone, cc_operator)
				utils.print_msg("info", "0", sql)
				dbh:query(sql)
			else
				utils.print_msg("info", "0", "loggedon paramter lost")
			end

		elseif cc_status == 'LoggedOut' then
			local cc_phone = event:getHeader("CC-Agent")

			if cc_phone ~= nil then
				sql = string.format("delete from callout_relation where phone_name = '%s'", cc_phone)
				utils.print_msg("info", "0", sql)
				dbh:query(sql)
			else
				utils.print_msg("info", "0", "loggedout paramter lost")
			end
		end
	end
end

return c_event
