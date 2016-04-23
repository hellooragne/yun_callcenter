json = require('json')
lfs  = require("lfs")
utils = require('c_utils')

callId = (session == nil) and "0" or session:get_uuid()

local cc_state = {}

function cc_state.vdn_get_queue(dbh, vdn)

	local sql_vdn = string.format("select queue from vdn where name = %s", vdn)

	utils.print_msg("debug", callId, sql_vdn)
	local res = {}
	dbh:query(sql_vdn, function(row)
		table.insert(res, row)
		utils.print_msg("debug", callId, "vdn: "..vdn.."  queue: "..row.queue)
		session:setVariable("cc_queue", row.queue)
	end)

	return res

end


return cc_state
