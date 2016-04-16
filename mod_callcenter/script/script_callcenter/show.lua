utils = require('c_utils')

local show = {}

callId = (session == nil) and "0" or session:get_uuid()

function show.show(dbh, id)
	local sql_login = string.format("select * from callout_relation where  phone_name = '%s' or queue_name = '%s' or operator_name = '%s'", id, id, id)

	utils.print_msg("debug", callId, "show relation")
	utils.print_msg("info", callId, "|queue_name|phone_name|operator_name|")
	dbh:query(sql_login, function(row)
		--for k,v in pairs(row) do print(k,v) end
		--print(row.c)
		utils.print_msg("info", callId, "|" .. row.queue_name .. "|" .. row.phone_name .. "|" .. row.operator_name .. "|")
	end)
end

function show.showall(dbh)
	local sql_login = string.format("select * from callout_relation")

	utils.print_msg("debug", callId, "show all relation")
	utils.print_msg("info", callId, "|queue_name|phone_name|operator_name|")
	dbh:query(sql_login, function(row)
		--for k,v in pairs(row) do print(k,v) end
		--print(row.c)
		utils.print_msg("info", callId, "|" .. row.queue_name .. "|" .. row.phone_name .. "|" .. row.operator_name .. "|")
	end)
end

return show

