utils = require('c_utils')

local operator = {}

callId = (session == nil) and "0" or session:get_uuid()

function operator.run(dbh, id)
	local sql_login = string.format("select phone_name from callout_relation where operator_name = '%s'",  id)

	utils.print_msg("debug", callId, "operator_name [" .. id .. "]")

	if id ~= nil then
		dbh:query(sql_login, function(row)
			utils.print_msg("info", callId, "operator_name [" .. id .. "] phone_name [" .. row.phone_name .. "]")
			if session ~= nil then
				session:setVariable("phone_name", row.phone_name)
			end
		end)
	end
end

return operator

