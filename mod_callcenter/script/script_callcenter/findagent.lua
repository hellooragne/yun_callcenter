utils = require('c_utils')
local findagent = {}

call_id = (session == nil) and "0" or session:get_uuid()

function findagent.setagent(dbh,op_num)
	local localnum = string.format("select phone_name from callout_relation where operator_name = '%s'", op_num)

	if op_num ~= nil then
		dbh:query(localnum,function(row)
			utils.print_msg("info",call_id," operater number : " .. op_num .. "get phone number: " .. row.phone_name);

			if session ~= nil then
				session:setVariable("agent",row.phone_name)
			end
		end)        
	end	
end	

return findagent
