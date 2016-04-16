local route = {}

utils = require('c_utils')

callId = (session == nil) and "0" or session:get_uuid()

function route_get_default(dbh, caller_phone_number, callee_phone_number)
	local sql_route = string.format("select * from (select distinct line_name, line_point, line_pri from  callout_queue where name = 'default') t left join callout_line on t.line_name = callout_line.line_name")

	utils.print_msg("info", callId, "route get default caller  " .. caller_phone_number .. "  callee  " .. callee_phone_number)
	utils.print_msg("debug", callId, sql_route)
	local res = {}
	dbh:query(sql_route, function(row)
		--for k,v in pairs(row) do print(k,v) end
		table.insert(res, row)
	end)

	return res	
end

function route_get_queue(dbh, caller_phone_number, callee_phone_number)
	local sql_route = string.format("select * from (select distinct line_name, line_point, line_pri from callout_relation left join callout_queue on callout_relation.queue_name = callout_queue.name where callout_relation.phone_name = '%s' and line_name != '') t left join callout_line on t.line_name = callout_line.line_name", caller_phone_number)

	utils.print_msg("info", callId, "route get queue caller " .. caller_phone_number .. "  callee  " .. callee_phone_number)

	utils.print_msg("debug", callId, sql_route)
	local res = {}
	dbh:query(sql_route, function(row)
		table.insert(res, row)
	end)

	return res	
end

function route_get_phone(dbh, caller_phone_number, callee_phone_number)

	local sql_route = string.format("select * from (select * from callout_phone where callout_phone.name = '%s' and callout_phone.line_name != '') t left join callout_line on t.line_name = callout_line.line_name", caller_phone_number)

	utils.print_msg("info", callId, "route get phone caller " .. caller_phone_number .. "  callee  " .. callee_phone_number)

	utils.print_msg("debug", "0", sql_route)
	local res = {}
	dbh:query(sql_route, function(row)
		table.insert(res, row)
	end)

	return res

end

function route_get_operator(dbh, caller_phone_number, callee_phone_number)

	local sql_route = string.format("select * from (select distinct line_name, line_point, line_pri from callout_relation left join callout_operator on callout_relation.operator_name = callout_operator.name where callout_relation.phone_name = '%s'and line_name != '')  t left join callout_line on t.line_name = callout_line.line_name", caller_phone_number)

	utils.print_msg("info", callId, "route get operator caller " .. caller_phone_number .. "  callee  " .. callee_phone_number)

	utils.print_msg("debug", "0", sql_route)

	local res = {}
	dbh:query(sql_route, function(row)
		table.insert(res, row)
	end)

	return res
end

function set_route(route)
	if session ~= nil then
		if #route >= 1 then
			session:setVariable("callout_caller_number_1", route[1].caller_number)
			session:setVariable("callout_tac_1", string.sub(route[1].route_code, -3, -1))
			session:setVariable("callout_route_1", route[1].route_code)

			utils.print_msg("info", callId, "route set 1 caller_number  " .. route[1].caller_number ..  "  route_code  " .. route[1].route_code)
		end


		if #route >= 2 then
			session:setVariable("callout_caller_number_2", route[2].caller_number)
			session:setVariable("callout_tac_2", string.sub(route[2].route_code, -3, -1))
			session:setVariable("callout_route_2", route[2].route_code)

			utils.print_msg("info", callId, "route set 2 caller_number  " .. route[2].caller_number ..  "  route_code  " .. route[2].route_code)
		end

		if #route >= 3 then
			session:setVariable("callout_caller_number_3", route[3].caller_number)
			session:setVariable("callout_tac_3", string.sub(route[3].route_code, -3, -1))
			session:setVariable("callout_route_3", route[3].route_code)

			utils.print_msg("info", callId, "route set 3 caller_number  " .. route[3].caller_number ..  "  route_code  " .. route[3].route_code)
		end

		session:setVariable("callout_line_count", #route)
	end
end

function route_sort_function(a, b)
	if a.line_pri == "" then
		return  false
	end

	if b.line_pri == "" then
		return  true
	end

	return a.line_pri < b.line_pri
end

function route.run(dbh, caller_phone_number, callee_phone_number)
	local route = nil

	if route == nil or #route == 0 then
		route = route_get_operator(dbh, caller_phone_number, callee_phone_number)
	end

	if route == nil or #route == 0 then
		route = route_get_phone(dbh, caller_phone_number, callee_phone_number)
	end

	if route == nil or #route == 0 then
		route = route_get_queue(dbh, caller_phone_number, callee_phone_number)
	end
	
	if route == nil or #route == 0 then
		route = route_get_default(dbh, caller_phone_number, callee_phone_number)
	end

	if route ~= nil or #route ~= 0 then
		table.sort(route, route_sort_function)
		set_route(route)
		--[[
		for k,v in pairs(route) do 
			print('------------------------------')
			for k2,v2 in pairs(v) do
				print(k2, v2)	
			end
		end
		--]]
	end
end

function route_show_relation(dbh, number)
	local sql_route = string.format("select * from (select distinct line_name, line_point, line_pri from callout_relation left join callout_queue on callout_relation.queue_name = callout_queue.name where callout_relation.phone_name = '%s') t left join callout_line on t.line_name = callout_line.line_name", caller_phone_number)

	local res = {}
	dbh:query(sql_route, function(row)
		table.insert(res, row)
	end)

	return res	
end


function route.show(dbh, number)
	
end

return route

