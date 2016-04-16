utils = require('c_utils')

local authority = {}

callId = (session == nil) and "0" or session:get_uuid()

function check_login(dbh, phone_number)
	local sql_login = string.format("select count(*) c from callout_relation where  phone_name = '%s'", phone_number)
	local is_login = '' 
	utils.print_msg("info", callId, "authority check login " .. phone_number)
	utils.print_msg("debug", callId, sql_login)
	dbh:query(sql_login, function(row)
		--for k,v in pairs(row) do print(k,v) end
		--print(row.c)
		is_login = row.c
	end)

	return is_login
end

function authority_get_queue(dbh, phone_number)
	local sql_authority = string.format("select authority_value from callout_authority where  authority_name = (select distinct callout_queue.authority_name from callout_relation left join callout_queue on callout_relation.queue_name = callout_queue.name where callout_relation.phone_name = '%s')", phone_number)
	local authority_code = '' 
	utils.print_msg("info", callId, "authority get queue " .. phone_number)
	utils.print_msg("debug", callId, sql_authority)
	dbh:query(sql_authority, function(row)
		authority_code = row.authority_value
	end)

	return authority_code
end

function authority_get_phone(dbh, phone_number)
	local sql_authority = string.format("select authority_value from callout_authority where authority_name = (select distinct authority_name from callout_phone where name = '%s')", phone_number)
	local authority_code = '' 
	utils.print_msg("info", callId, "authority get phone " .. phone_number)
	utils.print_msg("debug", callId, sql_authority)
	dbh:query(sql_authority, function(row)
		authority_code = row.authority_value
	end)

	return authority_code
end

function authority_get_operator(dbh, phone_number)
	local sql_authority = string.format("select authority_value from callout_authority where  authority_name = (select distinct callout_operator.authority_name from callout_relation left join callout_operator on callout_relation.operator_name = callout_operator.name where callout_relation.phone_name = '%s')", phone_number)
	local authority_code = '' 
	utils.print_msg("info", callId, "authority get operator " .. phone_number)
	utils.print_msg("debug", callId, sql_authority)
	dbh:query(sql_authority, function(row)
		authority_code = row.authority_value
	end)

	return authority_code
end

function authority_get(dbh, phone_number, ability)

	local authority_code = authority_get_operator(dbh, phone_number)

	if authority_code == '' then
		authority_code = authority_get_phone(dbh, phone_number)
	end

	if authority_code == '' then
		authority_code = authority_get_queue(dbh, phone_number)
	end

	return authority_code 
end

function authority.run(dbh, phone_number, ability)
	local audioLoginThenCall = "please-login-and-call-again.wav"
	local audioNoRight = "sorry-no-right.wav"

	local is_login = check_login(dbh, phone_number)

	local authority_code = authority_get(dbh, phone_number)
	local authority_ability = string.sub(authority_code, "-" .. ability, "-" .. ability)

	utils.print_msg("info", callId, 'is_login [' .. is_login .. ']' .. '  authority code [' .. authority_code .. ']' .. "  ability [" .. ability .. "] authority [" .. authority_ability .. ']')

	if is_login == '0' and authority_ability == '' and session ~= nil then

		session:answer();
		session:sayPhrase("play-ivr-msg", audioLoginThenCall, "zh")
		session:hangup("NORMAL_CLEARING")
	end

	if authority_ability == '0' and session ~= nil then
		--session:execute("playback", audioNoRight)
		session:sayPhrase("play-ivr-msg", audioNoRight, "zh")
		session:hangup("NORMAL_CLEARING")
	elseif authority_ability == '' and session ~= nil then

		session:answer();
		session:sayPhrase("play-ivr-msg", audioNoRight, "zh")
		session:hangup("NORMAL_CLEARING")
	end
end

return authority

