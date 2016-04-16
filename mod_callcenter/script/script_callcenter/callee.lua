utils = require('c_utils')
csv = require('csv')

local callee = {}

callId = (session == nil) and "0" or session:get_uuid()

function check(dbh, phone)
        local sql_check = string.format("select code, count(*) from callout_phone_location where phone = '%s' limit 1",  string.sub(phone, 1, 7))

        utils.print_msg("info", callId, sql_check)

        local phone_num = phone

        if phone ~= nil then
                dbh:query(sql_check, function(row)
                        utils.print_msg("info", callId, "phone location  [" .. phone .. "] phone_name [" .. row.code.. "]")

                        if row.code == '021' then

                        else
                                phone_num = '0' .. phone
                        end
                end)
        end

        return phone_num
end

function callee.run(dbh, phone)
	
	phone_len = string.len(phone);

	utils.print_msg("info", callId, "calee id" .. phone)

	if phone_len == 11 or phone_len == 12 then
		phone_number_first  = string.sub(phone, 1, 1)
		phone_number_second = string.sub(phone, 1, 2)

		if phone_number_first == '1' then
			phone_num = check(dbh, phone)
			session:setVariable("ctrip_callout_callee", phone_num)
			return
		elseif phone_number_second == '01' then
			phone_num = check(dbh, string.sub(phone, 2, -1))
			session:setVariable("ctrip_callout_callee", phone_num)
			return
		else
			session:setVariable("ctrip_callout_callee", phone)
			return
		end
	else
		session:setVariable("ctrip_callout_callee", phone)
		return
	end
end


function save_phone_location(dbh, file_name)
	local config_str = ""
	local j = 1


	local sql_del = "delete from callout_phone_location"

	dbh:query(sql_del, function(row) end)

	local f = csv.open(file_name)
	for fields in f:lines() do
		local config_str_line = "" 
		for i, v in ipairs(fields) do 
			if i == 1 then
				config_str_line = config_str_line .. "'" .. v .. "'"
			else
				config_str_line = config_str_line .. ",'" .. v .. "'"
			end
		end

		if j == 1 then
			config_str = config_str .. "(" .. config_str_line .. ")"
		else
			config_str = config_str .. ",(" .. config_str_line .. ")"
		end
		j = j + 1

		if j == 1000 then
			local sql_load = "insert into callout_phone_location values " .. config_str

			dbh:query(sql_load, function(row) end)
			config_str = ""
			j = 1
		end
	end

	return config_str
end

function callee.reload(dbh, file_name)

	local sql = "CREATE TABLE `callout_phone_location` ("
	.."`prefix` varchar(100) ,"
	.."`phone` varchar(100) ,"
	.."`p` varchar(100) ,"
	.."`c` varchar(100) ,"
	.."`isp` varchar(100) ,"
	.."`code` varchar(100) ,"
	.."`zip` varchar(100) ,"
	.."`types` varchar(100) )"
	.."ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1 COMMENT=''"

	dbh:test_reactive("SELECT count(*) FROM callout_phone_location", "DROP TABLE callout_phone_location", sql)

	data = save_phone_location(dbh, file_name)

end

return callee
