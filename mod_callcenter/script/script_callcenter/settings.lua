json = require('json')
lfs  = require("lfs")
utils = require('c_utils')

callId = (session == nil) and "0" or session:get_uuid()

local settings = {}


function get_file_content(file_name)
	local config_str = ""
	local file_content = io.open(file_name, "r")
	for line in file_content:lines() do  
		config_str = config_str .. line
	end  

	file_content:close()

	return config_str
end

function load_to_db_init(dbh)
	local sql_queue = "CREATE TABLE `callout_queue` ("
	.."`queue_id` int(11) NOT NULL AUTO_INCREMENT,"
	.."`name` varchar(25) NOT NULL,"
	.."`authority_name` varchar(25) ,"
	.."`line_name` varchar(25),"
	.."`line_point` varchar(25),"
	.."`line_pri` varchar(25),"
	.."PRIMARY KEY (`queue_id`),"
	.."UNIQUE KEY `queue_id_UNIQUE` (`queue_id`)) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1 COMMENT=''"

	dbh:query("delete from callout_queue")
	dbh:query(sql_queue)

	local sql_phone = "CREATE TABLE `callout_phone` ("
	.."`phone_id` int(11) NOT NULL AUTO_INCREMENT,"
	.."`name` varchar(25) NOT NULL,"
	.."`authority_name` varchar(25) ,"
	.."`line_name` varchar(25),"
	.."`line_point` varchar(25),"
	.."`line_pri` varchar(25),"
	.."PRIMARY KEY (`phone_id`),"
	.."UNIQUE KEY `phone_id_UNIQUE` (`phone_id`)) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1 COMMENT=''"

	dbh:query("delete from  callout_phone")
	dbh:query(sql_phone)

	local sql_operator = "CREATE TABLE `callout_operator` ("
	.."`operator_id` int(11) NOT NULL AUTO_INCREMENT,"
	.."`name` varchar(25) NOT NULL,"
	.."`authority_name` varchar(25) ,"
	.."`line_name` varchar(25),"
	.."`line_point` varchar(25),"
	.."`line_pri` varchar(25),"
	.."PRIMARY KEY (`operator_id`),"
	.."UNIQUE KEY `operator_id_UNIQUE` (`operator_id`)) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1 COMMENT=''"

	dbh:query("delete from  callout_operator")
	dbh:query(sql_operator)

	local sql_authority = "CREATE TABLE `callout_authority` ("
	.."`authority_id` int(11) NOT NULL AUTO_INCREMENT,"
	.."`authority_name` varchar(25) ,"
	.."`authority_value` varchar(25),"
	.."PRIMARY KEY (`authority_id`),"
	.."UNIQUE KEY `authority_name_UNIQUE` (`authority_name`)) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1 COMMENT=''"

	dbh:query("delete from  callout_authority")
	dbh:query(sql_authority)

	local sql_line = "CREATE TABLE `callout_line` ("
	.."`line_id` int(11) NOT NULL AUTO_INCREMENT,"
	.."`line_name` varchar(25) ,"
	.."`caller_number` varchar(25) ,"
	.."`caller_number_bak` varchar(25) ,"
	.."`route_code` varchar(25) ,"
	.."PRIMARY KEY (`line_id`),"
	.."UNIQUE KEY `line_name_UNIQUE` (`line_name`)) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1 COMMENT=''"

	dbh:query("delete from callout_line")
	dbh:query(sql_line)

	local sql_relation = "CREATE TABLE `callout_relation` ("
	.."`relation_id` int(11) NOT NULL AUTO_INCREMENT,"
	.."`queue_name` varchar(25) ,"
	.."`phone_name` varchar(25) ,"
	.."`operator_name` varchar(25) ,"
	.."PRIMARY KEY (`relation_id`),"
	.."UNIQUE KEY `relation_id_UNIQUE` (`relation_id`)) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1 COMMENT=''"

	dbh:test_reactive("SELECT count(*) FROM callout_relation", "DROP TABLE callout_relation", sql_relation)
end

function load_to_db_queue(dbh, config)
	if config['queue'] ~= nil then
		for key, value in pairs(config['queue']) do  

			if value['name'] == nil then
				return
			end

			name = value['name']
			authority = value['authority']

			if value['route'] ~= nil and authority ~= nil then
				if #value['route'] == 0 then
					sql = string.format("insert into callout_queue (name, authority_name, line_name, line_point, line_pri) values ('%s', '%s', '%s', '%s', '%s')", name, authority, "", "", "")

					utils.print_msg("info", callId, sql)
					dbh:query(sql)
				end

				for key2, value2 in pairs(value['route']) do
					if value2['line_name'] ~= nil or  value2['line_point'] ~= nil or value2['line_pri'] ~= nil then
						sql = string.format("insert into callout_queue (name, authority_name, line_name, line_point, line_pri) values ('%s', '%s', '%s', '%s', '%s')", name, authority, value2['line_name'], value2['line_point'], value2['line_pri'])

						utils.print_msg("info", callId, sql)
						dbh:query(sql)
					end
				end
			elseif value['route'] ~= nil and authority == nil then
				for key2, value2 in pairs(value['route']) do
					if value2['line_name'] ~= nil or  value2['line_point'] ~= nil or value2['line_pri'] ~= nil then
						sql = string.format("insert into callout_queue (name, authority_name, line_name, line_point, line_pri) values ('%s', '%s', '%s', '%s', '%s')", name, "", value2['line_name'], value2['line_point'], value2['line_pri'])
						utils.print_msg("info", callId, sql)
						dbh:query(sql)
					end
				end
			elseif value['route'] == nil and authority ~= nil then
				sql = string.format("insert into callout_queue (name, authority_name, line_name, line_point, line_pri) values ('%s', '%s', '%s', '%s', '%s')", name, authority, "", "", "")

				utils.print_msg("info", callId, sql)
				dbh:query(sql)
			end
		end 
	end
end



function load_to_db_phone(dbh, config)
	if config['phone'] ~= nil then
		for key, value in pairs(config['phone']) do  

			if value['name'] == nil then
				return
			end

			name = value['name']
			authority = value['authority']

			if value['route'] ~= nil then
				for key2, value2 in pairs(value['route']) do
					if value2['line_name'] ~= nil or  value2['line_point'] ~= nil or value2['line_pri'] ~= nil then
						sql = string.format("insert into callout_phone (name, authority_name, line_name, line_point, line_pri) values ('%s', '%s', '%s', '%s', '%s')", name, authority, value2['line_name'], value2['line_point'], value2['line_pri'])
						utils.print_msg("info", callId, sql)
						dbh:query(sql)
					end
				end
			else
				sql = string.format("insert into callout_phone (name, authority_name, line_name, line_point, line_pri) values ('%s', '%s', '%s', '%s', '%s')", name, authority, "", "", "")

				utils.print_msg("info", callId, sql)
				dbh:query(sql)
			end
		end
	end
end

function load_to_db_operator(dbh, config)
	if config['operator'] ~= nil then
		for key, value in pairs(config['operator']) do  

			if value['name'] == nil then
				return
			end

			name = value['name']
			authority = value['authority']
			if value['route'] ~= nil then
				for key2, value2 in pairs(value['route']) do
					if value2['line_name'] ~= nil or  value2['line_point'] ~= nil or value2['line_pri'] ~= nil then
						sql = string.format("insert into callout_operator (name, authority_name, line_name, line_point, line_pri) values ('%s', '%s', '%s', '%s', '%s')", name, authority, value2['line_name'], value2['line_point'], value2['line_pri'])
						utils.print_msg("info", callId, sql)
						dbh:query(sql)
					end
				end
			else
				sql = string.format("insert into callout_operator (name, authority_name, line_name, line_point, line_pri) values ('%s', '%s', '%s', '%s', '%s')", name, authority, "", "", "")

				utils.print_msg("info", callId, sql)
				dbh:query(sql)
			end
		end
	end
end

function load_to_db_authoritys(dbh, config)

	if config['authority'] == nil then
		return
	end

	for key, value in pairs(config['authority']) do  
		authority_name  = value['authority_name']
		authority_value = value['value']
		sql = string.format("insert into callout_authority (authority_name, authority_value) values ('%s', '%s')", authority_name, authority_value)

		utils.print_msg("info", callId, sql)
		dbh:query(sql)
	end
end

function load_to_db_lines(dbh, config)

	if config['lines'] == nil then
		return
	end

	for key, value in pairs(config['lines']) do  
		local line_name         = value['line_name']
		local caller_number     = value['caller_number']
		local caller_number_bak = value['caller_number_bak']
		local route_code = value['route_code']
		sql = string.format("insert into callout_line (line_name, caller_number, caller_number_bak, route_code) values ('%s', '%s', '%s', '%s')", line_name, caller_number, caller_number_bak, route_code)

		utils.print_msg("info", callId, sql)
		dbh:query(sql)
	end

end

function load_to_db(dbh, config)

	load_to_db_queue(dbh, config)
	load_to_db_phone(dbh, config)
	load_to_db_operator(dbh, config)
	load_to_db_authoritys(dbh, config)
	load_to_db_lines(dbh, config)

end

function load_one_config(dbh, config_name)

	local config_str = get_file_content(config_name)	

	local config_context = json.decode(config_str)

	--print(config_context['queue'][1]["name"])
	--table.foreach(config_context['queue'][1], print)
	
	if config_context ~= nil then
		load_to_db(dbh, config_context)
	end
end

function settings.load(dbh, config_path)

	load_to_db_init(dbh)

	for file in lfs.dir(config_path) do
		if file ~= "." and file ~= ".." then
			file_type = string.sub(file, -5, -1) 
			if string.find(file_type, '.json') then
				local f = config_path .. "/" .. file
				print(f)
				load_one_config(dbh, f)
			end
		end
	end
	
end

return settings
