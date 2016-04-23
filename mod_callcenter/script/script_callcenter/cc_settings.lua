json = require('json')
lfs  = require("lfs")
utils = require('c_utils')

api = freeswitch.API()

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


	print("load to db init")
	utils.print_msg("info", callId, "load to db init")

	local sql_vdn = "CREATE TABLE vdn ("
	.."   name     VARCHAR(255),"
	.."   queue    VARCHAR(255))";

	dbh:query("delete from vdn")

	dbh:test_reactive("SELECT count(*) FROM vdn", "DROP TABLE vdn", sql_vdn)


	local sql_queue = "CREATE TABLE cc_queue ("
	.."   name											VARCHAR(255),"
	.."   strtegy										VARCHAR(255),"
	.."   moh_sound										VARCHAR(255),"
	.."   time_base_score								VARCHAR(255),"
	.."   max_wait_time									VARCHAR(255),"
	.."   max_wait_time_with_no_agent     			   	VARCHAR(255),"
	.."   max_wait_time_with_no_agent_time_reached     	VARCHAR(255),"
	.."   tier_rules_apply     							VARCHAR(255),"
	.."   discard_abandoned_after     					VARCHAR(255),"
	.."   abandoned_resume_allowed     					VARCHAR(255))"

	dbh:query("delete from cc_queue")

	dbh:test_reactive("SELECT count(*) FROM cc_queue", "DROP TABLE cc_queue", sql_queue)

	local sql_tiers = "CREATE TABLE tiers ("
	.."   queue    VARCHAR(255),"
	.."   agent    VARCHAR(255),"
	.."   state    VARCHAR(255),"
	.."   level    INTEGER NOT NULL DEFAULT 1,"
	.."   position INTEGER NOT NULL DEFAULT 1)"

	dbh:query("delete from tiers")

	dbh:test_reactive("SELECT count(*) FROM tiers", "DROP TABLE tiers", sql_tiers)

	local sql_agent = "CREATE TABLE agents ("
	.."   name      VARCHAR(255),"
	.."   system    VARCHAR(255),"
	.."   uuid      VARCHAR(255),"
	.."   type      VARCHAR(255),"
	.."   contact   VARCHAR(255),"
	.."   status    VARCHAR(255),"
	.."   state   VARCHAR(255),"
	.."   max_no_answer INTEGER NOT NULL DEFAULT 0,"
	.."   wrap_up_time INTEGER NOT NULL DEFAULT 0,"
	.."   reject_delay_time INTEGER NOT NULL DEFAULT 0,"
	.."   busy_delay_time INTEGER NOT NULL DEFAULT 0,"
	.."   no_answer_delay_time INTEGER NOT NULL DEFAULT 0,"
	.."   last_bridge_start INTEGER NOT NULL DEFAULT 0,"
	.."   last_bridge_end INTEGER NOT NULL DEFAULT 0,"
	.."   last_offered_call INTEGER NOT NULL DEFAULT 0," 
	.."   last_status_change INTEGER NOT NULL DEFAULT 0,"
	.."   no_answer_count INTEGER NOT NULL DEFAULT 0,"
	.."   calls_answered  INTEGER NOT NULL DEFAULT 0,"
	.."   talk_time  INTEGER NOT NULL DEFAULT 0,"
	.."   ready_time INTEGER NOT NULL DEFAULT 0,"
	.."PRIMARY KEY (`name`));";

	dbh:test_reactive("SELECT count(*) FROM agents", "DROP TABLE agents", sql_agent)

end

function load_to_db_queue(dbh, config)
	if config['queue'] == nil then
		return
	end

	for key, value in pairs(config['queue']) do  
		local name = value['name']
		local strtegy = value['strtegy']
		local moh_sound = value['moh_sound']
		local time_base_score = value['time_base_score']
		local max_wait_time = value['max_wait_time']
		local max_wait_time_with_no_agent = value['max_wait_time_with_no_agent']
		local max_wait_time_with_no_agent_time_reached = value['max_wait_time_with_no_agent_time_reached']
		local tier_rules_apply = value['tier_rules_apply']
		local discard_abandoned_after = value['discard_abandoned_after']
		local abandoned_resume_allowed = value['abandoned_resume_allowed']
	


		sql = string.format("insert into cc_queue (name, strtegy, moh_sound, time_base_score, max_wait_time,max_wait_time_with_no_agent, max_wait_time_with_no_agent_time_reached,tier_rules_apply,discard_abandoned_after,abandoned_resume_allowed) values ('%s', '%s', '%s', '%s','%s', '%s', '%s', '%s', '%s', '%s')", name, strtegy, moh_sound, time_base_score, max_wait_time,max_wait_time_with_no_agent, max_wait_time_with_no_agent_time_reached,tier_rules_apply,discard_abandoned_after,abandoned_resume_allowed);

		utils.print_msg("info", callId, sql)

		dbh:query(sql)

		api:executeString("callcenter_config queue load "..name)
	end
end

function load_to_db_tiers(dbh, config)
	if config['tiers'] == nil then
		return
	end

	for key, value in pairs(config['tiers']) do  
		local queue    = value['queue_name']
		local agent    = value['agent']
		local state    = value['state']
		local level    = value['level']
		local position = value['position']

		sql = string.format("insert into tiers (queue, agent,  level, position, state) values ('%s', '%s', '%s', '%s', 'Ready')", queue, agent, level, position);

		utils.print_msg("info", callId, sql)

		dbh:query(sql)

	end	
end

local agent_list = {}

function load_to_db_agent(dbh, config)

	if config['agent'] == nil then
		return
	end

	for key, value in pairs(config['agent']) do  
		local name       = value['name']
		local ntype      = value['type']
		local status     = value['status']
		local max_no_answer = value['max_no_answer']
		local wrap_up_time = value['wrap_up_time']
		local reject_delay_time = value['reject_delay_time']
		local busy_delay_time = value['busy_delay_time']


		sql = string.format("insert into agents (name, type,  status, max_no_answer, wrap_up_time,reject_delay_time,busy_delay_time,state) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', 'Waiting')", name, ntype, status, max_no_answer, wrap_up_time, reject_delay_time, busy_delay_time);

		utils.print_msg("info", callId, sql)

		dbh:query(sql)

	end	
end

function load_to_db_vdn(dbh, config)
	if config['vdn'] == nil then
		return
	end

	for key, value in pairs(config['vdn']) do  
		local name       = value['name']
		local queue_name = value['queue_name']

		sql = string.format("insert into vdn (name, queue) values ('%s', '%s')", name, queue_name);

		utils.print_msg("info", callId, sql)

		dbh:query(sql)
	end
end

function load_to_db(dbh, config)
	load_to_db_vdn(dbh, config)
	load_to_db_queue(dbh, config)
	load_to_db_tiers(dbh, config)
	load_to_db_agent(dbh, config)

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


	utils.print_msg("info", callId, "load to db settings")

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
