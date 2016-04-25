action = argv[1]
--calleeNum = argv[2]

--http = require('httpAgent')
route  = require('route')
io     = require('io')

settings  = require('cc_settings')
authority = require('authority')
operator  = require('operator')
route     = require('route')
callee    = require('callee')
show      = require('show')
c_event   = require('c_event')
findagent = require('findagent')
cc_state  = require('cc_state')

utils = require('c_utils')

callId = (session == nil) and "0" or session:get_uuid()
--print(action)

--route
local dbh = freeswitch.Dbh("odbc://freeswitch::")

local f = io.popen("cd; pwd")
local l = string.sub(f:read("*a"), 1, -2)
local p = "/node_modules/config_callout_control/fs_conf/"

if action == 'reload' then
	utils.print_msg("info", callId, "load to db init")
	settings.load(dbh, '/usr/local/freeswitch/scripts/config_callcenter/conf/')
elseif action == 'vdn_to_queue' then
	cc_state.vdn_get_queue(dbh, argv[2])
else
	utils.print_msg("info", callId, 'lua config_callcenter/index.lua  reload ')
	utils.print_msg("info", callId, 'lua config_callcenter/index.lua  lreload ')
	utils.print_msg("info", callId, 'lua config_callcenter/index.lua  authority [agent] [ability]')
	utils.print_msg("info", callId, 'lua config_callcenter/index.lua  route     [agent]')
	utils.print_msg("info", callId, 'lua config_callcenter/index.lua  show      [agent]')
end
