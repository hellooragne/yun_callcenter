action = argv[1]
--calleeNum = argv[2]

http = require('httpAgent')
route  = require('route')
io     = require('io')

settings  = require('settings')
authority = require('authority')
operator  = require('operator')
route     = require('route')
callee    = require('callee')
show      = require('show')
c_event   = require('c_event')
findagent = require('findagent')

utils = require('c_utils')

callId = (session == nil) and "0" or session:get_uuid()
--print(action)

--route
local dbh = freeswitch.Dbh("odbc://systeminfo::")

local f = io.popen("cd; pwd")
local l = string.sub(f:read("*a"), 1, -2)
local p = "/node_modules/config_callout_control/fs_conf/"

if action == 'reload' then
	settings.load(dbh, l..p)
elseif action == 'lreload' then
	callee.reload(dbh, l..p.."phone_location.csv")
elseif action == 'authority' then
	show.show(dbh, argv[2])
	authority.run(dbh, argv[2], argv[3])
elseif action == 'route' then
	route.run(dbh, argv[2], argv[3])
elseif action == 'operator' then
	operator.run(dbh, argv[2])
elseif action == 'callee' then
	callee.run(dbh,argv[2])
elseif action == 'event' then
	utils.print_msg("info", "0", "event")
	c_event.run(dbh)
elseif action == 'show' then
	show.show(dbh, argv[2])
elseif action == 'showall' then
	show.showall(dbh)
elseif action == 'findagent' then	
	findagent.setagent(dbh,argv[2])
else
	utils.print_msg("info", callId, 'lua CALLOUT_CONTROL/index.lua  reload ')
	utils.print_msg("info", callId, 'lua CALLOUT_CONTROL/index.lua  lreload ')
	utils.print_msg("info", callId, 'lua CALLOUT_CONTROL/index.lua  authority [agent] [ability]')
	utils.print_msg("info", callId, 'lua CALLOUT_CONTROL/index.lua  route     [agent]')
	utils.print_msg("info", callId, 'lua CALLOUT_CONTROL/index.lua  show      [agent]')
end
