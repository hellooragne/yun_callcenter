v_utils = {}

function v_utils.getfunctionlocation()
	local w = debug.getinfo(2, "S")
	return "["..w.short_src..":"..w.linedefined.."] "
end


function v_utils.print_msg(level, callId, msg)
	if callId ~= nil then
		freeswitch.consoleLog(level, 'CALLOUT CONTROL [' .. callId .. '] ' .. msg .. "\n")
	else
		freeswitch.consoleLog(level, 'CALLOUT CONTROL [0] ' .. msg .. "\n")
	end
end

return v_utils
