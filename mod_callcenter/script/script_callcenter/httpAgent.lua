local http  = require("socket.http")
local ltn12 = require("ltn12")
local json  = require("json")

local httpAgent = {}
function httpAgent.post(_url, _postData)
    local r,c 
    local responseBody = {}
    local data = json.encode(_postData)

	freeswitch.consoleLog('info', '<******************************************* ' .. '\n')
	freeswitch.consoleLog('info', 'http url      : ' .. _url .. '\n')
	freeswitch.consoleLog('info', 'http request  : ' .. data .. '\n')
	--print('http url      : ' .. _url)
	--print('http request  : ' .. data)
	
	--http.TIMEOUT = 5

    body,code = http.request{  
        url =  _url,
        method = "POST",  
        headers =   
        {   
            ["Content-Type"] = "application/json",  
            ["Content-Length"] = #data,  
        },  
        source = ltn12.source.string(data),  
        sink = ltn12.sink.table(responseBody)  

    }   

	freeswitch.consoleLog('info', 'http response : ['.. code .. ']' .. table.concat(responseBody) .. '\n')
	freeswitch.consoleLog('info', '>****************************************** ' .. '\n')
	--print('http response : ' .. table.concat(responseBody))
	
	if table.getn(responseBody) == 0 then
		return code, responseBody
	end

    return code,json.decode(table.concat(responseBody))
end


return httpAgent
