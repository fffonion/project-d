local _host = require("pd-vm-host")
local string = require("../stdlib/rss/strings.rss")

-- Complex Lua flavor example: loop + stdlib + host + closure.
local total = 0
for i = 0, 3, 1 do
    total = total + i
end

if string.non_empty("lua") then
    total = add_one(total)
else
    total = 0
end

local base = 7
local add = function(value) return value + base end
base = 8
local closure_value = add(5)

print(closure_value)
