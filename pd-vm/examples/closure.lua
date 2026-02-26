local _host = require("pd-vm-host")

local base = 7
local add = function(value) return value + base end
base = 8
print(add(5))
