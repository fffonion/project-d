local _host = require("pd-vm-host")

-- Example: all core syntax in Lua flavor
local sum = 0
local limit = 5

for i = 0, limit - 1, 1 do
    if i == 1 then
        goto continue
    end
    if i > 2 then
        break
    end
    sum = sum + i
    ::continue::
end

local j = 0
while j < 4 do
    j = j + 1
    if j == 2 then
        goto continue
    end
    sum = sum + 1
    if j > 2 then
        break
    end
    ::continue::
end

local bump = 1
local adjust = function(value) return value + bump end
bump = 100

if sum > 0 then
    sum = adjust(sum)
else
    sum = 0
end

print(add_one(sum))
