local _proxy = require("pd-proxy-host")

local header = get_header("x-client-id")

if rate_limit_allow(header, 3, 60) then
    set_header("x-vm", "allowed")
    set_response_content("request allowed")
else
    set_header("x-vm", "rate-limited")
    set_response_content("rate limit exceeded")
end
