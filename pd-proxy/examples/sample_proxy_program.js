import { get_header, set_header, set_response_content, set_upstream, rate_limit_allow } from "pd-proxy-host";

let header = get_header("x-client-id");

if (rate_limit_allow(header, 3, 60)) {
    set_header("x-vm", "allowed");
    set_response_content("request allowed");
} else {
    set_header("x-vm", "rate-limited");
    set_response_content("rate limit exceeded");
}
