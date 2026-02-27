import * as vm from "vm";

let header = vm.get_header("x-client-id");

if (vm.rate_limit_allow(header, 3, 60)) {
    vm.set_header("x-vm", "allowed");
    vm.set_response_content("request allowed");
} else {
    vm.set_header("x-vm", "rate-limited");
    vm.set_response_content("rate limit exceeded");
}
