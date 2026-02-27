(require (prefix-in vm. "vm"))

(define header (vm.get_header "x-client-id"))

(if (vm.rate_limit_allow header 3 60)
    (begin
      (vm.set_header "x-vm" "allowed")
      (vm.set_response_content "request allowed"))
    (begin
      (vm.set_header "x-vm" "rate-limited")
      (vm.set_response_content "rate limit exceeded")))
