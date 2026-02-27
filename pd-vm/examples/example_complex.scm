(import (prefix "../stdlib/rss/strings.rss" string:))

; Complex Scheme flavor example: loop + stdlib + host + closure.
(define total 0)
(for (i 0 4)
  (set! total (+ total i)))

(if (string:non_empty "scheme")
    (set! total (add_one total))
    (set! total 0))

(define base 7)
(define add (lambda (value) (+ value base)))
(set! base 8)
(define closure-value (add 5))

(print closure-value)
