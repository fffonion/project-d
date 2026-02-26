(define base 7)
(define add (lambda (value) (+ value base)))
(set! base 8)
(print (add 5))
