(define i 0)
(define total 0)

(while (< i 3)
  (set! total (+ total 1))
  (set! i (+ i 1)))

(if (> total 2)
    (print (add_one 5))
    (print 0))
