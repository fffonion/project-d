; Example: core syntax in Scheme flavor
(define sum 0)
(define limit 6)
(define i 0)

(while (< i limit)
  (if (= i 1)
      (begin
        (set! i (+ i 1))
        (continue)))
  (if (> i 3)
      (break))
  (set! sum (+ sum i))
  (set! i (+ i 1)))

(define bump 0)
(define adjust (lambda (value) (+ value bump)))
(set! bump 100)

(if (> sum 0)
    (set! sum (adjust sum))
    (set! sum 0))

(print (add_one sum))
