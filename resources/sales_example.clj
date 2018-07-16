(require '[rica.core :refer :all])

(def sales
  (create-data-frame
    :sale-id [0 1 2 3 4 5 6 7 8 9]
    :amount [34.24 31.24 10.40 22.78 58.24 14.26 61.56 13.95 58.15 69.87]
    :num-items [3 2 1 2 3 3 2 1 3 1]
    :customer-id ["A" "C" "B" "A" "D" nil "B" "E" "A" "F"]))


;; Largest purchase made by customer "A".
(-> sales
    (where #(= "A" (:customer-id %)))
    (order-by :amount)
    last
    println)
; #ordered/map ([:customer-id A] [:amount 58.15] [:num-items 3] [:sale-id 8])


;; Average cost of individual items in each sale.
;;
(-> sales
    (select :amount :num-items)
    (with-column :avg-item-cost #(/ (:amount %) (:num-items %)))
    show)
; | :amount | :num-items |     :avg-item-cost |
; |---------+------------+--------------------|
; |   34.24 |          3 | 11.413333333333334 |
; |   31.24 |          2 |              15.62 |
; |    10.4 |          1 |               10.4 |
; |   22.78 |          2 |              11.39 |
; |   58.24 |          3 | 19.413333333333334 |
; |   14.26 |          3 |  4.753333333333333 |
; |   61.56 |          2 |              30.78 |
; |   13.95 |          1 |              13.95 |
; |   58.15 |          3 | 19.383333333333333 |
; |   69.87 |          1 |              69.87 |
; nil
