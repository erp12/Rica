(ns rica.agg
  "")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Aggregation utilities

(defn- sum-col
  [rows col-name]
  (apply + (map col-name rows)))


(defn- safe-col-max
  [rows col-name]
  (let [col-vals (map col-name rows)]
    (if (every? number? col-vals)
      (apply max col-vals)
      (last (sort (col-vals))))))


(defn- safe-col-min
  [rows col-name]
  (let [col-vals (map col-name rows)]
    (if (every? number? col-vals)
      (apply min col-vals)
      (first (sort (col-vals))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Common Aggregation Functions

(defn count-agg
  []
  (fn [group] (count group)))


(defn count-distinct-agg
  []
  (fn [group] (count (distinct group))))


(defn max-agg
  [col-name]
  (fn [group] (safe-col-max (map col-name group))))


(defn min-agg
  [col-name]
  (fn [group] (safe-col-min (map col-name group))))


(defn sum-agg
  [col-name]
  (fn [group] (sum-col group col-name)))


; (defn sum-distinct-agg)


(defn mean-agg
  [col-name]
  (fn [group]
    (float (/ (sum-col group col-name) (count group)))))


; (defn stddev-agg)
; (defn variance-agg)


(defn first-agg
  [col-name]
  (fn [group] (first (map col-name group))))


(defn last-agg
  [col-name]
  (fn [group] (last (map col-name group))))
