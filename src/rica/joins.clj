(ns rica.joins
  "Definitions of join operations and related helper functions."
  (:require [clojure.string :refer [split]]
            [rica.utils :as u]))


(defn- columns->key-fn
  [columns]
  #(select-keys % columns))


(defn- key-fn->natural-join-pred
  [key-fn]
  (fn [l r] (= (key-fn l) (key-fn r))))


(defn hash-table
  [df columns]
  (u/index-bag (seq df) columns))


(defn- build-&-probe
  [left right join-type]
  (cond
    (some #{join-type} '("inner" "full" "cross"))
    (let [left-smaller? (< (count left) (count right))]
          (if left-smaller? [left right left-smaller?] [right left left-smaller?]))

    (= join-type "left") [right left false]
    (= join-type "right")  [left right true]))


(defn- fill-nils-per-schema
  [row result-columns]
  (merge (into {} (map vector result-columns (repeat nil))) row))


(defn- inner-hash-join
  "- Only supports natrual joins."
  [left-df right-df on]
  (let [[build probe build-left?] (build-&-probe (seq left-df) (seq right-df) "inner")
        result-columns (keys (merge (.schema left-df) (.schema right-df)))
        key-fn (columns->key-fn on)
        join-pred (key-fn->natural-join-pred key-fn)
        hashed-build (hash-table build on)]
    (reduce (fn [output-rows probe-row]
              (into output-rows
                    (let [probe-keys (key-fn probe-row)
                          build-rows (get hashed-build probe-keys [])]
                      (map (fn [build-row]
                             (merge probe-row build-row))
                           build-rows))))
            [] probe)))


(defn hash-join
  [left-df right-df on join-type]
  (let [join-type (first (split join-type #"_|-|\s"))
        [build probe build-left?] (build-&-probe (seq left-df) (seq right-df) join-type)
        all-columns (keys (merge (.schema left-df) (.schema right-df)))
        key-fn (columns->key-fn on)
        join-pred (key-fn->natural-join-pred key-fn)
        hashed-build (hash-table build on)
        rows (reduce (fn [row-buff probe-row]
                       (into row-buff
                             (let [probe-keys (key-fn probe-row)
                                   build-rows (get hashed-build probe-keys [])]
                               (if (empty? build-rows)
                                 (if (some #{join-type} '("left" "right" "full"))
                                   [(fill-nils-per-schema probe-row all-columns)]
                                   [])
                                 (map #(merge probe-row %) build-rows)))))
                     [] probe)]
    (if (= join-type "full")
      (let [hashed-rows (hash-table rows on)]
        (concat (filter #(not (nil? %))
                        (map (fn [build-row]
                               (let [build-keys (key-fn build-row)
                                     result-rows (get hashed-rows build-keys [])]
                                 (if (empty? result-rows)
                                   (fill-nils-per-schema build-row all-columns))))
                             build))
                rows))
      rows)))


(defn nested-loop-join
  "Pred is a function that takes two rows and returns true/false."
  [left-df right-df pred join-type]
  (let [join-type (first (split join-type #"_|-|\s"))
        [build probe build-left?] (build-&-probe (seq left-df) (seq right-df) join-type)
        build-first-pred (if build-left? pred #(pred %2 %1))
        all-columns (keys (merge (.schema left-df) (.schema right-df)))
        rows (reduce (fn [row-buff probe-row]
                       (into row-buff
                             (let [build-rows (filter #(build-first-pred % probe-row) build)]
                               (if (empty? build-rows)
                                 (if (some #{join-type} '("left" "right" "full"))
                                   [(fill-nils-per-schema probe-row all-columns)]
                                   [])
                                 (map #(merge probe-row %) build-rows)))))
                     [] probe)]
    (if (= join-type "full")
      (concat (filter #(not (nil? %))
                      (map (fn [build-row]
                             (let [probe-rows (filter #(build-first-pred build-row %) probe)]
                               (if (empty? probe-rows)
                                 (fill-nils-per-schema build-row all-columns))))
                           build))
              rows)
      rows)))


(defn find-join-fn
  [join-on join-type]
  (let [natural-join? (and (seqable? join-on) (every? keyword? join-on))
        pred-join? (fn? join-on)
        join-type (first (split join-type #"_|-|\s"))
        join-fn (cond
                  natural-join?
                  #(hash-join %1 %2 join-on join-type)

                  pred-join?
                  #(nested-loop-join %1 %2 join-on join-type))]
    (if (nil? join-fn)
      (throw (Exception. (format "Bad join: %s on %s" join-type join-on)))
      join-fn)))
