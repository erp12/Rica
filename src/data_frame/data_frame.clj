(ns data-frame.data-frame
  "The `data-frame` namespace contains an implementation of the DataFrame type.
  This type serves as a data structure that is indexed with respect to rows and
  associative with respect to columns. To acheive this, the DataFrame type
  implements the `clojure.lang.Associative` and `clojure.lang.Indexed`
  interfaces."
  (:require [flatland.ordered.map :refer [ordered-map]]
            [data-frame.schema :as sch]
            [data-frame.column :as col])
  (:import (clojure.lang IPersistentVector Associative Indexed)))


(deftype DataFrame
  [columns schema]

  Associative
  (containsKey [self key]
    (.containsKey schema key))

  (entryAt [self key]
    (.entryAt columns key))

  (assoc [self key val]
    (let [new-col (if (instance? data_frame.column.Column val)
                    val
                    (col/create-column val))
          new-col-dtype (col/column-type new-col)]
      (if (not= (count new-col)
                (count self))
        (throw (Exception. "Number of elements in a column must match number of rows.")))
      (DataFrame. (assoc columns key new-col)
                  (assoc schema key new-col-dtype))))

  ;; ILookup
  (valAt [self key]
    (.valAt columns key))

  (valAt [self key notFound]
    (.valAt columns key notFound))

  ;; IPersistentCollection
  (cons [self o]
    (cond
      (and (map? o)
           (sch/map-conforms? o schema))
      (loop [remaining-attr (keys schema)
             new-columns columns]
        (if (empty? remaining-attr)
          (DataFrame. new-columns schema)
          (recur (rest remaining-attr)
                 (let [attr (first remaining-attr)]
                   (assoc new-columns
                          attr
                          (conj (attr columns)
                                (attr o)))))))

      (and (vector? o)
           (sch/vec-conforms? o schema))
      (loop [remaining-attr (keys schema)
             i 0
             new-columns columns]
        (if (empty? remaining-attr)
          (DataFrame. new-columns schema)
          (recur (rest remaining-attr)
                 (inc i)
                 (let [attr (first remaining-attr)]
                   (assoc new-columns
                          attr
                          (conj (attr columns)
                                (nth o i)))))))

      :else
      (throw (Exception. "Cannot cons. Is not a valid row."))))

  (empty [self]
    (loop [remaining-attr (keys schema)
           new-columns columns]
      (if (empty? remaining-attr)
        (DataFrame. new-columns schema)
        (let [attr (first remaining-attr)]
          (recur (rest remaining-attr)
                 (assoc new-columns
                        attr
                        (col/create-column [] (attr schema))))))))

  (equiv [self o]
    (and (isa? (class o) DataFrame)
         (= (vec (.schema o)) (vec schema))
         (loop [remaining-attr (keys schema)]
           (let [attr (first remaining-attr)]
             (cond
               (empty? remaining-attr)
               true

               (not= (get columns attr)
                     (get o attr))
               false

               :else
               (recur (rest remaining-attr)))))))

  ;; Seqable
  (seq [self]
    (loop [remaining-i (range (count self))
           rows []]
      (if (empty? remaining-i)
        (seq rows)
        (let [i (first remaining-i)
              next-row (nth self i nil)]
          (recur (rest remaining-i)
                 (conj rows next-row))))))

  Indexed
  (nth [self i]
    (nth self i nil))

  (nth [self i notFound]
    (if (and (>= i 0)
             (< i (count self)))
      (apply ordered-map
             (flatten (map (fn [[k v]]
                             [k (nth (k columns) i)])
                           schema)))
      notFound))

  ;; Counted
  (count [self]
    (count (second (first columns))))

  Object
  (toString [self]
    (str "DataFrame:"
         (loop [remaining-attr (keys schema)
                str-vec []]
           (let [attr (first remaining-attr)]
             (if (empty? remaining-attr)
               str-vec
               (recur (rest remaining-attr)
                      (conj str-vec (str (attr columns)))))))))
  )
