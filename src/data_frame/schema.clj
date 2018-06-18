(ns data-frame.schema
  (:require [flatland.ordered.map :refer [ordered-map]]))


(defn schema
  "Creates an ordered map representing a schema of a dataframe or map. Args
  should be alternating keyword and Class names (ie Long, String, Double).

  Example
  (schema :name String :age Long)"
  [& args]
  (doseq [i (range (count args))]
    (let [el (nth args i)]
      (if (zero? (mod i 2))
        (if (not (keyword? el))
          (throw (Exception. "Schema attribute names must be keywords.")))
        (if (not (instance? Class el))
          (throw (Exception. "Schema attribute names must be associated with a Class."))))))
  (apply ordered-map args))


(defn map-conforms?
  "Indicates if the given map-like object conforms to the given schema."
  [m sch]
  (if (not= (set (keys m)) (set (keys sch)))
    false
    (loop [remaining-attr (keys sch)]
      (let [attr (first remaining-attr)]
        (cond
          (empty? remaining-attr)
          true

          (not (instance? (attr sch) (attr m)))
          false

          :else
          (recur (rest remaining-attr)))))))


(defn vec-conforms?
  "Indicates if the given vector conforms to the given schema."
  [v sch]
  (if (not= (count v) (count sch))
    false
    (loop [remaining-v v
           remaining-dtypes (vals sch)]
      (cond
        (empty? remaining-v)
        true

        (not (instance? (first remaining-dtypes)
                        (first remaining-v)))
        false

        :else
        (recur (rest remaining-v)
               (rest remaining-dtypes))))))


(defn print-schema
  "Prints a schema."
  [schm]
  (println "root")
  (doseq [[attr-name attr-class] schm]
    (println "|--" attr-name ":" attr-class)))
