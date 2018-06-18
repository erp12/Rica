(ns data-frame.core
  (:gen-class)
  (:require [data-frame.schema :as sch]
            [data-frame.column :as col]
            [data-frame.data-frame :as df]))


(defn column-names
  "Returns the names of all the column in the given data-frame."
  [df]
  (keys (.schema df)))


;;
;;
;;

(def sch (sch/schema :email String :age Long))

(def email-col
  (col/create-column ["alice@hmail.com" "cathy@zmail.com" "bob@ymail.com" "eddie@hmail.com"]
                     String))

(def age-col
 (col/create-column [20 45 32 24] Long))


; (defn -main
;   [& args]
;   (println (str (conj age-col 1))))

(defn -main
  [& args]
  (->> (df/->DataFrame {:email email-col
                       :age age-col}
                      sch)
      (take 2)
      println))
