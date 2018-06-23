(ns rica.io
  (:require [rica.schema :as sch]
            [rica.column :as col]
            [rica.data-frame :as df]))


; (defn from-csv
;   "Creates a DataFrame from a csv file."
;   [filename]
;   (with-open [reader (io/reader filename)]
;     (let [csv-contents (csv/read-csv reader)
;           headers (map keyword (first csv-contents))
;           data (rest csv-contents)]
;       (row-vecs->DataFrame data headers))))
;
;
; (defn to-csv
;   [df filename]
;   (with-open [writer (io/writer filename)]
;     (csv/write-csv writer
;                    (concat [(map u/keyword-to-str (column-names df))]
;                            (vec (map #(map second %)
;                                      (as-rows df)))))))
