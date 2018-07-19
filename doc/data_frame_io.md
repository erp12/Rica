# Reading and Writing DataFrames

DataFrames store relational/tabular data with has many standard forms of
serialization. Rica targets the most common, non-distributed file types and
data stores to read and write from.

## CSV

One of the most ubiquitous file types for storing data is the comma seperated
values files. The `rica.io` namespace provides the `from-csv` and `to-csv`
functions for reading and writing DataFrames to and from CSV files.

As shown in the example below, any non-string column must be cast to the
correct type.

```clojure
(require '[rica.core :refer :all])
(require '[rica.io :refer :all])


(def apps
  (-> (from-csv "resources/apps_data.csv" true)
      (with-column :major_version #(Long/parseLong (:major_version %)))
      (with-column :minor_version #(Long/parseLong (:minor_version %)))))

```

Rica wraps `org.clojure/data.csv` for serialization and thus passes the
arguments typically used as options to `read-csv` and `write-csv` through to
the underlying function.

For example, the below Rica example sets the `:seperator` option in the same
way as `org.clojure/data.csv`.

```clojure
(def pre-releases
  (-> apps
      (where #(zero? (:major_version %)))
      (order-by :minor_version)
      (with-column :version
                   #(str (:major_version %) "." (:minor_version %)))
      (select :app :version)))


(show pre-releases)

; |      :app | :version |
; |-----------+----------|
; |    Zoolab |      0.2 |
; | Lotstring |      0.5 |
; |  Wrapsafe |      0.6 |


(to-csv pre-releases "resources/prerelease_apps_data.csv" true :separator \|)

; Contents of "resources/prerelease_apps_data.csv"
; app|version
; Zoolab|0.2
; Lotstring|0.5
; Wrapsafe|0.6

```
