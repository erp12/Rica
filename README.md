# Rica

Data-frame abstraction for Clojure data scientists.

A Data-frame is a data structure where **r**ows are accessed by **i**ndex and **c**olumns are **a**ssociated with names. Hence the name **Rica**.

Rica offers a data-frame abstraction that can be broken down into two parts. First, a DataFrame type that implements a handful of Clojure interfaces and protocols and is compatible with most clojure functions that manipulate data-structures. Second, `rica.core` contains functions that make up a data-frame API inspired by [SparkSQL](https://spark.apache.org/sql/), [Pandas](https://pandas.pydata.org/), and [R](https://www.r-project.org/).


Rica aims to offer an intuavite abstraction for manipulating structured data, without requiring a special runtimes (like Spark). The consequence of this is that Rica is not (currently) ideal for large dataset, however it can be useful for smaller tasks and interactive data exploration.


## Usage

Rica has not been put on Clojars yet.

### Creating DataFrames

The `create-data-frame` function can create data-frames in a way that is similar to maps. Every other argument is a keyword that denotes a column name. The following argument is a collection (list of vector) denoting the values stored in that column.

```clojure
(require '[data-frame.core :refer :all])
; nil

(def df
  (create-data-frame :name ["alice" "bob" "eddie"]
                     :age [32 nil 24]))
; #'data-frame.core/df

(print-schema df)
; root
; |-- :name : java.lang.String
; |-- :age : java.lang.Long
; nil

(show df)
; | :name | :age |
; |-------+------|
; | alice |   32 |
; |   bob |      |
; | eddie |   24 |
; nil
```

Often times datasets are stored as nested data structres. Rica provides 2 functions to


### Some Example Pipelines

Suppose we generate the following example dataset.

```clojure
(require '[data-frame.core :refer :all])

(def sales
  (create-data-frame
    :sale-id [0 1 2 4 5 6 7 8 9]
    :amount [74.24 31.24 10.40 22.78 58.24 14.26 61.56 13.95 58.15 69.87]
    :num-items [3 2 1 2 3 3 2 1 3 1]
    :customer-id ["A" "C" "B" "A" "D" nil "B" "E" "A" "F"]))
```

The below snippet finds the biggest purchase made by customer "A".

```clojure
(-> sales
    (where #(= "A" (:customer-id %)))
    (order-by :amount)
    last
    println)
; #ordered/map ([:customer-id A] [:amount 58.15] [:num-items 3] [:sale-id 8])
```

The below snippet finds the average cost of individual items purchased in each sale.

```clojure
(-> sales
    (select :amount :num-items)
    (with-column :avg-item-cost
                 (map #(/ %1 %2)
                      (get-col sales :amount)
                      (get-col sales :num-items)))
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
```

### Full API



## Road Map / To Do

If you have any requests or encounter any issues using Rica, please open a GitHub issue (or PR). A more complete contributing guide is coming soon.

Things which are already on the horizon:
- [ ] CSV -> DataFrame, DataFrame -> CSV
- [ ] DataFrame joining.
- [ ] Grouping and Aggregation
  - [ ] Investigate using Clojure meta-data to store grouping informaiton.

## License

Copyright © 2018 Edward Pantridge

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
