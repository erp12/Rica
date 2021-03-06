(defproject io.github.erp12/rica "0.1.0"
  :description "Data-frame abstraction for Clojure data scientists."
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.flatland/ordered "1.5.6"]
                 [org.clojure/data.csv "0.1.4"]
                 [net.mikera/core.matrix "0.61.0"]
                 [net.mikera/vectorz-clj "0.47.0"]]
  :plugins [[lein-codox "0.10.4"]]
  :codox {:output-path "docs"}
  :main rica.core
)

; @TODO: Agg namespace tests.
; @TODO: Finish core tests.
; @TODO: Docstrings for all public functions.
