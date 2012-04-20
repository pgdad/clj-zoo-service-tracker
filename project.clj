(defproject clj-zoo-service-tracker "1.0.1"
  :description "FIXME: write description"
  :aot [clj-zoo-service-tracker.util
        clj-zoo-service-tracker.instance
        clj-zoo-service-tracker.route
        clj-zoo-service-tracker.clientRegistration
        clj-zoo-service-tracker.core]
  :dependencies [[org.clojure/clojure "1.3.0"]
   [clj-zoo-watcher "1.0.5"]
   [clj-tree-zipper "1.0.0"]
   [log4j/log4j "1.2.16"]
   [org.clojure/tools.logging "0.2.3"]])
