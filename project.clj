(defproject clj-zoo-service-tracker "1.0.9"
  :description "FIXME: write description"
  ;; :aot [clj-zoo-service-tracker.util
  ;;       clj-zoo-service-tracker.instance
  ;;       clj-zoo-service-tracker.route
  ;;       clj-zoo-service-tracker.trace
  ;;       clj-zoo-service-tracker.regionalRoutes
  ;;       clj-zoo-service-tracker.clientRegistration
  ;;       clj-zoo-service-tracker.core]
  :plugins [[lein-swank "1.4.4"]]
  :dependencies [[org.clojure/clojure "1.4.0"]
   [clj-zoo "1.0.11"]
   [clj-zoo-watcher "1.0.10"]
   [log4j/log4j "1.2.16"]
   [org.clojure/tools.logging "0.2.3"]
   [com.netflix.curator/curator-recipes "1.1.13"]]
  :warn-on-reflection true
  :jar-exclusions [#"project.clj"]
  ;:omit-source true
  )
