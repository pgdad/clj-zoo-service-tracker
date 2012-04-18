(ns clj-zoo-service-tracker.instance
  (:require [zookeeper :as zk] [clj-zoo-watcher.core :as w]
            [clj-zoo-service-tracker.util :as util]
            [clojure.tools.logging :as log])
  (:gen-class))

(defn- instance-str-value
  [data-str]
  (let [parts (clojure.string/split data-str util/nl-split-pattern)]
    {:data-version (first parts)
     :load (second parts)}))

(defn- instance-value
  [data]
  (instance-str-value (String. data "UTF-8")))

(defn instance-created
  [instance-to-load-ref instance-root client file-node]
  ;; sleep a while to make sure the server has time to update the data
  (. Thread sleep 100)
  (log/spy :debug (str "INSTANCE CREATED: " file-node))
  (dosync
   (Thread/sleep 100)
   (let [data-str (util/get-file-data client file-node)
         i-to-load (ensure instance-to-load-ref)
         value (instance-str-value data-str)]
     (alter instance-to-load-ref (fn [& args]
                                   (assoc i-to-load file-node value))))))

(defn instance-removed
  [instance-to-load-ref file-node]
  (log/spy (str "INSTANCE removed: " file-node))
  (dosync
   (let [i-to-load (ensure instance-to-load-ref)]
     (alter instance-to-load-ref
            (fn [& args] (dissoc i-to-load file-node))))))

(defn instance-load-changed
  [instance-to-load-ref file-node data]
  (dosync
   (let [i-to-load (ensure instance-to-load-ref)
         value (instance-value data)]
     (alter instance-to-load-ref
            (fn [& args] (assoc i-to-load file-node value))))))

