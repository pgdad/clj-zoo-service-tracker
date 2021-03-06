(ns clj-zoo-service-tracker.util
  (:require [zookeeper :as zk])
  (:gen-class))

(def uri-split-pattern (re-pattern "/"))

(def nl-split-pattern (re-pattern "\n"))

(def version-split-pattern (re-pattern "\\."))

(defn get-file-data
  [client file-node]
  (let [node-data (zk/data client file-node)
        data (if node-data (:data node-data) nil)
        data-str (if data (String. data "UTF-8") nil)]
      data-str))
