(ns clj-zoo-service-tracker.util
  (:require [zookeeper :as zk] [clj-zoo-watcher.core :as w]
            [clojure.reflect] [clj-tree-zipper.core :as tz] [clojure.zip :as z]
            [clojure.tools.logging :as log])
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

(defmacro route-root-node
  [env app region]
  `(str "/" ~env "/" ~app "/services/" ~region))

(defmacro client-reg-root-node
  [env app]
  `(str "/" ~env "/" ~app "/clientregistrations"))

(defmacro instance-root-node
  [env app region]
  `(str "/" ~env "/" ~app "/servers/" ~region))
