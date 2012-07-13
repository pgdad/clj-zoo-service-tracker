(ns clj-zoo-service-tracker.serviceCache
  (:require [clj-zoo-service-tracker.util :as util]
            [clojure.tools.logging :as log]
            [clj-zoo.serverSession :as s])
  (:import (java.util.Map))
  (:gen-class))

(defn- ->keyword
  [key]
  (keyword (if (.startsWith key ":") (.substring key 1) key)))

(defn- ->clj
 [o]
 (let [entries (.entrySet o)]
   (reduce (fn [m [^String k v]]
             (assoc m (->keyword k) v))
           {} entries)))

(defn- service->payload-map
  [service]
  (->clj (.getPayload service)))

(defn cache
  [fWork active? region name]
  (let [discovery (if active?
                    (s/activated-discovery fWork region)
                    (s/passivated-discovery fWork region))]
    (atom {:discovery discovery :name name})))

(defn instances
  [cache]
  (-> (:discovery @cache) (.queryForInstances (:name @cache))))

(defn instance
  [cache id]
  (-> (:discovery @cache) (.queryForInstance (:name @cache) id)))

(defn payloads
  [cache]
  (map service->payload-map (instances cache)))