(ns clj-zoo-service-tracker.serviceCache
  (:require [clj-zoo-service-tracker.util :as util]
            [clojure.tools.logging :as log]
            [clj-zoo.serverSession :as s])
  (:import (java.util Map)
           (com.netflix.curator.x.discovery.details ServiceCacheListener))
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

(defn- listener
  [f]
  (proxy [com.netflix.curator.x.discovery.details.ServiceCacheListener] []
    (cacheChanged [] (f))))

(defn- instance->keys
  [instance]
  (let [pay-map (service->payload-map instance)
        major (read-string (:major pay-map))
        minor (read-string (:minor pay-map))
        id (.getId instance)]
    [major minor id]))

(defn- instances->map
  [instances]
  (reduce (fn [m inst]
            (let [[major minor id] (instance->keys inst)]
              (println (str "GOT ID: " id))
              (update-in m [major minor] assoc id inst)))
          {} instances))

(defn close
  [cache]
  (.close (:cache @cache)))

(defn instances
  [cache]
  (-> (:cache @cache) (.getInstances)))

(defn payloads
  [cache]
  (map service->payload-map (instances cache)))

(defn cache
  [fWork path services-ref caches-ref]
  (println (str "SERVICES CACHE FOR: " path))
  (let [s-parts (clojure.string/split path #"/")
        name (last s-parts)
        region (nth s-parts 2)
        discovery (s/sd-builder fWork path)
        c (-> discovery .serviceCacheBuilder (.name name) .build)
        _ (.start c)
        l (listener (fn [& args]
                      (dosync
                       (alter services-ref assoc-in [region name]
                              (-> c .getInstances instances->map)))))]
    (dosync
     (alter caches-ref assoc path c))
    (.addListener c l)
    (.cacheChanged l)
    (atom {:discovery discovery :cache c :name name :listner l})))
