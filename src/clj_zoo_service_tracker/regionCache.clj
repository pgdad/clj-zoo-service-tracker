(ns clj-zoo-service-tracker.regionCache
  (:require [clj-zoo-watcher.cache :as wc]
            [clj-zoo-service-tracker.regionalServiceCache :as regional-cache]
            [clj-zoo.serverSession :as s])
  (:gen-class))


(defn close
  [cache]
  (.close (:cache @cache)))

(defn- add-region
  [fWork services-ref caches-ref event]
  (println (str "ADDIG REGION: " event))
  (let [event-data (.getData event)
        path (.getPath event-data)
        region (last (clojure.string/split path #"/"))]
    (dosync
     (alter caches-ref assoc-in [path] (regional-cache/cache fWork path services-ref caches-ref)))))

(defn- rm-region
  [fWork services-ref caches-ref event]
  (println (str "REMOVING REGION: " event))
  (let [event-data (.getData event)
        path (.getPath event-data)
        region (last (clojure.string/split path #"/"))
        cache (get-in @caches-ref [path])]
    (close cache)
    (dosync
     (alter caches-ref dissoc path))))

(defn cache
  "regions is a region -> services map ref"
  [fWork path services-ref caches-ref]
  (let [watcher-cache (wc/cache fWork
                                path
                                (partial add-region fWork services-ref caches-ref)
                                (partial rm-region fWork services-ref caches-ref)
                                nil
                                nil)]
    (ref {:cache watcher-cache :caches-ref caches-ref :services-ref services-ref})))
