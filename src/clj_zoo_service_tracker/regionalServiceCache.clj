(ns clj-zoo-service-tracker.regionalServiceCache
  (:require [clj-zoo-service-tracker.serviceCache :as sc]
            [clj-zoo-watcher.cache :as wc]
            [clj-zoo.serverSession :as s])
  (:gen-class))


(defn close
  [cache]
  (close (:cache @cache)))

(defn- add-service-cache
  [fWork services-ref caches-ref event]
  (let [event-data (.getData event)
        path (.getPath event-data)]
    (println (str "ADD REGIONAL: " path))
    (dosync
     (alter caches-ref assoc path (sc/cache fWork path services-ref caches-ref))))
  )

(defn- rm-service-cache
  [fWork services-ref caches-ref event]
  (let [event-data (.getData event)
        path (.getPath event-data)
        cache (@caches-ref path)]
    (println (str "REMOVE REGIONAL: " path))
    (close (:cache @cache))
    (dosync
     (alter caches-ref dissoc path))))

(defn cache
  "services ref contains a name -> serviceCache-ref map"
  [fWork path services-ref caches-ref]
  (let [watcher-cache (wc/cache fWork
                                path
                                (partial add-service-cache fWork services-ref caches-ref)
                                (partial rm-service-cache fWork services-ref caches-ref)
                                nil
                                nil)]
    (ref {:cache watcher-cache})))
