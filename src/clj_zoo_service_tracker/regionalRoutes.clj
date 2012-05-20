(ns clj-zoo-service-tracker.regionalRoutes
  (:require [clj-zoo-service-tracker.util :as util][clojure.tools.logging :as log])
  (:gen-class))


(defn new
  []
  (ref {}))

(defn- ensure-region-added
  [reg-routes-ref region]
  (dosync
   (if-not (@reg-routes-ref region)
     (alter reg-routes-ref assoc region {}))))

(defn add-route
  [reg-routes-ref region file-node value]
  (ensure-region-added reg-routes-ref region)
  (dosync
   (let [reg-routes @reg-routes-ref
        regional (reg-routes region)]
     (alter reg-routes-ref assoc region (assoc regional file-node value)))))

(defn remove-route
  [reg-routes-ref region file-node]
  (dosync
   (let [reg-routes @reg-routes-ref
         regional (reg-routes region)]
     (alter reg-routes-ref assoc region (dissoc regional file-node)))))



