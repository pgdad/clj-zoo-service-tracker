(ns clj-zoo-service-tracker.trace
  (:require [clojure.tools.logging :as log]
            [clj-zoo-service-tracker.util :as util])
  (:gen-class))

(defn created
  [traces-ref _ file-node]
  (log/spy :debug (str "TRACE CREATED: " file-node))
  (dosync
   (let [traces @traces-ref
         trace-parts (clojure.string/split file-node util/uri-split-pattern)
         address (last trace-parts)]
     (alter traces-ref assoc address true))))

(defn removed
  [traces-ref _ file-node]
  (log/spy :debug (str "TRACE REMOVED: " file-node))
  (dosync
   (let [traces @traces-ref
         trace-parts (clojure.string/split file-node util/uri-split-pattern)
         address (last trace-parts)]
     (alter traces-ref dissoc address))))
