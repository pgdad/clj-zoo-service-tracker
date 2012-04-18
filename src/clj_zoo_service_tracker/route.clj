(ns clj-zoo-service-tracker.route
  (:require [clj-zoo-service-tracker.util :as util][clojure.tools.logging :as log])
  (:gen-class))


(defn route-created
  [file-to-data-ref route-root client file-node]
  ;; sleep a while to make sure the server has time to update the data
  (. Thread sleep 100)
  (dosync
   (let [data (util/get-file-data client file-node)
         serv-def (clojure.string/replace-first file-node
                                                (str route-root "/") "")
         serv-parts (clojure.string/split serv-def util/uri-split-pattern)
         service (first serv-parts)
         major (read-string (second serv-parts))
         minor (read-string (nth serv-parts 2))
         data-parts (clojure.string/split data util/nl-split-pattern)
         value {:service service :major major :minor minor
                :data-version (first data-parts)
                :instance-node (second data-parts)
                :url (nth data-parts 2)}
         f-to-data (ensure file-to-data-ref)]
     (alter file-to-data-ref (fn [& args]
                               (assoc f-to-data file-node value))))))

(defn route-removed
  [file-to-data-ref file-node]
  (dosync
   (let [f-to-data (ensure file-to-data-ref)]
     (alter file-to-data-ref
            (fn [& args] (dissoc f-to-data file-node))))))


