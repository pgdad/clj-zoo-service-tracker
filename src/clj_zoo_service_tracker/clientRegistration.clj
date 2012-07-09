(ns clj-zoo-service-tracker.clientRegistration
  (:require [clojure.tools.logging :as log]
            [clj-zoo-service-tracker.util :as util]
            [clojure.set])
  (:gen-class))

(defn client-registration-created
  [client-regs-ref client-reg-root client data-ref dir-node]
  (dosync
   (let [registrations @client-regs-ref
         reg-def (clojure.string/replace-first dir-node
                                               (str client-reg-root "/") "")
         reg-parts (clojure.string/split reg-def util/uri-split-pattern)
         service (first reg-parts)
         is-client (if (= 2 (count reg-parts)) true)
         servs-for-client (registrations
                           (if is-client (second reg-parts)))]
     (log/spy :debug (str " - SERVS FOR: " servs-for-client))
     (let [client (second reg-parts)
           new-servs-for-client
           (if servs-for-client
             (clojure.set/union servs-for-client #{service})
             #{service})]
       (if is-client (alter client-regs-ref
                            (fn [trans-val] (assoc trans-val  client new-servs-for-client))))))))

(defn client-registration-removed
  [client-regs-ref client-reg-root client data-ref dir-node]
  (log/spy :debug (str "CLIENT REGISTRATION REMOVED: " dir-node))
  (dosync
   (let [registrations @client-regs-ref
         reg-def (clojure.string/replace-first dir-node
                                               (str client-reg-root "/") "")
         reg-parts (clojure.string/split reg-def util/uri-split-pattern)
         service (first reg-parts)
         is-client (if (= 2 (count reg-parts)) true)
         servs-for-client (registrations
                           (if is-client (second reg-parts)))]
     (if servs-for-client
       (let [new-servs (clojure.set/difference servs-for-client
                                               #{service})]
         (alter client-regs-ref
                (assoc registrations client new-servs)))))))
