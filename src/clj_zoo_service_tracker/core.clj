(ns clj-zoo-service-tracker.core
  (:require [zookeeper :as zk]
            [clj-zoo.session :as session]
            [clj-zoo-watcher.core :as w]
            [clj-zoo-watcher.multi :as mw]
            [clj-zoo-watcher.cache :as c]
            [clj-zoo-watcher.mapper :as mapperc]
            [clj-zoo-service-tracker.regionCache :as rc]
            [clj-zoo-service-tracker.serviceCache :as sc]
            [clj-zoo-service-tracker.util :as util] 
            [clojure.tools.logging :as log])
  (:gen-class))

(defn- get-load-0
  [instance-cache-ref service-instance]
  (get-in @instance-cache-ref [:m (:server service-instance) :data :load]))

(defn- get-load
  "given service-ref, instance-cache-ref and service instance,
fetch server instance load"
 [service-ref load-ref item]
 (let [instance (get-in @service-ref [item :instance-node])
       l (get-in @load-ref [:m instance :data :load])
       res (if l l 0)]
   res))

(defn- major-minor-order-o
  [instance-cache-ref item]
  (let [current-load (get-in @instance-cache-ref [:m item :data :load])
        l (if current-load current-load 0.0)]
    ))

(defn- major-minor-order
  [service-ref load-ref item]
  (let [serv-data (service-ref item)
	current-load (get-load service-ref load-ref item)
	res (+ (* 10000 (:major serv-data)) (* 100 (:minor serv-data)))]
    res))

(defn- major-minor-order-rev
  [service-ref load-ref item]
  (* -1 (major-minor-order service-ref load-ref item)))

(defn- minor-filter
  "major and minor are know"
  [file-to-data item minor]
  (let [serv-data (file-to-data item)]
    (<= minor (:minor serv-data))))


(defn- regional-value-of
  [tracker-ref region service-instance]
  (let [regional-routes-ref (:regional-routes-ref @tracker-ref)
        regional-f-to-data (@regional-routes-ref region)]
    (regional-f-to-data service-instance)))

(defn- lookup-latest
  "returns nil if no services available, else returns the highest versioned one"
  [instance-cache-ref services-ref region service]
  (let [services (get-in @services-ref [region service])]
    (when (and services (not (= {} services)))
      (let [highest (reduce max (keys services))
            highest-services (services highest)]
        (when (and highest-services (not (= {} highest-services)))
          (let [highest-minor (reduce max (keys highest-services))
                highest-minor-services (highest-services highest-minor)
                services-w-pay (map sc/service->payload-map
                                    (vals highest-minor-services))
                sorted (sort-by (partial get-load-0 instance-cache-ref)
                                services-w-pay)]
            (:url (first sorted))))))))

(defn- lookup-services
  "returns nil if no services available, else returns a set of services
that 'match' the required version.

Required version is defined as:

'(<MAJOR>)  - Means any version where <MAJOR> part of the version matches is ok.
For example, if availabe services are:
'(1 1 1), (1 2 1), (1 3 1)  and <MAJOR> == 1, then all the services match
but for example (2 1 1) would not match.

'(<MAJOR> <MINOR>)  - Means that any version where <MAJOR> matches, and
<MINOR> is greater that or equal to requested is ok.
For example, if available services are:
'(1 1 1), (1 2 1), (1 3 1) and <MAJOR> == 1, <MINOR> == 2,
then (1 2 1) and (1 3 1) match, again (2 1 1) would not match."

  [tracker-ref region service major minor]
  (log/spy :debug (str "LOOKUP SERVICES: " (list service major minor)))
  (dosync
   (let [regional-routes-ref (:regional-routes-ref @tracker-ref)
         regional-f-to-data (@regional-routes-ref region) 
         regional-nodes (keys regional-f-to-data)
         regional-for-service (filter (fn [item]
                                        (and (= service
                                                (:service (regional-value-of tracker-ref
                                                                             region item)))
                                             (minor-filter regional-f-to-data item minor)))
                                      regional-nodes)]
     (log/spy :debug (str "LOOKUP SERVICES for-service: " regional-for-service))
     (if (and regional-for-service (not (= regional-for-service '())))
       (do
         (first (sort-by (partial get-load
                                  (:file-to-data-ref @tracker-ref)
                                  (:instance-cache-ref @tracker-ref))
                         regional-for-service)))
       
       nil))))

(defn- regional-url-of
  [tracker-ref region service-instance uri]
  (log/spy :debug (str "REGIONAL URL of: " service-instance " Region: " region))
  (str (:url (regional-value-of tracker-ref region service-instance)) uri))

;; function to be used to filter out regions based on client id
(defn- filter-lookup-regions
  [client-id ^String region]
  (or (.endsWith region (str "-" client-id))
      (= -1 (.indexOf region "-"))))

(defn- sort-lookup-regions
  [my-region regions client-id]
  (sort-by (fn [^String item]
             (if (= item (str my-region "-" client-id))
               -2
               (if (.endsWith item (str "-" client-id))
                 -1
                 (if (= my-region item)
                   0
                   1))))
           regions))

(defn- allowed-regions-sorted
  [my-region regions client-id]
  (sort-lookup-regions my-region
                       (filter (partial filter-lookup-regions client-id) regions)
                       client-id))

(defn- lookup-latest-in-regions
  [regions tracker-ref my-region service uri]
  (loop [xs regions]
    (when (seq xs)
      (let [latest (lookup-latest tracker-ref (first xs) service)]
        (if-not latest
          (recur (next xs))
          (regional-url-of tracker-ref (first xs) latest uri))))))

(defn- lookup-services-in-regions
  [regions tracker-ref service major minor uri]
  (loop [xs regions]
    (when (seq xs)
      (let [services (lookup-services tracker-ref (first xs) service major minor)]
        (if-not services
          (recur (next xs))
          (regional-url-of tracker-ref (first xs) services uri))))))

(defn lookup-service
  [tracker-ref service major minor uri client-id]
  (let [regions (keys @(:services-ref @tracker-ref))
        sorted-regions (allowed-regions-sorted (:my-region @tracker-ref) regions client-id)]
    (if (and (= major -1) (= minor -1))
      ;; this means the latest version (major minor combo)
      (lookup-latest-in-regions sorted-regions tracker-ref (:my-region @tracker-ref) service uri)

      (if (= major -1)
        nil
        ;; minor = -1 means take any
        (let [m (if (= -1 minor) 0 minor)]
          (lookup-services-in-regions sorted-regions tracker-ref service major m uri))))))

(def create-passive-base "/createpassive")

(def route-root-node "/services")

(defmacro route-root-region-node
  [region]
  `(str route-root-node "/" ~region))

(def client-reg-root-node "/clientregistrations")

(def instance-root-node "/servers")

(defmacro instance-root-region-node
  [region]
  `(str instance-root-node "/" ~region))

(defn- create-non-existing-node
  [fWork node]
  (if-not (-> fWork .checkExists (.forPath node))
    (-> fWork .create .creatingParentsIfNeeded (.forPath node))))

(defn- ensure-root-exists
  [keepers]
  (let [keepers-parts (clojure.string/split keepers #"/")
        host-part (first keepers-parts)
        chroot-part (second keepers-parts)]
    (if chroot-part
      (let [z-session (session/login host-part)
            fWork (:fWork @z-session)]
        (create-non-existing-node fWork (str "/" chroot-part))
        (session/logout z-session)))))

(defn- ensure-nodes-exist
  [keepers region]
  (ensure-root-exists keepers)
  (let [z-session (session/login keepers)
        fWork (:fWork @z-session)
        route-root (route-root-region-node region)
        instance-root (instance-root-region-node region)]
    (doseq [node (list route-root client-reg-root-node instance-root create-passive-base)]
      (create-non-existing-node fWork node))
    (session/logout z-session)))

(defn- instance-data-f
  [data]
  (let [data-str (String. data "UTF-8")]
    (if-not data-str
      {:data-version 1 :load 0.0}
      (let [parts (clojure.string/split data-str util/nl-split-pattern)]
        {:data-version (first parts)
         :load (read-string (second parts))}))))

(defn initialize
  [keepers region]
  (ensure-nodes-exist keepers region)
  (let [z-session (session/login keepers)
        fWork (:fWork @z-session)
        services-ref (ref {})
        caches-ref (ref {})
        instance-root (instance-root-region-node region)

        instance-cache-ref (ref {})
        instance-cache (mapperc/mapper-cache fWork instance-cache-ref
                                             instance-data-f
                                             instance-root)

        services-cache (rc/cache fWork "/services" services-ref caches-ref)

        client-regs-curator-ref (ref {})
        client-reg-cache (mapperc/mapper-cache fWork client-regs-curator-ref nil client-reg-root-node)]

    (ref {:keepers keepers
          :fWork fWork
          :my-region region
          :instance-cache instance-cache
          :instance-cache-ref instance-cache-ref
          :services-cache services-cache
          :services-ref services-ref
          :caches-ref caches-ref
          :client-reg-cache client-reg-cache
          :client-regs-curator-ref client-regs-curator-ref})))

(defn close
  [session]
  (let [cli-reg-cache (:client-reg-cache @session)
        instance-cache (:instance-cache @session)
        services-cache (:services-cache @session)]
    (mapperc/close cli-reg-cache)
    (mapperc/close instance-cache)
    (rc/close services-cache)
    (session/logout session)))