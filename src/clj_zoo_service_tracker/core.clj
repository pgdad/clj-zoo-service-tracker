(ns clj-zoo-service-tracker.core
  (:require [zookeeper :as zk]
            [clj-zoo-watcher.core :as w]
            [clj-zoo-watcher.multi :as mw]
            [clj-zoo-service-tracker.util :as util] 
            [clj-zoo-service-tracker.route :as rt]
            [clj-zoo-service-tracker.instance :as inst]
            [clj-zoo-service-tracker.clientRegistration :as clireg]
            [clojure.reflect] [clj-tree-zipper.core :as tz] [clojure.zip :as z]
            [clojure.tools.logging :as log])
  (:gen-class))

(defn- get-load
  "given a service instance, fetch server instance load"
  [item]
  (let [instance (:instance-node item)
	l (if instance
            (if (:load instance) (:load instance)
                0)
            0)]
    l))

(defn- major-minor-order
  [instance-to-load-ref file-to-data-ref item]
  (let [serv-data (@file-to-data-ref item)
	current-load (get-load item)
	res (+ (* 10000 (:major serv-data)) (* 100 (:minor serv-data)))]
    res))

(defn- major-minor-order-rev
  [instance-to-load-ref file-to-data-ref item]
  (* -1 (major-minor-order instance-to-load-ref file-to-data-ref item)))

(defn- highest-major-order
  [tree]
  (reverse (sort-by :name (z/children tree))))

(defn- minor-filter
  "major and minor are know"
  [file-to-data-ref item minor]
  (let [serv-data (@file-to-data-ref item)]
    (<= minor (:minor serv-data))))


(defn- lookup-latest
  "returns nil if no services available, else returns the highest versioned one"
  [instance-to-load-ref file-to-data-ref route-root service]
  (dosync
   (log/spy :debug "LOOKING UP LATEST")
   (let [f-to-data (ensure file-to-data-ref)
         nodes (keys f-to-data)
         node-prefix (str route-root "/" service)
         for-service (filter (fn [item]
                               (.startsWith item node-prefix))
                             nodes)
         high-order (sort-by (partial major-minor-order-rev instance-to-load-ref file-to-data-ref) for-service)]
     (if (and high-order (not (= high-order '())))
       (first high-order)
       nil))))

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

  [instance-to-load-ref file-to-data-ref route-root service major minor]
  (log/spy :debug (str "LOOKUP SERVICES: " (list service major minor)))
  (dosync
    (let [f-to-data (ensure file-to-data-ref)
	nodes (keys f-to-data)
	node-prefix (str route-root "/" service "/" major)
	for-service (filter (fn [item]
				(and (.startsWith item node-prefix)
					(minor-filter file-to-data-ref item minor)))
				nodes)]
      (log/spy :debug (str "LOOKUP SERVICES for-service: " for-service))
	(if (and for-service (not (= for-service '())))
		(first (sort-by get-load for-service))
		nil))))

(defn- url-of
  [file-to-data-ref service-instance uri]
  (log/spy :debug (str "URL of: " service-instance))
  (let [value (@file-to-data-ref service-instance)]
    (str (:url value) uri)))

(defn- lookup-regional-service
  [tracker-ref my-region service major minor uri]
  (println (str "REGIONAL LOOKUP FOR: " my-region))
  (println (str "REGIONAL LOOKUP KID: " (:kids-ref @tracker-ref)))
  (let [kids-ref (:kids-ref @tracker-ref)
	have-region (contains? @kids-ref my-region)]
    (if have-region
	(let [services (lookup-services (:instance-to-load @tracker-ref)
					(:file-to-data-ref @tracker-ref)
			 		(:route-root @tracker-ref)
					service major minor)]
	   (println (str "HAVE MY-REGION: " my-region))))))

(defn lookup-service
  [tracker-ref service major minor uri]
  (if (and (= major -1) (= minor -1))
    ;; this means the latest version (major minor combo)
    (let [latest (lookup-latest (:instance-to-load-ref @tracker-ref)
                                (:file-to-data-ref @tracker-ref)
                                (:route-root @tracker-ref) service)]
      (if latest
        (url-of (:file-to-data-ref @tracker-ref) latest uri)
        nil))
    (if (= major -1)
      nil
      ;; minor = -1 means take any
      (let [m (if (= -1 minor) 0 minor)
            my-region (:my-region @tracker-ref)
	    routes-multi (:routes-multi @tracker-ref)
	    kids-ref (:kids-ref @routes-multi)
            services (lookup-services (:instance-to-load-ref @tracker-ref)
                                      (:file-to-data-ref @tracker-ref)
                                      (:route-root @tracker-ref)
                                      service major minor)]
;	   regional-services (lookup-regional-service
;		tracker-ref
;		my-region
;		service
;		major
;		minor
;;		uri)]
;	(println (str "MY-REGION: " my-region))
;	(println (str "MULTI-ROUTES: " @routes-multi))
;	(println (str "KIDS-REF: " @kids-ref))
        (if services
          (url-of (:file-to-data-ref @tracker-ref) services uri)
          nil)))))

(defmacro route-root-node
  [env app]
  `(str "/" ~env "/" ~app "/services"))

(defmacro route-root-region-node
  [env app region]
  `(str (route-root-node ~env ~app) "/" ~region))

(defmacro client-reg-root-node
  [env app]
  `(str "/" ~env "/" ~app "/clientregistrations"))


(defmacro instance-root-node
  [env app]
  `(str "/" ~env "/" ~app "/servers"))

(defmacro instance-root-region-node
  [env app region]
  `(str (instance-root-node ~env ~app) "/" ~region))

(defn- ensure-root-exists
  [keepers]
  (let [keepers-parts (clojure.string/split keepers #"/")
        host-part (first keepers-parts)
        chroot-part (second keepers-parts)]
    (if chroot-part
       (let [client (zk/connect host-part)]
          (zk/create-all client (str "/" chroot-part) :persistent? true)))))

(defn- ensure-nodes-exist
  [keepers env app region]
  (ensure-root-exists keepers)
  (let [client (zk/connect keepers)
        route-root (route-root-region-node env app region)
        client-reg-root (client-reg-root-node env app)
        instance-root (instance-root-region-node env app region)]
    (log/spy :info (str "Ensuring route root node exists: " route-root))
    (zk/create-all client route-root :persistent? true)
    (log/spy :info (str "Ensuring client registration root node exists: "
	client-reg-root))
    (zk/create-all client client-reg-root :persistent? true)
    (log/spy :info (str "Ensuring instance root node exists: " instance-root))
    (zk/create-all client instance-root :persistent? true)
    (zk/close client)))

(defn initialize
  [keepers env app region]
  (ensure-nodes-exist keepers env app region)
  (let [client (zk/connect keepers)
        routes-root (route-root-node env app)
        routes-kids-ref (ref {})
        route-root (route-root-region-node env app region)
        client-reg-root (client-reg-root-node env app)
        instance-root (instance-root-region-node env app region)
	file-to-data-ref (ref {})
	instance-to-load-ref (ref {})
        
	i (w/watcher client instance-root
                     (fn [event]
                       (println (str "CONNECTION EVENT: " event)))
                     (fn [data-ref dir-node] nil)
                     (fn [data-ref dir-node] nil)
                     (partial inst/instance-created instance-to-load-ref instance-root client)
                     (partial inst/instance-removed instance-to-load-ref)
                     (partial inst/instance-load-changed instance-to-load-ref)
                     nil)
        mw (mw/child-watchers client routes-root
                       routes-kids-ref
                       (fn [event] (println (str "CONNECTION EVENT: " event)))
                       (fn [data-ref dir-node] nil)
                       (fn [data-ref dir-node] nil)
                       (partial rt/route-created file-to-data-ref route-root client)
                       (partial rt/route-removed file-to-data-ref)
                       (fn [file-node data] nil)
                       nil)
        w (w/watcher client route-root
                     (fn [event] (println (str "CONNECTION EVENT: " event)))
                     (fn [data-ref dir-node] nil)
                     (fn [data-ref dir-node] nil)
                     (partial rt/route-created file-to-data-ref route-root client)
                     (partial rt/route-removed file-to-data-ref)
                     (fn [file-node data] nil)
                     nil)
	client-regs-ref (ref {})
	c (w/watcher client client-reg-root
                     (fn [event] (println (str "CONNECTION EVENT: " event)))
                     (partial clireg/client-registration-created client-regs-ref client-reg-root client)
                     (partial clireg/client-registration-removed  client-regs-ref client-reg-root client)
                     (fn [data-ref file-node] nil)
                     (fn [data-ref file-node] nil)
                     (fn [data-ref file-node data] nil)
                     nil)]
    (ref {:keepers keepers
          :my-region region
          :routes w
          :route-root route-root
          :routes-root routes-root
          :routes-multi mw
          :client-reg-root client-reg-root
          :client-regs c
          :client-regs-ref client-regs-ref
          :file-to-data-ref file-to-data-ref})))

