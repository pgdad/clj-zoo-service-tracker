(ns clj-zoo-service-tracker.core
  (:require [zookeeper :as zk] [clj-zoo-watcher.core :as w]
            [clojure.reflect] [clj-tree-zipper.core :as tz] [clojure.zip :as z]
            [clojure.tools.logging :as log])
  (:gen-class))

(def uri-split-pattern (re-pattern "/"))

(def nl-split-pattern (re-pattern "\n"))

(def version-split-pattern (re-pattern "\\."))

(defn- get-file-data
  [client file-node]
  (String. (:data (zk/data client file-node)) "UTF-8"))

(defn- route-created
  [file-to-data-ref route-root client file-node]
  ;; sleep a while to make sure the server has time to update the data
  (. Thread sleep 100)
  (dosync
   (let [data (get-file-data client file-node)
         serv-def (clojure.string/replace-first file-node
                                                (str route-root "/") "")
         serv-parts (clojure.string/split serv-def uri-split-pattern)
         service (first serv-parts)
         major (read-string (second serv-parts))
         minor (read-string (nth serv-parts 2))
         data-parts (clojure.string/split data nl-split-pattern)
         value {:service service :major major :minor minor
                :data-version (first data-parts)
                :instance-node (second data-parts)
                :url (nth data-parts 2)}
         f-to-data (ensure file-to-data-ref)]
     (alter file-to-data-ref (fn [& args]
                               (assoc f-to-data file-node value))))))

(defn- route-removed
  [file-to-data-ref file-node]
  (dosync
   (let [f-to-data (ensure file-to-data-ref)]
     (alter file-to-data-ref
            (fn [& args] (dissoc f-to-data file-node))))))

(defn- client-registration-created
  [client-regs-ref client-reg-root client dir-node]
  (dosync
   (let [registrations (ensure client-regs-ref)
         reg-def (clojure.string/replace-first dir-node
                                               (str client-reg-root "/") "")
         reg-parts (clojure.string/split reg-def uri-split-pattern)
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

(defn- client-registration-removed
  [client-regs-ref client-reg-root client dir-node]
  (log/spy :debug (str "CLIENT REGISTRATION REMOVED: " dir-node))
  (dosync
   (let [registrations (ensure client-regs-ref)
         reg-def (clojure.string/replace-first dir-node
                                               (str client-reg-root "/") "")
         reg-parts (clojure.string/split reg-def uri-split-pattern)
         service (first reg-parts)
         is-client (if (= 2 (count reg-parts)) true)
         servs-for-client (registrations
                           (if is-client (second reg-parts)))]
     (if servs-for-client
       (let [new-servs (clojure.set/difference servs-for-client
                                               #{service})]
         (alter client-regs-ref
                (assoc registrations client new-servs)))))))

(defn- instance-str-value
  [data-str]
  (let [parts (clojure.string/split data-str nl-split-pattern)]
    {:data-version (first parts)
     :load (second parts)}))

(defn- instance-value
  [data]
  (instance-str-value (String. data "UTF-8")))

(defn- instance-created
  [instance-to-load-ref instance-root client file-node]
  ;; sleep a while to make sure the server has time to update the data
  (. Thread sleep 100)
  (log/spy :debug (str "INSTANCE CREATED: " file-node))
  (dosync
   (Thread/sleep 100)
   (let [data-str (get-file-data client file-node)
         i-to-load (ensure instance-to-load-ref)
         value (instance-str-value data-str)]
     (alter instance-to-load-ref (fn [& args]
                                   (assoc i-to-load file-node value))))))

(defn- instance-removed
  [instance-to-load-ref file-node]
  (log/spy (str "INSTANCE removed: " file-node))
  (dosync
   (let [i-to-load (ensure instance-to-load-ref)]
     (alter instance-to-load-ref
            (fn [& args] (dissoc i-to-load file-node))))))

(defn- instance-load-changed
  [instance-to-load-ref file-node data]
  (dosync
   (let [i-to-load (ensure instance-to-load-ref)
         value (instance-value data)]
     (alter instance-to-load-ref
            (fn [& args] (assoc i-to-load file-node value))))))

(defn- extract-service-parts
  [parts]
  (let [cnt (count parts)
	tmp (rest parts)
	service (first tmp)
	tmp2 (rest tmp)
	version (first tmp2)
	split-version (clojure.string/split version version-split-pattern)
	ver-major (read-string (first split-version))
	ver-minor (if (= 2 (count split-version)) (read-string (second split-version)) 0)
	;; ver-minor 0
	r-path (clojure.string/join "/" (rest tmp2))
	m {:service service :ver-major ver-major
           :ver-minor ver-minor :uri r-path}]
    m))

(defn- get-service-from-parts
  [parts]
  [(second parts) (rest (rest parts))])

(defn- get-version-from-parts
  [parts]
  (if (= parts '())
    nil
    (let [version (read-string (first parts))
          v-type (clojure.reflect/reflect version)
          bases (:bases v-type)
          is-version (contains? bases 'java.lang.Number)
          is-minor (and is-version (< 0 (.indexOf (first parts) ".")))
          minor-part (if is-minor (second (clojure.string/split (first parts) version-split-pattern)) nil)
          minor (if minor-part (read-string minor-part) 0)
          ]
      (if is-minor
        (list (read-string (first (clojure.string/split (first parts) version-split-pattern))) minor)
        (if is-version
          (list version)
          '()))
      )))

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
            services (lookup-services (:instance-to-load-ref @tracker-ref)
                                      (:file-to-data-ref @tracker-ref)
                                      (:route-root @tracker-ref)
                                      service major minor)]
        (if services
          (url-of (:file-to-data-ref @tracker-ref) services uri)
          nil)))))

(defn initialize
  [keepers route-root client-reg-root instance-root]
  (let [client (zk/connect keepers)
	file-to-data-ref (ref {})
	instance-to-load-ref (ref {})
	i (w/watcher client instance-root
                     (fn [event] (println (str "CONNECTION EVENT: " event)))
                     (fn [dir-node] nil)
                     (fn [dir-node] nil)
                     (partial instance-created instance-to-load-ref instance-root client)
                     (partial instance-removed instance-to-load-ref)
                     (partial instance-load-changed instance-to-load-ref))
        w (w/watcher client route-root
                     (fn [event] (println (str "CONNECTION EVENT: " event)))
                     (fn [dir-node] nil)
                     (fn [dir-node] nil)
                     (partial route-created file-to-data-ref route-root client)
                     (partial route-removed file-to-data-ref)
                     (fn [file-node data] nil))
	client-regs-ref (ref {})
	c (w/watcher client client-reg-root
                     (fn [event] (println (str "CONNECTION EVENT: " event)))
                     (partial client-registration-created client-regs-ref client-reg-root client)
                     (partial client-registration-removed  client-regs-ref client-reg-root client)
                     (fn [file-node] nil)
                     (fn [file-node] nil)
                     (fn [file-node data] nil))]
    (ref {:keepers keepers
          :routes w
          :route-root route-root
          :client-reg-root client-reg-root
          :client-regs c
          :client-regs-ref client-regs-ref
          :file-to-data-ref file-to-data-ref})))

