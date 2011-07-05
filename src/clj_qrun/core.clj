;; clj-qrun - Clojure queue run - run queued commands in parallel on multiple hosts
;; Kyle Harrington (kyleh@cs.brandeis.edu) and Brian Martin, 2011.
;;
;; Much of the rabbitMQ code originated from Brian Martin's clojure-parallel-runs (https://github.com/brianmartin/clojure-parallel-runs)
;;

(ns clj-qrun.core
  (:gen-class)
  (:require [clojure.contrib.duck-streams :as ds]
	    [clojure.contrib.str-utils2 :as s]
	    [fleetdb.client :as fdb])
  (:use [clojure.contrib.command-line]
	[clojure.contrib.java-utils]
	[clojure.contrib.shell-out]
	[rabbitcj.client]
	[clj-serializer.core]
	[clojure.contrib.json]))

;; ----------------------------- rabbit

(def chan nil)

(defn init-connection
  "needs to be done once before other functions are used"
  [{:keys [host port user pass]}]
  (def chan (-> {:username user :password pass
		 :virtual-host "/" :host host :port port}
		(connect)
		(create-channel))))

(defn is-connection-open?
  "Is the connection open?"
  []
  (if (nil? chan)
    false
    (channel-open? chan)))

(defn declare-and-bind-queues
  "Initialize queues with the given list of names."
  [& queue-names]
  (doseq [queue-name queue-names]
    (declare-exchange chan queue-name fanout-exchange false false nil)
    (declare-queue chan queue-name false false true nil)
    (bind-queue chan queue-name queue-name "")))

(defn get-one-json
  "Get one message from the queue with the given name."
  [q]
  (try (read-json (String. (.. chan (basicGet q true) (getBody))) true)
       (catch java.lang.Exception e nil)))
       
(defn get-one-serialized
  "Get one message from the queue with the given name."
  [q]
  (try (let [msg  (.. chan (basicGet q true) (getBody))]
	 (println (deserialize msg (Object.)))
	 (deserialize msg (Object.)))
       (catch java.lang.Exception e (do (println e) nil))))

(defn get-one
  "Get one message from the queue with the given name."
  [q]
  (get-one-json q))

(defn send-one-json
  "Send one message to the queue with the given name."
  [q i]
  (publish chan q "" (json-str i)))

(defn send-one-serialized
  "Send one message to the queue with the given name."
  [q i]
  (publish chan q "" (String. (serialize i))))

(defn send-one
  "Send one message to the queue with the given name."
  [q i]
  (send-one-json q i))

;; --------------------------------- worker

(defn log-file-path 
  "Given 'logs' and 'name', outputs a File for 'logs/name/Wed_Feb_16_12:50:36_EST_20111297878636191"
  [output-dir param-name]
  (if output-dir
    (do
      (let [f (ds/file-str (str output-dir "/" param-name))]
	(if (not (.exists f)) (.mkdir f)))
      (ds/file-str (str output-dir "/" param-name "/"
			(-> (java.util.Date.) (.toString) (.replace \space \_) (.concat (str (System/currentTimeMillis)))))))))

(defn process
  [params q-in log-db-param log-table]
  (let [log (with-out-str
	      (when-not (. (file (last (s/split (:executable-uri params) #"/"))) exists)
		(sh "wget" "--no-check-certificate" (:executable-uri params)))
	      (println "Running: " (:command params))
	      (loop [thisProcess (.. Runtime (getRuntime) (exec (into-array (:command params))))]
		(let [terminated? (try (.. thisProcess (exitValue)); may need extra arg
				       (catch java.lang.Exception e false)
				       (finally true))]
		  (if (not terminated?)
		    (let [num-bytes (.. thisProcess (getInputStream) (available))
			  bytes (byte-array num-bytes)
			  s (do (.. thisProcess (getInputStream) (read bytes 0 num-bytes))
				(String. bytes))]
		      (print s)
		      (Thread/sleep 250)
		      (recur thisProcess))))))]
    (loop [log-db (fdb/connect log-db-param)
	   num-added 0]
      (when (zero? num-added)
	(recur log-db 
	       (log-db ["insert", log-table, {"id" (log-db ["count" log-table]),
					     "body" log}]))))))

(defn start-worker
  "Process params from the queue (if no more messages, quit)."
  [connection-creds q-out q-in log-db-param log-table]
  (init-connection connection-creds)
  (loop [msg (get-one q-out)]
    (cond (not (nil? msg)) ;; Process actual messages
	  (do (process msg q-in log-db-param log-table)
	      (recur (get-one q-out)))
	  
	  (and (nil? msg)
	       (not (is-connection-open?))) ;; If the message wasn't real, check if we have a connection and reconnect if we dont
	  (do (init-connection connection-creds)
	      (recur (get-one q-out)))

	  true
	  (binding [*out* *err*]
	    (println "start-worker: nil message with open connection")))))

;; -------------------------------- Core
  
(defn distribute
  "Distributes a set of messages (map, vector, or seq of JSON acceptable values) to a message queue."
  [connection-params q-out q-in msgs]
  (if (not (is-connection-open?)) (init-connection connection-params))
  (declare-and-bind-queues q-in q-out)
  (doseq [m msgs] (do (send-one q-out m)
                      (println "sent: " m))))

(defn receive-responses
  "Receives responses and writes them to file."
  [connection-params q-in output-file]
  (if (not (is-connection-open?)) (init-connection connection-params))
  (loop [msg (get-one q-in)]
    (if (nil? msg)
      (Thread/sleep 2000)
      (do (println "got msg: " msg)
          (ds/with-out-append-writer output-file (prn msg))))
    (recur (get-one q-in))))

(defn clear
  "Empties the given queues"
  [connection-params & queues]
  (if (not (is-connection-open?)) (init-connection connection-params))
  (doseq [q queues]
    (while (get-one q)
      nil)))

(defn -main [& args]
  (with-command-line args "clj-qrun, command line evaluation via message queue."
    [[distribute? d? "Distribute the parameters given by '--parameters'"]
     [parameters m "Clojure file providing a vector of parameter maps." "~/parameters.clj"]
     [queue-name q "A distinct queue name for distribution and collection." "default"]
     [output-file o "Outputs information about runs performed on the parameters given." "~/output"]
     [worker? w? "Process messages from queue."]
     [clear? c? "Clears the specified queue of all messages."]
     [log-dir l "Worker log directory.  Can be the same for all workers." nil]
     [host h "RabbitMQ host name." "127.0.0.1"]
     [port P "RabbitMQ port number." "5672"]
     [user u "RabbitMQ username." "guest"]
     [pass p "RabbitMQ password." "guest"]
     [log-host "FleetDB log host name." "127.0.0.1"]
     [log-port "FleetDB log port." "3400"]
     [log-table "FleetDB log table name." "qrun-logs"]]
     (let [connection-params {:host host :port (Integer. port) :user user :pass pass}
	   log-db-param {:host log-host :port (read-string log-port)}	   
           q-in (str queue-name "-in")
           q-out (str queue-name "-out")]
       (cond distribute? (distribute connection-params q-out q-in (load-file parameters))
             worker? (start-worker connection-params q-out q-in log-db-param log-table)
             clear? (clear connection-params q-out q-in)
             :else (receive-responses connection-params q-in output-file))))
  (System/exit 0))

