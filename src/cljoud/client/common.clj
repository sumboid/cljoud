(ns cljoud.client.common
  (:use [cljoud tcp common])
  (:use [cljoud serialization])
  (:import [java.net ConnectException InetSocketAddress InetAddress]
           [java.nio.channels ClosedChannelException]
           [java.util.concurrent ScheduledThreadPoolExecutor
            TimeUnit ScheduledFuture]))

(defn- handle-valid-response [response]
  (let [data (get response :data)]
    data))
(defn- next-trans-id [trans-id-gen]
  (swap! trans-id-gen unchecked-inc))

(defn handle-response [response]
  (case (get response :type)
    "response" (handle-valid-response response)
    "error" {:error (:data response)}
    nil))

(defn make-request [func-name func-code params]
    (serialize {:type "task" :task [func-name func-code params]}))

(defprotocol ClientProtocol
  (connect[this])
  (async-call-remote [this func-name func-code params])
  (sync-call-remote [this func-name func-code params])
  (check-progress [this])
  (request-result [this]))

(deftype Client [addr task-id]
  ClientProtocol
  (connect[this]
    (let [[host port] (host-port addr)]
          (create-client-socket host port)))
  (async-call-remote [this func-name func-code params]
    (let [ conn (connect this)
           request (make-request func-name func-code params)]
      (ssend conn request)
      ;;(println "Request sent")
      (let [msg (deserialize (srecv conn) )
            tid (get msg :id)]
        ;;(println "Taskid received" tid)
        (reset! task-id tid)
        (sclose conn))))
  (sync-call-remote [this func-name func-code params]
    (let [c (async-call-remote this func-name func-code params)
          sleep (Thread/sleep 1000)
          result (request-result this)]
        result))
  (check-progress[this]
    (let [ conn (connect this)
           request (serialize {:type "progress" :task-id @task-id})]
      ;;(println "check-progress:" @task-id)
      (ssend conn request)
      (let [msg (deserialize (srecv conn))
            progress (get msg :progress)]
        (sclose conn)
        progress)))
  (request-result [this]
    (let [ conn (connect this)
           request (serialize {:type "subscribe" :task-id @task-id})]
      ;;(println "request-result:" @task-id)
      (ssend conn request)
      (let [msg (deserialize (srecv conn))
            result (handle-response msg)]
        (sclose conn)
        result))))

(defn create-client [addr]
  (let [task-id (atom "")]
    (println "Creating client for " addr)
    (Client. addr task-id)))
