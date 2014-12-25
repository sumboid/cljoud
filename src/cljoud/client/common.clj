(ns cljoud.client.common
  (:use [cljoud tcp common])
  (:use [cljoud serialization])
  (:import [java.net ConnectException InetSocketAddress InetAddress]
           [java.nio.channels ClosedChannelException]
           [java.util.concurrent ScheduledThreadPoolExecutor
            TimeUnit ScheduledFuture]))

(defn- handle-valid-response [response]
  (let [[code data] (second response)]
    (case code
      :success {:result (deserialize data)}
      :not-found {:cause {:error code}}
      :exception {:cause {:error code :exception (deserialize data)}}
      {:cause {:error :invalid-result-code}})))
(defn- next-trans-id [trans-id-gen]
  (swap! trans-id-gen unchecked-inc))

(defn handle-response [response]
  (case (first response)
    :type-response (handle-valid-response response)
    :type-error {:cause {:error (-> response second first)}}
    nil))

(defn make-request [tid func-name func-code params]
  (let [serialized-params (serialize params)]
    [tid [:type-request [func-name func-code serialized-params]]]))

(defprotocol ClientProtocol
  (sync-call-remote [this func-name func-code params])
  (close [this]))

(deftype Client [conn]
  ClientProtocol
  (sync-call-remote [this func-name func-code params]
    (println "Sync-call-remote")
    (let [ tid 0
           request (make-request tid func-name func-code params)]
      (ssend conn (str request));; <- TODO
      (let [msg (srecv conn)
            tid (first msg)
            msg-body (second msg)
            result (handle-response msg-body)]
        result)))
  (close [this]
    (close conn)))

(defn create-client [addr]
  (do
    (println "Creating client in" addr)
  (let [[host port] (host-port addr)
        client (create-client-socket host port)
        cljoud-client (Client. client)]
      cljoud-client)))