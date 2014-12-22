(ns cljoud.client.common
  (:use [tcp])
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
  (sync-call-remote [this func-name func-code params options])
  (close [this]))

(defn- channel-hostport [ch]
  (let [addr (remote-addr ch)]   ;;<- TODO
    (str (.getHostAddress ^InetAddress
    (.getAddress ^InetSocketAddress addr)) ":" (.getPort ^InetSocketAddress addr))))

(deftype Client [conn]
  ClientProtocol
  (sync-call-remote [this func-name func-code params call-options]
    (let [request (make-request tid func-name func-code params)
           tid 0]
      (send conn request);; <- TODO
      (recv conn msg)
      (let [tid (first msg)
            msg-body (second msg)
            result (handle-response msg-body)]
        result)))
  (close [this]
    (close conn)))

(defn host-port
  "get host and port from connection string"
  [connection-string]
  (let [[host port] (split connection-string #":")]
    [host (Integer/valueOf ^String port)]))

(defn create-client [addr]
  (let [[host port] (host-port addr)
        client (open-tcp-client host port)
        cljoud-client (Client. client)]
      cljoud-client))