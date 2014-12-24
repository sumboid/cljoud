(ns cljoud.tcp)
"Wtf am I doing?"
(defn host-port
  "get host and port from connection string"
  [connection-string]
  (let [[host port] (split connection-string #":")]
    [host (Integer/valueOf ^String port)]))
(defprotocol MessageChannel
  (id [this])
  (send [this msg])
  (valid? [this])
  (channel-addr [this])
  (remote-addr [this])
  (close [this]))
(deftype Channel
  MessageChannel
  (id [this])
  (send [this msg])
  (valid? [this])
  (channel-addr [this])
  (remote-addr [this])
  (close [this]))
(defprotocol ServerChannelProtocol
  (accept [this])
  (send [this msg])
  (recv [this])
  (close[this]))
(deftype ServerChannel [recvhandler]
  ServerChannelProtocol
  (accept [this])
  (send [this conn msg])
  (recv [this])
  (close[this]))
(defn open-tcp-client host port
  "Returns MessageChannel")
(defn start-tcp-server [port handler])
(defn accept-nodes[])