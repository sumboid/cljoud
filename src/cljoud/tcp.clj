(ns cljoud.tcp)
(defprotocol MessageChannel
  (id [this])
  (send [this msg])
  (send* [this msg cb])
  (valid? [this])
  (channel-addr [this])
  (remote-addr [this])
  (close [this]))

(defn start-tcp-server [host port handler]
  )