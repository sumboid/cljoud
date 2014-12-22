(defn handle-client-query [msg])
(defn handle-node-answer [msg])

(defprotocol ManagerProtocol
  (async-call-remote [this func-name func-code params options])
  (close [this]))