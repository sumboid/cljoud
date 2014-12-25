(ns cljoud.node
  (:use [cljoud serialization common])
  (:use [cljoud tcp]))

(defn do-map[func-name func-code params]
  (create-ns 'user)
  (intern 'user 'func-name func-code)
  (let [result (map user/func-name params)]
    (remove-ns 'user)
    result))
(defn handle-request [msg]
  (let [tid (first msg)
        data (second (second msg))
        func-name (first data)
        func-code (nth data 1)
        params (deserialize (nth data 2))]
    (do-map func-name func-code params)))

(defn start-node [host-port-str]
  (let [host port] (host-port host-port-str)
    (start-tcp-client[host port])))
(defn -main [& args]
  (let [node-soc (create-client-socket 8080)
        nl (spawn-fiber node-listener nm nl-soc)]
    (do
      (join nm)
      (join nl))))

