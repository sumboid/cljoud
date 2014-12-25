(ns cljoud.node
  (:use [cljoud serialization common])
  (:use [cljoud tcp])
  (:use [co.paralleluniverse.pulsar core actors]))

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

(defn start-node [port manager-addr]
  (let [ [maddr mport] (host-port manager-addr)
         node-soc (create-client-socket port)
         wm (worker)
         mc (spawn-fiber manager-client wm node-soc)
         ml (spawn-fiber manager-listener wm node-soc)]
    (do
      (join ml))))

