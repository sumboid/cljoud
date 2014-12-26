(ns cljoud.node
  (:use [cljoud serialization common])
  (:use [cljoud tcp])
  (:use [co.paralleluniverse.pulsar core actors]))

(defn call [^String nm & args]
    (when-let [fun (ns-resolve *ns* (symbol nm))]
        (apply fun args)))
(defn do-map[func-name func-code params]
  (create-ns 'user)
  ;;(binding [*ns* 'user]
    (intern 'user (symbol func-name) (eval (read-string func-code)))
    (let [result (map (fn [x] (call func-name x)) params)]
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

