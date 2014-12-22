(ns cljoud.node
  (:use [cljoud serialization]))

(defn do-map[func-name func-code params]
  (create-ns 'user)
  (intern 'user 'func-name func-code)
  (let [result (map user/func-name params)]
    (remove-ns 'user)
    result))
(defn handle-request [msg]
  (let [tid (first msg)
        data (second msg)
        func-name (first data)
        func-code (nth data 2)
        params (deserialize (nth data 3))]
    (do-map func-name func-code params)))