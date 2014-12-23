(ns cljoud.manager)
(defn make-request [tid func-name func-code params]
  (let [serialized-params (serialize params)]
    [taskid [func-name func-code serialized-params]]))
(defprotocol ManagerProtocol
  (async-call-remote [this func-name func-code params options])
  (handle-client-query [this msg])
  (handle-node-answer[this msg])
  (get-next-task-id[this])
  (concat-node-answers[this results])
  (node-num[this])
  (send-answer-to-client[this client tid coll])
  (get-data-by-tid [this tid])
  (get-data-length [this tid])
  (save-client[this tid clientconn])
  (get-client[this tid])
  (remove-client[this tid])
  (close [this]))
(deftype Manager [taskid clientid node-num]
  ManagerProtocol
(handle-client-query [this msg]
  "Inspects recieved queue and spreads it via nodes"
  (let [tid (first msg)
        data (second (second msg))
        func-name (first data)
        func-code (nth data 1)
        coll (deserialize (nth data 2))
        len (count coll)
        nodes node-num
        step (/ len nodes)
        manager-tid (get-next-task-id)]
    (loop [offset 0 queries '() tail coll]
      (if (> (+ step offset) len)
        (cons tail queries)
        (recur (+ offset step) (cons (make-request [offset manager-tid] func-name func-code (take step tail)) queries) (drop step tail))))))
(handle-node-answer [this msg]
  "Handles receive of partial map() result. May invoke sending of whole result to client."
  (let [[offset manager-tid] (first msg)
        data (second msg)
        responses (get-data-by-tid manager-tid)
        length (get-data-length manager-tid)
        partial-results (get responses manager-tid)
        new-partial-result (assoc partial-results {offset data})]
    (swap! responses (assoc responses {manager-id new-partial-result}))
    (if (= (- length 1) (count partial-results))
      (let client (get-client manager-tid))
    )))
  (save-client[this tid clientconn]
    (swap! clientid assoc tid clientconn))
  (get-client[this tid]
    "Returns a connection by given manager-taskid")
    (get @clientid tid))
  (concat-node-answers[this results])
  (async-call-remote [this func-name func-code params options])
  (get-next-task-id[this]
    (let [nid (inc @taskid)]
      (swap! taskid inc)))
  (node-num [this]
    "Returns number of connected nodes"
    node-num)
  (send-answer-to-client[this client tid coll])
  (get-data-by-tid [this tid]
    "Returns a map {offset, data} by given manager-task-id")
  (get-data-length [this tid]
    "Returns number of completed partial map()s")
  (close [this]))
(defn create-manager[port node-num]
  (let ))