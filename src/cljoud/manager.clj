(ns cljoud.manager)
(defn make-request [tid func-name func-code params]
  (let [serialized-params (serialize params)]
    [tid [func-name func-code serialized-params]]))
(defn make-answer [tid coll]
  (serialize [tid coll]))
(defprotocol ManagerProtocol
  (async-call-remote [this func-name func-code params options])
  (generate-node-queries [this msg])
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
(deftype Manager [taskid clientid responses nodes]
  ManagerProtocol
(generate-node-queries [this msg]
  "Inspects recieved queue and spreads it via nodes"
  (let [tid (first msg)
        data (second (second msg))
        func-name (first data)
        func-code (nth data 1)
        coll (deserialize (nth data 2))
        len (count coll)
        nodes node-num
        step (quot len nodes)
        manager-tid (get-next-task-id)]
    (loop [offset 0 queries '() tail coll]
      (if (> (+ step offset) len)
        (cons (make-request [offset manager-tid] func-name func-code tail) queries)
        (recur (+ offset step) (cons (make-request [offset manager-tid] func-name func-code (take step tail)) queries) (drop step tail))))))

  (handle-client-query [this msg]
    (let [queries (generate-node-queries msg)]
      (for [[q node] (map vector queries nodes)] (async-send-request node q))))

  (handle-node-answer [this msg]
  "Handles receive of partial map() result. May invoke sending of whole result to client."
  (let [[offset manager-tid] (first msg)
        data (second msg)
        responses (get-data-by-tid manager-tid)
        length (get-data-length manager-tid)
        partial-results (get @responses manager-tid)
        new-partial-result (assoc partial-results {offset data})]
    (swap! responses (assoc responses {manager-id new-partial-result}))
    (if (= (- length 1) (count partial-results)) ;; If all the partial results is here
      (let [[clientconn clienttid] (get-client manager-tid)
            result (concat-node-answers manager-tid)
            msg (make-answer clienttid result)]
        (send clientconn msg))
    )))
  (save-client[this tid clientconn clienttid]
    (swap! clientid assoc tid (list clientconn clientid))

  (get-client[this tid]
    "Returns a connection by given manager-taskid")
    (get @clientid tid))

  (concat-node-answers[this manager-tid]
    (let [responses (get @responses manager-tid)
          sorted-responses (sorted-map-by < responses)]
      (reduce (fn [acc x] (concat acc x)) '[] sorted-map)))
      ;;(loop [offset 0 acc '[]]
      ;;  (if (> offset ))
      ;;  (recur (inc offset) (concat acc (get responses offset)))))

  (async-call-remote [this func-name func-code params options])

  (get-next-task-id[this]
    (let [nid (inc @taskid)]
      (swap! taskid inc)))

  (node-num [this]
    "Returns number of connected nodes"
    (count nodes))
  (get-data-by-tid [this tid]
    "Returns a map {offset, data} by given manager-task-id"
    (get @responses tid))
  (get-data-length [this tid]
    "Returns number of completed partial map()s"
    (count (get @responses tid)))
   (get-full-data-length [this tid]
      "Returns length of completed partial map()s"
     (let [m (get @responses tid)]
       (reduce (fn [acc y] (+ acc (count y))) 0 m)))
  (close [this]))
(defn create-manager[client-port node-port num-nodes]
  ;; TODO Connect nodes
  (let [ client-tcp-server (create-tcp-server client-port handle-client-request)
         node-tcp-server (create-tcp-server node-port handle-node-answer)
         nodes (accept-nodes num-nodes)
        ]
    (let [responses (atom {}) ;; Map storing responses from working nodes. Looks like {taskid, {offset, data}}
        taskid (atom 0)    ;; Unique id for each incoming task
        clientid (atom {}) ;; Map storing {taskid, connection} pairs.
        manager (Manager. taskid clientid responses nodes)]
    manager)))