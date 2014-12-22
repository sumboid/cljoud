(ns framework.core
  (:use [clojure.repl] :only [source-fn])
  (:use [clojure.string :only [split]]))

(defn cloudrc
  "Create connection to a manager."
  [addr]
  (delay (create-client addr)))

(defn process-call-result [call-result]
  (if (nil? (:cause call-result))
    (:result call-result)))

(defn invoke-remote
  "Invoke remote function with given connection.
   Used exclusevely by defn-remote."
  [sc remote-call-info]
  (let [sc @(or *sc* sc) ;; allow local binding to override client
        [fname fcode args] remote-call-info]
      (process-call-result (sync-call-remote sc fname fcode args options))))

(defn rmap[f coll]
  (f coll))

(defmacro defn-remote
  "Define a facade for remote function. You have to provide the
  connection and the function name."
  ([sc fname]
    (let [fname-str (str fname)
          ns-declared (> (.indexOf fname-str "/") 0)
          [remote-ns remote-name] (split fname-str #"/" 2)
          facade-sym (if ns-declared
                       (symbol remote-name)
                       fname)
          fsource (or (clojure.repl/source-fn fname) (throw Exception . (+ "Source not found:" fname)))]
      `(def ~facade-sym
         (with-meta
           (fn [& args#]
             (apply invoke-remote ~sc [~remote-ns ~remote-name ~fsource (into [] args#)]))
           {:remote-fn true
            :client ~sc
            :remote-ns ~remote-ns
            :remote-name ~remote-name
            :source ~fsource}
           )))))
