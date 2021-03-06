(ns simple-queue.core
  (:require [langohr.core      :as rmq]
            [langohr.channel   :as lch]
            [langohr.queue     :as lq]
            [langohr.consumers :as lc]
            [langohr.basic     :as lb]
            [langohr.exchange  :as le]
            [com.stuartsierra.component :as component]))


(defrecord Queue [exchange-name connection-info qname]
  component/Lifecycle
  (start [component]
    (println "Connection to Rabbit MQ message bus")
    (let [conn (rmq/connect (or connection-info rmq/*default-config*))
          ch   (lch/open conn)
          q    (.getQueue (lq/declare ch (or qname "") {:exclusive false :auto-delete false}))]
      (le/declare ch exchange-name "fanout" {:durable true})
      (lq/bind ch q exchange-name)
      (-> component
          (assoc :channel ch)
          (assoc :exchange exchange-name)
          (assoc :qname (or qname ""))
          (assoc :connection conn))))

  (stop [component]
    (println "Closing connection to Rabbit MQ")
    (rmq/close (:channel component))
    (rmq/close (:connection component))
    (->
     (assoc :channel nil)
     (assoc :connection nil)
     (assoc :exchange nil)
     (assoc :qname nil))))

(defn create-queue
  ([exchange-name]
   (.start (map->Queue {:exchange-name exchange-name})))
  ([exchange-name connection-info]
   (.start (map->Queue {:exchange-name exchange-name :connection-info connection-info}))))

(defn publish [component message]
  (lb/publish (:channel component) (:exchange component) (:qname component) message {:content-type "text/plain"}))

(defn subscribe-blocking [component message-handler]
  (lc/blocking-subscribe (:channel component) (:qname component) message-handler {:auto-ack true}))

(defn subscribe [component message-handler]
  (lc/subscribe (:channel component) (:qname component) message-handler {:auto-ack true}))

(defmacro defhandler [name & body]
  `(defn ~name [~'component]
     (fn
       [~'ch {:keys [content-type# delivery-tag#] :as ~'meta} ^"bytes" payload#]
       (let  [~'data (String. payload# "UTF-8")]
         ~@body))))
