(ns onyx-stream-sql.core
  (:require [onyx-local-rt.api :as api]
            [clojure.pprint :as pp]))

;; ^:export the function if using in ClojureScript.
(defn ^:export my-inc [segment]
  (update-in segment [:n] inc))

(def job
  {:workflow [[:in :inc] [:inc :out]]
   :catalog [{:onyx/name :in
              :onyx/type :input
              :onyx/batch-size 20}
             {:onyx/name :inc
              :onyx/type :function
              :onyx/fn ::my-inc
              :onyx/batch-size 20}
             {:onyx/name :out
              :onyx/type :output
              :onyx/batch-size 20}]
   :lifecycles []})

(pp/pprint
 (-> (api/init job)
     (api/new-segment :in {:n 41})
     (api/new-segment :in {:n 84})
     (api/drain)
     (api/stop)
     (api/env-summary)))

(defn add-segment [env [input-name segment]]
  (api/new-segment env input-name {:src input-name
                                   :data segment}))

(defn runtest [job segments]
  (let [env (api/init job)
        nextenv (reduce add-segment
                        env segments)]
    (-> nextenv
        (api/drain)
        (api/stop)
        (api/env-summary))))
;; =>
;; {:next-action :lifecycle/start-task?,
;;  :tasks
;;  {:in {:inbox []},
;;   :inc {:inbox []},
;;   :out {:inbox [], :outputs [{:n 42} {:n 85}]}}}

(def test1
  {:select [:b/age :b/name]
   :from [:person :b]
   :where [:age-large]})

(def test2
  {:select [:b/age :b/name :c/country-name]
   :from [:person :b]
   :join {:target [:country :c]
          :on-key [:c/country-code :b/country]}
   :where [:age-large]})

(defn gen-input [name]
  {:onyx/name name
   :onyx/type :input
   :onyx/batch-size 1})

(defn gen-output [name]
  {:onyx/name name
   :onyx/type :output
   :onyx/batch-size 1})

(defn sum-init-fn [window]
  {:src-hash {}
   :dst-hash {}})

(defn sum-aggregation-fn [window segment]
  (do 
    (if (= (:src segment) :person)
      (let [k (-> segment :data :country)]
        {:value k :loc :src-hash :segment segment})
      (let [k (-> segment :data :country-code)]
        {:value k :loc :dst-hash :segment segment}))))

;; Now just pull out the value and add it to the previous state
(defn sum-application-fn [window state value]
  (update-in state [(:loc value) (:value value)]
             (constantly (:segment value))))


;; sum aggregation referenced in the window definition.
(def sum
  {:aggregation/init sum-init-fn
   :aggregation/create-state-update sum-aggregation-fn
   :aggregation/apply-state-update sum-application-fn})

;;(defn dump-window! [event window trigger x state]
;;  (doall (map pp/pprint [event window trigger x state])))

(defn dump-window! [event window trigger x state]
  (if (= (:event-type x) :new-segment)
    (let [segment (:segment x)
          src (:src segment)
          data (:data segment)
          lookup-dst (get-in state [:dst-hash (:country data) :data] false)
          lookup-src (get-in state [:src-hash (:country-code data) :data] false)]
      (cond (and (= src :person) lookup-dst)
        {:person data :country lookup-dst}
            (and (= src :country) lookup-src)
        {:person lookup-src :country data}))))

(defn rev [[a b]]
  [b a])
(defn grab-alias [sql]
  (->> [(:from sql)
        (get-in sql [:join :target])]
       (map rev)
       (into {})))

;(defn run-process [sql segment]
;  (let [ks (map #(keyword (name %)) (:select sql))]
;    (select-keys (:data segment) ks)))

(defn run-process [sql segment]
  (let [alias (grab-alias sql)
        ks (:select sql)]
    (into {} (for [k ks
                   :let [k-ns (keyword (namespace k))
                         k-n (keyword (name k))
                         entity (get alias k-ns)]]
               [k (get-in segment [entity k-n])]))))

(defn compile-query [sql]
  (let [input-name (first (:from sql))
        output-name :out
        has-join? (contains? sql :join)
        join-name (if has-join? 
                    (-> sql
                        (get-in [:join :target])
                        (first)))]
    {:workflow (cond-> []
                 true (conj [input-name (if has-join? :join :proc)])
                 has-join? (conj [join-name :join])
                 has-join? (conj [:join :proc])
                 true (conj [:proc output-name]))
     :catalog (cond-> []
                true (conj (gen-input input-name))
                has-join? (conj (gen-input join-name))
                has-join? (conj {:onyx/name :join
                                 :onyx/type :function
                                 :onyx/fn :clojure.core/identity
                                 :onyx/batch-size 1})
                true (conj {:onyx/name :proc
                            :onyx/type :function
                            :onyx/fn ::run-process
                            :streamql/sql sql
                            :onyx/params [:streamql/sql]
                            :onyx/batch-size 1})
                true (conj (gen-output output-name)))
     :lifecycles []
     :windows [{:window/id :collect-segments
                :window/task :join
                :window/type :global
                :window/aggregation ::sum}]
     :triggers [{:trigger/window-id :collect-segments
                 :trigger/id :sync
                 :trigger/on :onyx.triggers/segment
                 :trigger/threshold [1 :elements]
                 :trigger/emit ::dump-window!}]}))

(runtest
 (compile-query test1)
 [[:person {:age 34 :name "Mary" :other true :test 123}]
  [:person {:age 67 :name "Peter" :other false :test 0}]
  [:person {:age 21 :name "Hackle" :other true}]
  [:person {:age 18 :name "Oliver" :other false :foo :bar}]])

(runtest
 (compile-query test2)
 [[:person {:age 34 :name "Mary" :other true :test 123 :country "HKG"}]
  [:country {:country-code "RUS" :country-name "Russia"}]
  [:country {:country-code "IND" :country-name "India"}]
  [:person {:age 67 :name "Peter" :other false :test 0 :country "RUS"}]
  [:country {:country-code "JAP" :country-name "Japan"}]
  [:person {:age 21 :name "Hackle" :other true :country "JAP"}]
  [:person {:age 18 :name "Oliver" :other false :foo :bar :country "RUS"}]
  [:country {:country-code "HKG" :country-name "Hong Kong"}]])
