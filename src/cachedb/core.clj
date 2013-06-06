(ns cachedb.core
  (:import [org.bson.types ObjectId]))

(defn- validate-keys? [coll keys]
  (every? (-> (:keys coll) (set)) keys))

(defn collection
  "create a cachedb collection."
  [keys indexes]
  {:pre [(every? keyword? keys)
         (every? #(every? (set keys) %) indexes)]}
  (let [idx-map (reduce #(assoc %1 (set %2) {}) {} indexes)
        keys (if (not-any? (partial = :id) keys)
               (concat [:id] keys)
               keys)]
    {:keys keys, :idx idx-map, :data {}}))

(defn- update-index [coll f doc]
  (loop [[[idx-key idx-data] & more-idxes] (seq (:idx coll)), coll coll]
        (if idx-key
          (let [coll (update-in coll [:idx idx-key (select-keys doc idx-key)] #(if (nil? %1) [%2] (f %1 %2)) doc)]
            (if (seq more-idxes)
              (recur more-idxes coll)
              coll))
          coll)))

(defn- insert-doc [coll {:keys [id] :as doc}]
  (-> (assoc-in coll [:data id] doc)
      (update-index conj doc)))

(defn insert [coll doc]
  {:pre [(validate-keys? coll (keys doc))]}
  (if-let [id (:id doc)]
    (when-not (contains? (:data coll) id)
      (insert-doc coll doc))
    (->> (assoc doc :id (ObjectId.))
         (insert-doc coll))))

(def user (collection [:a :b :c] [[:a] [:b]]))

(identity user)

(time (-> user
          (insert {:a 1 :b 2 :c 3})
          (insert {:a 1 :b 1 :c 4})
          (insert {:a 2 :b 1 :c 3})))

(defn- get-index [coll condition]
  (let [cond-keys-set (-> (keys condition) (set))]
    (get-in coll [:idx cond-keys-set])))

(defn- get-ids-from-indexes [coll condition]
  (-> (get-index coll condition)
      (get condition)
      (map :id)
      (seq)))

(defn- get-ids-from-data [coll condition]
  ;; TODO 从数据集中提取id
  )

(defn- get-doc-ids [coll condition]
  (or (get-ids-from-indexes coll condition)
      (get-ids-from-data coll condition)))

(defn update [coll condition doc]
  {:pre [(validate-keys? coll (keys doc))
         (not (contains? doc :id))]}
  (if-let [ids (get-doc-ids coll condition)]
    ;; TODO
    ))

(defn upsert [coll condition doc]
  {:pre [(validate-keys? coll (keys doc))
         (not-every? #(contains? % :id) [condition doc])]}
  ;; TODO
  )

(defcoll users [:name :age :address] [[:name] [:age]])

(dosync
 (insert users {:id 5 :name "bbb" :age 1}))

(clojure.pprint/pprint (deref users))

(comment
  ;; TODO 根据新结构重新实现
  (defn fetch
    ([coll condition]
       {:pre [(->> (keys condition) (validate-keys? coll))]}
       (let [cond-keys-set (-> (keys condition) set)]
         (if-let [idx (get-in coll [:idx cond-keys-set])]
           (-> (get idx condition) set)
           (select #(every? (fn [[k v]] (= (get % k) v)) condition) (:doc-set coll)))))
    ([coll condition fields]
       {:pre [(validate-keys? fields)]}
       (-> (fetch coll condition)
           (project fields))))

  (defn fetch-by-id
    ([coll id]
       {:pre [(legal-coll? coll)]}
       (-> (get-in coll [:idx #{:id} {:id id}]) (first)))
    ([coll id fields]
       {:pre [(validate-keys? coll fields)]}
       (some-> (fetch-by-id coll id)
               (select-keys fields))))



  (def ds #{{:id 1 :a 2 :b 3 :c 4} {:id 2 :a 5 :b 6 :c 7} {:id 3 :a 1 :b 3 :c 7}})

  (def tst {:keys [:id :a :b :c]
            :full-txt-keys []
            :idx {#{:id} (clojure.set/index ds [:id])
                  #{:b :c} (clojure.set/index ds [:b :c])}
            :doc-set ds})

  (fetch tst {:b 3 :c 7} )

  (fetch-by-id tst 1))
