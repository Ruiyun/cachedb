(ns cachedb.core
  (:import [org.bson.types ObjectId]))

(defn- validate-keys? [coll keys]
  (every? (-> (:keys coll) (set)) keys))

(defn collection
  "create a cachedb collection."
  [keys indexes]
  {:pre [(every? keyword? keys)
         (every? keyword? indexes)]}
  (let [idx-map (reduce #(assoc %1 %2 (sorted-map)) {} indexes)
        keys (if (not-any? (partial = :id) keys)
               (concat [:id] keys)
               keys)]
    {:keys keys, :idx idx-map, :data {}}))

(defn- update-index [coll f condition ids]
  (letfn [(init-idx-if-need [idx-data f ids]
            (apply f (or idx-data #{}) ids))]
    (reduce #(update-in %1 [:idx %2 (get condition %2)] init-idx-if-need f ids)
            coll (-> (:idx coll) (keys)))))

(defn- insert-doc [coll {:keys [id] :as doc}]
  (-> (assoc-in coll [:data id] doc)
      (update-index conj doc [(:id doc)])))

(defn insert [coll doc]
  {:pre [(validate-keys? coll (keys doc))]}
  (if-let [id (:id doc)]
    (when-not (contains? (:data coll) id)
      (insert-doc coll doc))
    (->> (assoc doc :id (ObjectId.))
         (insert-doc coll))))

(defn- use-index? [{:keys [idx] :as coll} condition]
  (every? (-> (keys idx) (set)) (keys condition)))

(defn update [coll condition doc]
  {:pre [(validate-keys? coll (keys doc))
         (not (contains? doc :id))]}
  (if-let [ids (cond
                (contains? condition :id) [(:id condition)]
                (use-index? coll condition) (->> (keys condition)
                                                 (map #(get-in coll [:idx % (get condition %)]))
                                                 (apply clojure.set/intersection))
                :else (->> (filter #(every? (fn [[k v]] (= (k %) v)) condition) (-> (:data coll) (vals)))
                           (map :id)))]
    (let [doc-keys (keys doc)
          old-docs (reduce (fn [docs id]
                             (->> (-> (get (:data coll) id)
                                      (select-keys doc-keys))
                                  (conj docs)))
                           [] ids)]
      (-> (reduce #(update-in %1 [:data %2] merge doc) coll ids)
          ((fn [coll]
             (reduce #(update-index %1 disj %2 ids) coll old-docs)))
          (update-index conj doc ids)))
    coll))

(def user (collection [:a :b :c] [:a :b]))
(-> user
    (insert {:id 1 :a 1 :b 2 :c 3})
    (insert {:id 2 :a 1 :b 1 :c 4})
    (insert {:id 3 :a 2 :b 1 :c 3})
    (update {:c 3} {:a 3 :b 3 :c 5})
    )

(defn upsert [coll condition doc]
  {:pre [(validate-keys? coll (keys doc))
         (not-every? #(contains? % :id) [condition doc])]}
  ;; TODO
  )

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
