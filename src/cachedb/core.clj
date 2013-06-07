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

(defn- insert-internal [coll doc]
  (if-let [id (:id doc)]
    (when-not (contains? (:data coll) id)
      (insert-doc coll doc))
    (->> (assoc doc :id (ObjectId.))
         (insert-doc coll))))

(defn insert [coll doc]
  {:pre [(validate-keys? coll (keys doc))]}
  (insert-internal coll doc))

(defn- use-index? [{:keys [idx]} condition]
  (every? (-> (keys idx) (set)) (keys condition)))

(defn- get-effect-ids-in-index [{:keys [idx]} condition]
  (->> (keys condition)
       (map #(get-in idx [% (get condition %)]))
       (apply clojure.set/intersection)))

(defn- match-condition [condition doc]
  (every? (fn [[k v]] (= (k doc) v)) condition))

(defn- get-effect-ids [{:keys [data] :as coll} {cond-id :id :as condition}]
  (cond
   cond-id (when (get data cond-id) #{cond-id})
   (use-index? coll condition) (get-effect-ids coll condition)
   :else (->> (filter (partial match-condition condition) (vals data))
              (reduce #(conj (or %1 #{}) (:id %2)) nil))))

(defn- get-docs-by-ids [data ids]
  (reduce #(->> (get data %2) (conj %1)) [] ids))

(defn- update-index-by-multi-condition [coll ids conditions doc]
  (let [doc-keys (keys doc)]
    (-> (reduce #(update-index %1 disj (select-keys %2 doc-keys) ids) coll conditions)
        (update-index conj doc ids))))

(defn- update-docs-by-ids [{:keys [data] :as coll} ids doc]
  (let [old-docs (get-docs-by-ids data ids)]
      (-> (reduce #(update-in %1 [:data %2] merge doc) coll ids)
          (update-index-by-multi-condition ids old-docs doc))))

(defn- update-internal [coll condition doc]
  (if-let [ids (get-effect-ids coll condition)]
    (update-docs-by-ids coll ids doc)
    coll))

(defn update [coll condition doc]
  {:pre [(validate-keys? coll (keys doc))
         (not (contains? doc :id))]}
  (update-internal coll condition doc))

(defn- exists-internal [{:keys [data] :as coll} {cond-id :id :as condition}]
  (cond
   cond-id (contains? data cond-id)
   (use-index? coll condition) (not (nil? (-> (get-effect-ids-in-index coll condition) (seq))))
   :else (not (nil? (some (partial match-condition condition) (vals data))))))

(defn exists? [coll condition]
  (exists-internal coll condition))

(defn upsert [coll condition doc]
  {:pre [(validate-keys? coll (keys doc))
         (not-every? #(contains? % :id) [condition doc])]}
  (if-let [ids (get-effect-ids coll condition)]
    (update-docs-by-ids coll ids doc)
    (->> (merge condition doc) (insert-internal coll))))

(def user (collection [:a :b :c] [:a :b]))
(-> user
    (insert {:id 1 :a 1 :b 2 :c 3})
    (insert {:id 2 :a 1 :b 1 :c 4})
    (insert {:id 3 :a 2 :b 1 :c 3})
    (update {:id 4} {:a 3 :b 3 :c 5})
    (exists? {:a 2 :c 4})
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
