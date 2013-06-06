(defproject cachedb "0.1.0-SNAPSHOT"
  :description "Cache something in memory."
  :url "http://github.com/Ruiyun/cachedb.git"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[com.novemberain/monger "1.6.0-beta2"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.5.1"]]}}
  :global-vars {*warn-on-reflection* true
                *print-length* 200
                *print-level* 10})
