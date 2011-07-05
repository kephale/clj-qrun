(defproject clj-qrun "1.0.3-SNAPSHOT"
  :description "FIXME: write"
  :main clj-qrun.core
  :dev-dependencies [[swank-clojure "1.3.0-SNAPSHOT"]]  
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
		 [clj-serializer "0.1.1"]
		 [fleetdb-client "0.2.2"]
                 [kephale/rabbitcj "0.1.1-SNAPSHOT"]])
