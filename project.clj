(defproject jepsen.swytch "0.1.0-SNAPSHOT"
  :description "Jepsen test suite for Swytch distributed cache"
  :url "https://github.com/bottled-codes/swytch.jepsen"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :main jepsen.swytch
  :dependencies [[org.clojure/clojure "1.12.4"]
                 [jepsen "0.3.11-SNAPSHOT"]
                 [io.jepsen/generator "0.1.1"]
                 [com.taoensso/carmine "3.5.0"]
                 [com.github.seancorfield/next.jdbc "1.3.939"]
                 [org.postgresql/postgresql "42.7.4"]]
  :jvm-opts ["-Xmx8g"
             "-Djava.awt.headless=true"
             ;; pgJDBC connects stall when the JVM tries IPv6 first
             ;; and the route drops. Nodes are addressed by IPv4
             ;; literals; force IPv4 so Java doesn't second-guess.
             "-Djava.net.preferIPv4Stack=true"
             "-server"]
  :profiles {:uberjar {:aot :all}})
