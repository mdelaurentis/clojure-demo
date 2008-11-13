;;;
;;; Crawl Wikipedia
;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Example 1
;; 
;; Define a function to generate a URL for a given Wikipedia article, and one to
;; pull an article name out of a URL.

(defn url-for-topic [topic]
  "Returns the full Wikipedia URL for the given topic."
  (str "http://wikipedia.org/wiki/" topic))

(def wiki-link-pattern (re-pattern "/wiki/(\\w*?)"))

(defn topic-from-url
  "Extracts and returns the Wikipedia topic from the given url, or nil if none
is found."
  ([url]
     (let [matches (when url (re-matches wiki-link-pattern url))]
       (when matches (second matches))))

  {:test (fn []
           (assert (= "Clojure" (topic-from-url "/wiki/Clojure")))
           (assert (not (topic-from-url "/wiki/Special:Foo"))))})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Example 2
;;
;; Write a function to get, parse, and cache a Wikipedia article


(clojure/refer 'clojure.xml)
(clojure/refer 'clojure.set)

(import '(java.io File PushbackReader FileWriter FileInputStream
                  InputStreamReader))

(def project-root 
     (str 
      (. System (getProperty "user.home"))
      (. File separator) "src"
      (. File separator) "clojure-demo"))

(def cache-root
     (str project-root
          (. File separator)
          "cache"))

(defn read-data-from-file [file]
  (when (. file exists)
    (with-open input 
        (new PushbackReader (new InputStreamReader(new FileInputStream file)))
      (read input))))

(defn write-data-to-file [file data]
  (with-open output (new FileWriter file)
    (binding [*out* output]
      (pr data)
      data)))

(defn cache-page [topic]
  "Given a wikipedia topic, returns the cached HTML for the page for that 
topic if we've already visited it, otherwise downloads the HTML from Wikipedia, saves it to the cache, and returns it."
  (let [file (new File (str cache-root topic))]
    (or (read-data-from-file file)
        (write-data-to-file  file (parse (url-for-topic topic))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Example 3
;;
;; Write a function to examine a parsed article and return a set of all linked
;; articles.

(defn topic-from-elem [elem]
  (when (= :a (tag elem))
    (topic-from-url (:href (attrs elem)))))

(defn linked-topics [topic]
  "Returns a set of all the other topics linked from the given topic"
  (disj 
   (apply hash-set
          (filter identity (map topic-from-elem 
                                (xml-seq (cache-page topic)))))
   topic))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Example 4
;;
;; Create a thread-safe stream of Wikipedia topics
;; 

(def visited (ref #{}))

(defn random-unvisited-topic [topics]
  (dosync 
   (let [unvisited (difference topics @visited)
         topic     (nth (seq unvisited) 
                        (rand-int (count unvisited)))]
     (alter visited conj topic)
     topic)))

(defn topic-seq [topic]
  "Return a lazy sequence of topics reachable from the given topic, excluding
topics present in @visited "
  (lazy-cons 
   topic
   ;; Fetch linked topics outside of transaction, in case it's retried
   (topic-seq (random-unvisited-topic (linked-topics topic)))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Example 5 
;; 
;; Create an agent for logging messages
  

(import '(java.util Date)
        '(java.text DateFormat))

(def *log* *out*)

(def logger (agent nil))

(defn log-action [logger msg]
  (binding [*out* *log*]
    (printf "%s: %s%n" (new Date) msg)
    (flush)))

(defn log [& msgs]
  "Sends a message to the logger agent, telling it to print the given 
messages to its log file."
  (send-off logger log-action (apply str msgs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Example 6
;;
;; Create agents for crawling Wikipedia.

(def running true)

(defn crawler-for-topic [topic]
  (agent {:origin topic
          :trail nil}))

(def crawlers (map crawler-for-topic '("Beagle" "Clojure" "Lisp")))

(defn crawl 
  
  ([crawler]
     (when-let topic (first (:trail crawler))
       (crawl 
        (assoc crawler :trail
               (rest (:trail crawler)))
        topic)))
  
  ([crawler topic]
     (when running
        (log "Crawler " (:origin crawler) " is visiting " topic)
        (if-let next (random-unvisited-topic (linked-topics topic))
          (do
            (send-off *agent* crawl next)
            (assoc crawler :trail
                   (cons topic (:trail crawler))))
          (crawl crawler))))) 

(defn start-crawling []
  (def running true)
  (dorun (map #(send-off % crawl (:origin @%)) crawlers)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Example 7
;;
;; Create a watchdog agent to monitor crawlers.

(def watchdog (agent nil))

(defn watch-crawlers [state]
  (let [poke (fn [crawler]
               (when-let errors (agent-errors crawler)
                 (clear-agent-errors crawler)
                 (log "Crawler " (:origin @crawler) " had error " errors)
                 (when (:trail @crawler)
                   (send-off crawler crawl))))]
    (when running
      (. Thread (sleep 5000))
      (send-off *agent* watch-crawlers)    
      (dorun (map poke crawlers)))))

(defn start-watching []
  (send-off watchdog watch-crawlers))

(defn shutdown []
  (def running false))


(comment

  ;; Typical usage
  
  ;; Start all the crawlers and the watchdog
  (start-crawling)
  (start-watching)
  
  ;; Crawl for some time

  ;; Then stop the agents
  (shutdown)

  )

(time (do
        (parse (url-for-topic "Clojure"))
        nil))