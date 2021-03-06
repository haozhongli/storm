;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns org.apache.storm.serialization-test
  (:use [clojure test])
  (:import [org.apache.storm.serialization KryoTupleSerializer KryoTupleDeserializer
            KryoValuesSerializer KryoValuesDeserializer])
  (:import [org.apache.storm.testing TestSerObject TestKryoDecorator])
  (:import [org.apache.storm.validation ConfigValidation$KryoRegValidator])
  (:import [org.apache.storm.utils Utils])
  (:use [org.apache.storm util config]))

(defn mk-conf [extra]
  (merge (clojurify-structure (Utils/readDefaultConfig)) extra))

(defn serialize [vals conf]
  (let [serializer (KryoValuesSerializer. (mk-conf conf))]
    (.serialize serializer vals)
    ))

(defn deserialize [bytes conf]
  (let [deserializer (KryoValuesDeserializer. (mk-conf conf))]
    (.deserialize deserializer bytes)
    ))

(defn roundtrip
  ([vals] (roundtrip vals {}))
  ([vals conf]
    (deserialize (serialize vals conf) conf)))

(deftest validate-kryo-conf-basic
  (.validateField (ConfigValidation$KryoRegValidator. ) "test" ["a" "b" "c" {"d" "e"} {"f" "g"}]))

(deftest validate-kryo-conf-fail
  (try
    (.validateField (ConfigValidation$KryoRegValidator. ) "test" {"f" "g"})
    (assert false)
    (catch IllegalArgumentException e))
  (try
    (.validateField (ConfigValidation$KryoRegValidator. ) "test" [1])
    (assert false)
    (catch IllegalArgumentException e))
  (try
    (.validateField (ConfigValidation$KryoRegValidator. ) "test" [{"a" 1}])
    (assert false)
    (catch IllegalArgumentException e))
)

(deftest test-java-serialization
  (let [obj (TestSerObject. 1 2)]
    (is (thrown? Exception
      (roundtrip [obj] {TOPOLOGY-KRYO-REGISTER {"org.apache.storm.testing.TestSerObject" nil}
                        TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION false})))
    (is (= [obj] (roundtrip [obj] {TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION true})))))

(deftest test-kryo-decorator
  (let [obj (TestSerObject. 1 2)]
    (is (thrown? Exception
                 (roundtrip [obj] {TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION false})))
    (is (= [obj] (roundtrip [obj] {TOPOLOGY-KRYO-DECORATORS ["org.apache.storm.testing.TestKryoDecorator"]
                                   TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION false})))))

(defn mk-string [size]
  (let [builder (StringBuilder.)]
    (doseq [i (range size)]
      (.append builder "a"))
    (.toString builder)))

(defn is-roundtrip [vals]
  (is (= vals (roundtrip vals))))

(deftest test-string-serialization
  (is-roundtrip ["a" "bb" "cde"])
  (is-roundtrip [(mk-string (* 64 1024))])
  (is-roundtrip [(mk-string (* 1024 1024))])
  (is-roundtrip [(mk-string (* 1024 1024 2))])
  )

(deftest test-clojure-serialization
  (is-roundtrip [:a])
  (is-roundtrip [["a" 1 2 :a] 2 "aaa"])
  (is-roundtrip [#{:a :b :c}])
  (is-roundtrip [#{:a :b} 1 2 ["a" 3 5 #{5 6}]])
  (is-roundtrip [{:a [1 2 #{:a :b 1}] :b 3}]))
