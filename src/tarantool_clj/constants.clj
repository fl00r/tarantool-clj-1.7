(ns tarantool-clj.constants)

;; https://github.com/tarantool/tarantool/blob/4e06b8e5bd8666bb82ab8f3f92ea6e7ed776bf7c/src/box/index.h#L68
(def ITERATORS
  {:eq 0
   :req 1
   :all 2
   :lt 3
   :le 4
   :ge 5
   :gt 6
   :bits-all-set 7
   :bits-any-set 8
   :overlaps 10
   :neighbor 11})

(def SPACES-SPACE-ID 280)

(def INDEXES-SPACE-ID 288)


;; https://tarantool.org/en/doc/dev_guide/internals_index.html#box-protocol-iproto-protocol
(def USER-KEYS
  {:code 0x00
   :sync 0x01
   :schema-id 0x05
   :space-id 0x10
   :index-id 0x11
   :limit 0x12
   :offset 0x13
   :iterator 0x14
   :key 0x20
   :tuple 0x21
   :function-name 0x22
   :username 0x23
   :expression 0x27
   :ops 0x28
   :data 0x30
   :error 0x31})

(def REQUEST-CODES
  {:select 0x01
   :insert 0x02
   :replace 0x03
   :update 0x04
   :delete 0x05
   :call-1.6 0x06
   :auth 0x07
   :eval 0x08
   :upsert 0x09
   :call 0x0a
   :ping 0x40})

(def RESPONSE-CODES
  {:ok 0x00
   :error nil ; 0x8XXX
   })


(def GREETING-LENGTH 128)

(def GREETING-VERSION-LENGTH 64)

(def GREETING-SALT-LENGTH 44)

(def HEADER-LENGTH 5)

(def DEFAULT-CONNECTION
  {:host "localhost"
   :port 3301
   :auto-reconnect? true
   :request-timeout 10000
   :async? true})

(def SCRAMBLE-SIZE 20)
