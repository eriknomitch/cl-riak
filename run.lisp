;; ===============================================
;; CL-RIAK =======================================
;; ===============================================
(in-package #:cl-user)

(defpackage #:cl-riak
  (:use #:cl
        #:utility
        #:drakma
        #:flexi-streams))

(in-package #:cl-riak)

;; -----------------------------------------------
;; GLOBALS ---------------------------------------
;; -----------------------------------------------
(defvar *riak-host* "http://127.0.0.1")
(defvar *riak-port* 8098)

;; -----------------------------------------------
;; URLS ------------------------------------------
;; -----------------------------------------------
(define-function make-url-suffix (bucket key)
  (format nil "/riak/~a/~a" bucket key))

;; -----------------------------------------------
;; RESPONSES -------------------------------------
;; -----------------------------------------------
(define-function parse-http-response (http-response)
  (typecase http-response
    ;; This is a normal response
    (string
      http-response)
    ;; This is the JSON description thing
    ((simple-array (unsigned-byte 8))
      (octets-to-string http-response))
    ;; This is unknown
    (otherwise
      http-response)))

;; -----------------------------------------------
;; REQUESTS --------------------------------------
;; -----------------------------------------------
(define-function request-url-suffix (url-suffix &key (method :get)
                                                     (content nil))
  (parse-http-response
    (http-request (format nil "~a:~a~a" *riak-host* *riak-port* url-suffix)
                  :method  method
                  :content content)))

;; External  - - - - - - - - - - - - - - - - - - -
(define-exported-function $request (bucket key &optional (value nil))
  (if value
    (request-url-suffix (make-url-suffix bucket key) :method :post :content value)
    (request-url-suffix (make-url-suffix bucket key))))

(define-exported-function $delete (bucket key)
  (request-url-suffix (make-url-suffix bucket key) :method :delete))

;; -----------------------------------------------
;; TEST ------------------------------------------
;; -----------------------------------------------
($request "test" "foo" "This is foo.")
($request "test" "bar" "This is bar.")

(defvar *get* ($request "test" "foo"))
(defvar *add* ($request "test" "bar" "This is a new bar."))

(format t "~a~%" *get*)
(format t "~a~%" *add*)
