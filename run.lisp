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
  ;; FIX: We need to handle "Not Found"
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
                                                     (content nil)
                                                     (content-type "text/plain"))
  (parse-http-response
    (http-request (format nil "~a:~a~a" *riak-host* *riak-port* url-suffix)
                  :method       method
                  :content-type content-type
                  :content      content)))

;; External  - - - - - - - - - - - - - - - - - - -
(define-exported-function $request (bucket key &optional (value nil))
  (apply #'request-url-suffix
         (append `(,(make-url-suffix bucket key) ,@(when value (list :method :post :content value))))))

(define-exported-function $delete (bucket key)
  (request-url-suffix (make-url-suffix bucket key) :method :delete))

;; -----------------------------------------------
;; LINKS -----------------------------------------
;; -----------------------------------------------

;; -----------------------------------------------
;; TEST ------------------------------------------
;; -----------------------------------------------
($request "test" "foo" "This is foo.")
($request "test" "bar" "This is bar.")

(defvar *get* ($request "test" "foo"))
(defvar *add* ($request "test" "bar" "This is a new bar."))

(format t "~a~%" *get*)
(format t "~a~%" *add*)
