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
;; REQUESTS --------------------------------------
;; -----------------------------------------------
(define-exported-function request-url-suffix (url-suffix)
  (let ((http-response
          (http-request (format nil "~a:~a~a" *riak-host* *riak-port* url-suffix))))
    (typecase http-response
      (string
        http-response)
      (vector
        (octets-to-string http-response))
      (otherwise
        http-response))))

(define-exported-function request (bucket key)
  (request-url-suffix (format nil "/riak/~a/~a" bucket key)))

;; -----------------------------------------------
;; TEST ------------------------------------------
;; -----------------------------------------------
(defvar *test* (request "test" "foo"))

;;   (octets-to-string 
;;(setq *header-stream* *standard-output*)
;;(setf *drakma-default-external-format* :utf-8)
