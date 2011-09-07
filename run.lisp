;; ===============================================
;; CL-RIAK =======================================
;; ===============================================
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
(define-exported-function request (url-suffix)
  (octets-to-string (http-request (format nil "~a:~a~a" *riak-host* *riak-port* url-suffix))))

;; -----------------------------------------------
;; TEST ------------------------------------------
;; -----------------------------------------------
(defvar *test* (request "/riak/test"))


;;(setq *header-stream* *standard-output*)
;;(setf *drakma-default-external-format* :utf-8)
