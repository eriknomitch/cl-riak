;; ===============================================
;; CL-RIAK =======================================
;; ===============================================
(defpackage #:cl-riak
  (:use #:cl
        #:drakma
        #:flexi-streams))

(in-package #:cl-riak)

;; -----------------------------------------------
;; TEST ------------------------------------------
;; -----------------------------------------------
;;(setq *header-stream* *standard-output*)
;;(setf *drakma-default-external-format* :utf-8)

(defvar *riak*
  (http-request "http://127.0.0.1:8098/riak/test"))

(format t "~a~%" *riak*)
(format t "~a~%" (octets-to-string *riak*))
