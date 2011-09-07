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

(define-function make-url-suffix (bucket key)
  (format nil "/riak/~a/~a" bucket key))

(define-exported-function request-url-suffix (url-suffix &key (method :get)
                                                              (content nil))
  (parse-http-response
    (http-request (format nil "~a:~a~a" *riak-host* *riak-port* url-suffix)
                  :method  method
                  :content content)))

;;   (let ((http-response
;;           (http-request (format nil "~a:~a~a" *riak-host* *riak-port* url-suffix)
;;                         :method     method
;;                         :content "this is another test")))
;;     (typecase http-response
;;       ;; This is a response
;;       (string
;;         http-response)
;;       ;; This is the JSON description thing
;;       ((simple-array (unsigned-byte 8))
;;         (format t "~a~%" (type-of http-response))
;;         (octets-to-string http-response))
;;       ;; This is unknown
;;       (otherwise
;;         http-response))))

(define-exported-function request (bucket key &optional (value nil))
  (if value
    (request-url-suffix (make-url-suffix bucket key) :method :post :content value)
    (request-url-suffix (make-url-suffix bucket key))))

;; -----------------------------------------------
;; TEST ------------------------------------------
;; -----------------------------------------------
(defvar *get* (request "test" "foo"))
(defvar *add* (request "test" "bar" "test1"))

;;(setq *header-stream* *standard-output*)
;;(setf *drakma-default-external-format* :utf-8)

;;(format t "~a~%" *get*)
