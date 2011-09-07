;; ===============================================
;; CL-RIAK =======================================
;; ===============================================
(in-package #:cl-user)

(defpackage #:cl-riak
  (:use #:cl
        #:utility
        #:drakma
        #:flexi-streams
        #:yason))

(in-package #:cl-riak)

;; -----------------------------------------------
;; EXTEND->YASON ---------------------------------
;; -----------------------------------------------
(defun yason::encode-to-string (hash-table)
  (with-output-to-string (stream)
    (yason:encode hash-table stream)))

;; -----------------------------------------------
;; GLOBALS ---------------------------------------
;; -----------------------------------------------
(defvar *riak-host* "http://127.0.0.1")
(defvar *riak-port* 8098)

;; -----------------------------------------------
;; URLS ------------------------------------------
;; -----------------------------------------------
(define-function make-url-suffix (bucket key &key (get-data nil))
  (format nil "/riak/~a~a~a" bucket
          (if key
            (concatenate 'string "/" key)
            "")
          (if get-data
            (concatenate 'string "?" get-data)
            "")))

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

;; Exported  - - - - - - - - - - - - - - - - - - -
(define-exported-function $request (bucket key &optional (value nil))
  (let ((is-hash-table (typep value 'hash-table)))
    (apply #'request-url-suffix
           (append `(,(make-url-suffix bucket key)
                      ,@(when value 
                          (append
                            (list :method :post 
                                  :content (or (and is-hash-table
                                                    (yason::encode-to-string value))
                                               value))
                            (when is-hash-table
                                 '(:content-type "application/json")))))))))

(define-exported-function $delete (bucket key)
  (request-url-suffix (make-url-suffix bucket key) :method :delete))

;; -----------------------------------------------
;; INFORMATION -----------------------------------
;; -----------------------------------------------
;; Exported  - - - - - - - - - - - - - - - - - - -
(define-exported-function $list-keys (bucket)
  (request-url-suffix (make-url-suffix bucket nil :get-data "keys=true")))

;; -----------------------------------------------
;; LINKS -----------------------------------------
;; -----------------------------------------------

;; -----------------------------------------------
;; TESTS -----------------------------------------
;; -----------------------------------------------
($request "test" "foo" "This is foo.")
($request "test" "bar" "This is bar.")

(defvar *baz* (make-hash-table))
(setf (gethash "foo" *baz*) "bar")
($request "test" "baz" *baz*)

(defvar *get* ($request "test" "foo"))
(defvar *add* ($request "test" "bar" "This is a new bar."))

(format t "~a~%" *get*)
(format t "~a~%" *add*)

