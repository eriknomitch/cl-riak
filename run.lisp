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
(define-function make-url-suffix (bucket key &key (link-tuples nil) (get-data nil) (wrap nil))
  (let ((raw-url-suffix
          (format nil "/riak/~a~a~a~a" bucket
                  (if key
                    (concatenate 'string "/" key)
                    "")
                  (if link-tuples
                    (concatenate 'string "/" (format-link-tuples link-tuples))
                    "")
                  (if get-data
                    (concatenate 'string "?" get-data)
                    ""))))
    (if wrap
      (concatenate 'string "<" raw-url-suffix ">")
      raw-url-suffix)))

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
                                                     (content-type "text/plain")
                                                     (additional-headers nil))
  (parse-http-response
    (http-request (format nil "~a:~a~a" *riak-host* *riak-port* url-suffix)
                  :method             method
                  :additional-headers additional-headers
                  :content-type       content-type
                  :content            content)))

;; Exported  - - - - - - - - - - - - - - - - - - -
;; FIX: This will probably remove any links when we update the value: http://wiki.basho.com/Links-and-Link-Walking.html
(define-exported-function $request (bucket key &optional (value nil))
  (let ((is-hash-table (typep value 'hash-table)))
    (apply #'request-url-suffix
           (append `(,(make-url-suffix bucket key)
                      ,@(when value 
                          (append
                            (list :method :post 
                                  :content (if is-hash-table
                                             (yason::encode-to-string value)
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
  ;; FIX: Why is this so slow? Yason's fault?
  ;; FIX: We need to catch HTTP status and handle it.
  (let* ((json-string 
           (request-url-suffix (make-url-suffix bucket nil :get-data "keys=true")))
         (json-hash-table
           (yason:parse json-string)))
    (gethash "keys" json-hash-table)))

;; -----------------------------------------------
;; LINKS -----------------------------------------
;; -----------------------------------------------
(define-function link-header (bucket key riak-tag)
  (concatenate 'string 
    (make-url-suffix bucket key :wrap t)
    "; riaktag=\""
    riak-tag
    "\""))

(define-function format-link-tuples (link-tuples)
  (concatenate 'string
    (list-delimited-by
      (mapcar #'(lambda (link-tuple)
                  (list-delimited-by link-tuple ","))
              link-tuples)
      "/")
    "/"))

;; Exported  - - - - - - - - - - - - - - - - - - -
;; CHECK: This method of handling existing content is almost certainly not the correct way to do this.
(define-exported-function $link (from-bucket from-key riak-tag to-bucket to-key &key (content nil))
  (if content
    ;; If :content was passed, ensure it
    (request-url-suffix (make-url-suffix from-bucket from-key)
      :method :put
      :content content
      :additional-headers `(("Link" . ,(link-header to-bucket to-key riak-tag))))
    ;; If :content was not passed, we'll try to find existing content
    (let ((existing-content ($request from-bucket from-key)))
      ($link from-bucket from-key riak-tag to-bucket to-key :content (or existing-content "")))))

(define-exported-function $link-walk (bucket key &rest link-tuples)
  (request-url-suffix (make-url-suffix bucket key :link-tuples link-tuples)))

;; -----------------------------------------------
;; TESTS -----------------------------------------
;; -----------------------------------------------
(define-exported-function link-test ()
  ($link "test" "foo" "friend" "test" "bar"))

;; - - - - - - - - - - - - - - - - - - - - - - - -
($request "test" "foo" "This is foo.")
($request "test" "bar" "This is bar.")

(defvar *baz* (make-hash-table))
(setf (gethash "foo" *baz*) "bar")
($request "test" "baz" *baz*)

(defvar *get* ($request "test" "foo"))
(defvar *add* ($request "test" "bar" "This is a new bar."))

(format t "~a~%" *get*)
(format t "~a~%" *add*)

