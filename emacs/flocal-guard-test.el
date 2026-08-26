;;; flocal-guard-test.el --- Tests for flocal-guard -*- lexical-binding: t; -*-

(require 'ert)
(require 'cl-lib)
(require 'flocal-guard)

(ert-deftest flocal-guard-most-specific-share-wins ()
  (let* ((root (make-temp-file "flocal-guard-" t))
         (nested (expand-file-name "nested" root))
         (file (expand-file-name "note.txt" nested)))
    (unwind-protect
        (progn
          (make-directory nested)
          (write-region "" nil file nil 'silent)
          (let ((flocal-guard--shares (list (cons root '((share . "outer")))
                                            (cons nested '((share . "inner"))))))
            (should (equal (alist-get 'share (cdr (flocal-guard--share-for-file file)))
                           "inner"))))
      (delete-directory root t))))

(ert-deftest flocal-guard-decodes-base64-roots ()
  (should (equal (flocal-guard--decode-root
                  '((encoding . "base64") (data . "L3RtcA==")))
                 "/tmp")))

(ert-deftest flocal-guard-status-validation-keeps-active-non-stopped-shares ()
  (let ((shares (flocal-guard--validate-report
                 '((schema . 1) (source . "live")
                   (observed_at . "2026-08-25T00:00:00Z")
                   (daemon . ((state . "live") (diagnostic . nil)))
                   (shares . (((share . "active")
                               (root . ((encoding . "base64") (data . "L3RtcA==")))
                               (enabled . t) (connection_state . "watching")
                               (scheduling . "idle") (role . "connector")
                               (initial_complete . t) (diagnostic . nil) (unsettled . 0))
                              ((share . "stopped")
                               (root . ((encoding . "base64") (data . "L3RtcC9zdG9wcGVk")))
                               (enabled . t) (connection_state . "stopped")
                               (scheduling . "idle") (role . "connector")
                               (initial_complete . t) (diagnostic . nil) (unsettled . 0))
                              ((share . "disabled")
                               (root . ((encoding . "base64") (data . "L3RtcC9kaXNhYmxlZA==")))
                               (enabled . :false) (connection_state . "stopped")
                               (scheduling . "idle") (role . "connector")
                               (initial_complete . :false) (diagnostic . nil) (unsettled . 0))))))))
    (should (= (length shares) 1))
    (should (equal (caar shares) "/tmp"))))

(ert-deftest flocal-guard-status-validation-accepts-json-null-and-false ()
  (should-not
   (flocal-guard--validate-report
    (json-parse-string
     "{\"schema\":1,\"source\":\"live\",\"observed_at\":\"2026-08-25T00:00:00Z\",\"daemon\":{\"state\":\"live\",\"diagnostic\":null},\"shares\":[{\"share\":\"disabled\",\"root\":{\"encoding\":\"base64\",\"data\":\"L3RtcA==\"},\"enabled\":false,\"connection_state\":\"stopped\",\"scheduling\":\"idle\",\"role\":\"connector\",\"initial_complete\":false,\"diagnostic\":null,\"unsettled\":0}]}"
     :object-type 'alist :array-type 'list))))

(ert-deftest flocal-guard-status-validation-rejects-an-invalid-disabled-share ()
  (should-error
   (flocal-guard--validate-report
    (json-parse-string
     "{\"schema\":1,\"source\":\"live\",\"observed_at\":\"2026-08-25T00:00:00Z\",\"daemon\":{\"state\":\"live\",\"diagnostic\":null},\"shares\":[{\"share\":\"bad\",\"root\":{\"encoding\":\"base64\",\"data\":\"L3RtcA==\"},\"connection_state\":\"stopped\",\"scheduling\":\"idle\",\"role\":\"connector\",\"initial_complete\":false,\"diagnostic\":null,\"unsettled\":0}]}"
     :object-type 'alist :array-type 'list))
   :type 'error))

(ert-deftest flocal-guard-status-validation-rejects-an-empty-root-and-bad-time ()
  (let ((report (json-parse-string
                 "{\"schema\":1,\"source\":\"live\",\"observed_at\":\"2026-08-25T00:00:00Z\",\"daemon\":{\"state\":\"live\",\"diagnostic\":null},\"shares\":[{\"share\":\"bad\",\"root\":{\"encoding\":\"base64\",\"data\":\"\"},\"enabled\":true,\"connection_state\":\"watching\",\"scheduling\":\"idle\",\"role\":\"connector\",\"initial_complete\":true,\"diagnostic\":null,\"unsettled\":0}]}"
                 :object-type 'alist :array-type 'list)))
    (should-error (flocal-guard--validate-report report) :type 'error)
    (setf (alist-get 'data (alist-get 'root (car (alist-get 'shares report))))
          "L3RtcA==")
    (setf (alist-get 'observed_at report) "2026-02-31T99:99:99Z")
    (should-error (flocal-guard--validate-report report) :type 'error)))

(ert-deftest flocal-guard-cold-cache-blocks-a-modified-save ()
  (with-temp-buffer
    (set-visited-file-name (make-temp-file "flocal-guard-"))
    (unwind-protect
        (progn
          (insert "mine")
          (let ((flocal-guard--cache-valid nil)
                (flocal-guard--cache-updated-at nil))
            (cl-letf (((symbol-function 'flocal-guard-refresh) #'ignore))
              (should-error (flocal-guard--save-buffer #'ignore nil)
                            :type 'user-error))))
      (delete-file buffer-file-name))))

(ert-deftest flocal-guard-refuses-a-custom-save-function ()
  (with-temp-buffer
    (set-visited-file-name (make-temp-file "flocal-guard-"))
    (unwind-protect
        (progn
          (insert "mine")
          (setq-local flocal-guard--share '("/tmp" . ((share . "test"))))
          (let ((flocal-guard--cache-valid t)
                (flocal-guard--cache-updated-at (float-time))
                (write-contents-functions (list #'ignore)))
            (should-error (flocal-guard--save-buffer #'ignore nil)
                          :type 'user-error)))
      (delete-file buffer-file-name))))

(ert-deftest flocal-guard-after-save-leaves-unrelated-buffers-alone ()
  (with-temp-buffer
    (setq-local flocal-guard--share nil)
    (cl-letf (((symbol-function 'flocal-guard--file-hash)
               (lambda (&rest _) (ert-fail "unrelated file was hashed"))))
      (flocal-guard--after-save))))

(ert-deftest flocal-guard-supersession-warning-needs-fresh-share-status ()
  (let ((flocal-guard--share '("/tmp" . ((share . "test"))))
        (flocal-guard--cache-valid t)
        (flocal-guard--cache-updated-at (- (float-time) 30))
        called)
    (flocal-guard--supersession-threat
     (lambda (&rest _) (setq called t)) nil)
    (should called)))

(ert-deftest flocal-guard-identical-disk-replacement-saves-without-ediff ()
  (let ((file (make-temp-file "flocal-guard-"))
        (flocal-guard--cache-valid t)
        (flocal-guard--cache-updated-at (float-time)))
    (unwind-protect
        (progn
          (with-temp-file file (insert "base\n"))
          (let ((buffer (find-file-noselect file))
                saved)
            (unwind-protect
                (with-current-buffer buffer
                  (setq-local flocal-guard--share '("/tmp" . ((share . "test"))))
                  (flocal-guard--capture-base)
                  (goto-char (point-max))
                  (insert "mine\n")
                  (with-temp-file file (insert "base\n"))
                  (cl-letf (((symbol-function 'ediff-buffers)
                             (lambda (&rest _) (ert-fail "unexpected Ediff"))))
                    (flocal-guard--save-buffer
                     (lambda (&rest _) (setq saved t)) nil))
                  (should saved))
              (kill-buffer buffer))))
      (delete-file file))))

(ert-deftest flocal-guard-first-save-of-a-new-protected-file-establishes-a-baseline ()
  (let ((file (make-temp-name (expand-file-name "flocal-guard-new-"
                                                 temporary-file-directory)))
        (flocal-guard--cache-valid t)
        (flocal-guard--cache-updated-at (float-time)))
    (unwind-protect
        (let ((buffer (find-file-noselect file)))
          (unwind-protect
              (with-current-buffer buffer
                (setq-local flocal-guard--share '("/tmp" . ((share . "test"))))
                (insert "first save\n")
                (cl-letf (((symbol-function 'ediff-buffers)
                           (lambda (&rest _) (ert-fail "new file should not open Ediff"))))
                  (flocal-guard--save-buffer #'basic-save-buffer nil))
                ;; `save-buffer' runs the global after-save hook in normal use;
                ;; this direct unit call uses its lower-level save primitive.
                (flocal-guard--after-save)
                (should flocal-guard--base-hash)
                (should-not (buffer-modified-p)))
            (kill-buffer buffer)))
      (when (file-exists-p file) (delete-file file)))))

(ert-deftest flocal-guard-save-opens-ediff-for-changed-disk ()
  (let ((file (make-temp-file "flocal-guard-"))
        (flocal-guard--cache-valid t)
        (flocal-guard--cache-updated-at (float-time)))
    (unwind-protect
        (progn
          (with-temp-file file (insert "base\n"))
          (let ((buffer (find-file-noselect file))
                disk)
            (unwind-protect
                (with-current-buffer buffer
                  (setq-local flocal-guard--share '("/tmp" . ((share . "test"))))
                  (flocal-guard--capture-base)
                  (goto-char (point-max))
                  (insert "mine\n")
                  (with-temp-file file (insert "disk\n"))
                  (with-current-buffer buffer
                    (should (buffer-modified-p))
                    (should flocal-guard--share)
                    (cl-letf (((symbol-function 'ediff-buffers)
                               (lambda (_a b) (setq disk b))))
                      (flocal-guard--save-buffer #'ignore nil)))
                  (should (buffer-live-p disk))
                  (with-current-buffer disk
                    (should (equal (buffer-string) "disk\n")))
                  (should (equal (with-temp-buffer
                                   (insert-file-contents file)
                                   (buffer-string))
                                 "disk\n")))
              (when (buffer-live-p disk) (kill-buffer disk))
              (kill-buffer buffer))))
      (delete-file file))))

(ert-deftest flocal-guard-visit-race-keeps-the-buffer-version-as-its-baseline ()
  (let ((file (make-temp-file "flocal-guard-"))
        (flocal-guard--cache-valid t)
        (flocal-guard--cache-updated-at (float-time)))
    (unwind-protect
        (progn
          (with-temp-file file (insert "seen\n"))
          (let ((buffer (find-file-noselect file)) disk)
            (unwind-protect
                (with-current-buffer buffer
                  (setq-local flocal-guard--share '("/tmp" . ((share . "test"))))
                  ;; Model flocal replacing the file between Emacs reading it
                  ;; and find-file-hook recording the baseline.
                  (with-temp-file file (insert "replacement\n"))
                  (flocal-guard--capture-base)
                  ;; Exercise the guard directly without Emacs's separately
                  ;; advised first-edit supersession question.
                  (set-visited-file-modtime)
                  (goto-char (point-max))
                  (insert "mine\n")
                  (cl-letf (((symbol-function 'ediff-buffers)
                             (lambda (_a b) (setq disk b))))
                    (flocal-guard--save-buffer #'ignore nil))
                  (should (buffer-live-p disk))
                  (with-current-buffer disk
                    (should (equal (buffer-string) "replacement\n"))))
              (when (buffer-live-p disk) (kill-buffer disk))
              (kill-buffer buffer))))
      (delete-file file))))

(ert-deftest flocal-guard-refresh-does-not-rebaseline-unsaved-edits ()
  (let ((file (make-temp-file "flocal-guard-"))
        (flocal-guard--cache-valid t)
        (flocal-guard--cache-updated-at (float-time))
        (flocal-guard--cache-source "live"))
    (unwind-protect
        (progn
          (with-temp-file file (insert "base\n"))
          (let ((buffer (find-file-noselect file)))
            (unwind-protect
                (with-current-buffer buffer
                  (setq-local flocal-guard--share '("/tmp" . ((share . "test"))))
                  (flocal-guard--capture-base)
                  (let ((baseline flocal-guard--base-hash)
                        (flocal-guard--shares (list (cons "/tmp" '((share . "test"))))))
                    (goto-char (point-max))
                    (insert "mine\n")
                    (flocal-guard--reclassify-buffers)
                    (should (equal flocal-guard--base-hash baseline))
                    (cl-letf (((symbol-function 'ediff-buffers)
                               (lambda (&rest _) (ert-fail "refresh caused a false conflict"))))
                      (flocal-guard--save-buffer #'ignore nil))))
              (kill-buffer buffer))))
      (delete-file file))))

(ert-deftest flocal-guard-ediff-write-a-saves-only-the-resolved-buffer ()
  (let ((file (make-temp-file "flocal-guard-"))
        (flocal-guard--cache-valid t)
        (flocal-guard--cache-updated-at (float-time)))
    (unwind-protect
        (progn
          (with-temp-file file (insert "base\n"))
          (let ((buffer (find-file-noselect file)))
            (unwind-protect
                (with-current-buffer buffer
                  (setq-local flocal-guard--share '("/tmp" . ((share . "test"))))
                  (flocal-guard--capture-base)
                  (goto-char (point-max))
                  (insert "merged\n")
                  (with-temp-file file (insert "disk\n"))
                  (setq-local flocal-guard--pending-disk-hash
                              (flocal-guard--disk-hash))
                  ;; The advice normally suppresses Emacs's mtime-only
                  ;; question for guarded buffers; pin the same precondition
                  ;; while exercising the save branch directly.
                  (set-visited-file-modtime)
                  (let ((flocal-guard--ediff-writing t))
                    (flocal-guard--save-buffer #'basic-save-buffer nil))
                  (should-not flocal-guard--pending-disk-hash)
                  (should-not (buffer-modified-p))
                  (should (equal (with-temp-buffer
                                   (insert-file-contents file)
                                   (buffer-string))
                                 "base\nmerged\n")))
              (kill-buffer buffer))))
      (delete-file file))))

(ert-deftest flocal-guard-aborted-conflict-reopens-ediff-on-save ()
  (let ((file (make-temp-file "flocal-guard-"))
        (flocal-guard--cache-valid t)
        (flocal-guard--cache-updated-at (float-time)))
    (unwind-protect
        (progn
          (with-temp-file file (insert "base\n"))
          (let ((buffer (find-file-noselect file))
                (ediff-count 0)
                disk)
            (unwind-protect
                (with-current-buffer buffer
                  (setq-local flocal-guard--share '("/tmp" . ((share . "test"))))
                  (flocal-guard--capture-base)
                  (goto-char (point-max))
                  (insert "mine\n")
                  (with-temp-file file (insert "disk\n"))
                  (cl-letf (((symbol-function 'ediff-buffers)
                             (lambda (_a b)
                               (setq ediff-count (1+ ediff-count) disk b))))
                    (flocal-guard--save-buffer #'ignore nil)
                    (flocal-guard--save-buffer #'ignore nil))
                  (should (= ediff-count 2))
                  (should flocal-guard--pending-disk-hash)
                  (should (buffer-modified-p)))
              (when (buffer-live-p disk) (kill-buffer disk))
              (kill-buffer buffer))))
      (delete-file file))))

;;; flocal-guard-test.el ends here
