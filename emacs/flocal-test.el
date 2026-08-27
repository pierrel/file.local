;;; flocal-test.el --- Tests for flocal -*- lexical-binding: t; -*-

(require 'ert)
(require 'cl-lib)
(require 'flocal)

(defun flocal-test--wire-root (path &optional device inode)
  `((encoding . "base64")
    (data . ,(base64-encode-string
              (string-make-unibyte
               (encode-coding-string path file-name-coding-system)) t))
    (device . ,(or device "1"))
    (inode . ,(or inode "2"))))

(defun flocal-test--share (root name)
  (let* ((attributes (file-attributes root))
         (details `((share . ,name)
                    (root . ,(flocal-test--wire-root
                               root
                               (number-to-string
                                (file-attribute-device-number attributes))
                               (number-to-string
                                (file-attribute-inode-number attributes))))
                    (enabled . t) (connection_state . "watching")
                    (scheduling . "idle") (role . "connector")
                    (initial_complete . t) (diagnostic . nil) (unsettled . 0))))
    (flocal--share-create :root root :canonical-root (file-name-as-directory (file-truename root))
                          :details details)))

(defun flocal-test--report-json (source share)
  (json-encode
   `((schema . 1) (source . ,source) (observed_at . "2026-08-25T00:00:00Z")
     (daemon . ((state . ,(if (equal source "live") "live" "unavailable"))
                (diagnostic . nil)))
     (shares . (,(flocal--share-details share))))))

(ert-deftest flocal-most-specific-share-wins ()
  (let* ((root (make-temp-file "flocal-" t))
         (nested (expand-file-name "nested" root))
         (file (expand-file-name "note.txt" nested)))
    (unwind-protect
        (progn
          (make-directory nested)
          (write-region "" nil file nil 'silent)
          (let ((flocal--shares (list (flocal-test--share root "outer")
                                      (flocal-test--share nested "inner"))))
            (should (equal (alist-get 'share
                                      (flocal--share-details (flocal--share-for-file file)))
                           "inner"))))
      (delete-directory root t))))

(ert-deftest flocal-decodes-base64-roots ()
  (should (equal (flocal--decode-root
                  '((encoding . "base64") (data . "L3RtcA==")))
                 "/tmp")))

(ert-deftest flocal-hides-the-modeline-marker-in-non-file-buffers ()
  (with-temp-buffer
    (setq-local flocal--state 'checking)
    (should (equal (flocal--mode-line) ""))))

(ert-deftest flocal-leaves-remote-buffers-outside ()
  (with-temp-buffer
    (setq buffer-file-name "/ssh:test@host:/tmp/note")
    (flocal--classify-buffer)
    (should (eq flocal--state 'outside))
    (should-not flocal--share)))

(ert-deftest flocal-buffer-hash-accepts-zero-and-rejects-nul ()
  (with-temp-buffer
    (insert "version 0\n")
    (should (stringp (flocal--buffer-hash)))
    (erase-buffer)
    (insert "x\0")
    (should-error (flocal--buffer-hash) :type 'user-error)))

(ert-deftest flocal-status-validation-keeps-active-non-stopped-shares ()
  (let* ((report (flocal--validate-report
                 '((schema . 1) (source . "live")
                   (observed_at . "2026-08-25T00:00:00Z")
                   (daemon . ((state . "live") (diagnostic . nil)))
                   (shares . (((share . "active")
                               (root . ((encoding . "base64") (data . "L3RtcC8w")
                                        (device . "1") (inode . "2")))
                               (enabled . t) (connection_state . "watching")
                               (scheduling . "idle") (role . "connector")
                               (initial_complete . t) (diagnostic . nil) (unsettled . 0))
                              ((share . "stopped")
                               (root . ((encoding . "base64") (data . "L3RtcC9zdG9wcGVk")
                                        (device . "1") (inode . "2")))
                               (enabled . t) (connection_state . "stopped")
                               (scheduling . "idle") (role . "connector")
                               (initial_complete . t) (diagnostic . nil) (unsettled . 0))
                              ((share . "disabled")
                               (root . ((encoding . "base64") (data . "L3RtcC9kaXNhYmxlZA==")
                                        (device . "1") (inode . "2")))
                               (enabled . :false) (connection_state . "stopped")
                               (scheduling . "idle") (role . "connector")
                               (initial_complete . :false) (diagnostic . nil) (unsettled . 0)))))))
         (shares (flocal--report-shares report)))
    (should (= (length shares) 3))
    (should (equal (flocal--share-root (car shares)) "/tmp/0"))))

(ert-deftest flocal-status-validation-accepts-json-null-and-false ()
  (let ((report
         (flocal--validate-report
          (json-parse-string
           "{\"schema\":1,\"source\":\"live\",\"observed_at\":\"2026-08-25T00:00:00Z\",\"daemon\":{\"state\":\"live\",\"diagnostic\":null},\"shares\":[{\"share\":\"disabled\",\"root\":{\"encoding\":\"base64\",\"data\":\"L3RtcA==\",\"device\":\"1\",\"inode\":\"2\"},\"enabled\":false,\"connection_state\":\"stopped\",\"scheduling\":\"idle\",\"role\":\"connector\",\"initial_complete\":false,\"diagnostic\":null,\"unsettled\":0}]}"
           :object-type 'alist :array-type 'list))))
    (should (= (length (flocal--report-shares report)) 1))))

(ert-deftest flocal-status-validation-rejects-an-invalid-disabled-share ()
  (should-error
   (flocal--validate-report
    (json-parse-string
     "{\"schema\":1,\"source\":\"live\",\"observed_at\":\"2026-08-25T00:00:00Z\",\"daemon\":{\"state\":\"live\",\"diagnostic\":null},\"shares\":[{\"share\":\"bad\",\"root\":{\"encoding\":\"base64\",\"data\":\"L3RtcA==\"},\"connection_state\":\"stopped\",\"scheduling\":\"idle\",\"role\":\"connector\",\"initial_complete\":false,\"diagnostic\":null,\"unsettled\":0}]}"
     :object-type 'alist :array-type 'list))
   :type 'error))

(ert-deftest flocal-status-validation-rejects-an-empty-root-and-bad-time ()
  (let ((report (json-parse-string
                 "{\"schema\":1,\"source\":\"live\",\"observed_at\":\"2026-08-25T00:00:00Z\",\"daemon\":{\"state\":\"live\",\"diagnostic\":null},\"shares\":[{\"share\":\"bad\",\"root\":{\"encoding\":\"base64\",\"data\":\"\"},\"enabled\":true,\"connection_state\":\"watching\",\"scheduling\":\"idle\",\"role\":\"connector\",\"initial_complete\":true,\"diagnostic\":null,\"unsettled\":0}]}"
                 :object-type 'alist :array-type 'list)))
    (should-error (flocal--validate-report report) :type 'error)
    (setf (alist-get 'data (alist-get 'root (car (alist-get 'shares report))))
          "L3RtcA==")
    (setf (alist-get 'observed_at report) "2026-02-31T99:99:99Z")
    (should-error (flocal--validate-report report) :type 'error)
    (setf (alist-get 'observed_at report) "2026-08-25T00:00:00Z"
          (alist-get 'data (alist-get 'root (car (alist-get 'shares report))))
          "L3RtcC8AeA==")
    (should-error (flocal--validate-report report) :type 'error)))

(ert-deftest flocal-status-shows-disabled-shares-with-escaped-fields ()
  (let* ((root (make-temp-file "flocal-" t))
         (share (flocal-test--share root "disabled\nshare")))
    (unwind-protect
        (progn
          (setf (alist-get 'enabled (flocal--share-details share)) :false
                (alist-get 'connection_state (flocal--share-details share)) "stopped"
                (alist-get 'diagnostic (flocal--share-details share)) "bad\tnews")
          (let ((flocal--report (flocal--report-create
                                 :source "stored" :observed-at "2026-08-25T00:00:00Z"
                                 :daemon '((state . "unavailable") (diagnostic . nil))
                                 :shares (list share)))
                (flocal--cache-valid t)
                (flocal--cache-updated-at (float-time)))
            (should-not (flocal--eligible-shares (flocal--report-shares flocal--report)))
            (flocal--render-status)
            (with-current-buffer "*flocal status*"
              (should (equal major-mode 'flocal-status-mode))
              (should (equal (lookup-key flocal-status-mode-map (kbd "g")) #'flocal-refresh))
              (should (string-match-p (regexp-quote "Share: \"disabled\\nshare\"")
                                      (buffer-string)))
              (should (string-match-p (regexp-quote "Diagnostic: \"bad\\11news\"")
                                      (buffer-string))))))
      (when (get-buffer "*flocal status*") (kill-buffer "*flocal status*"))
      (delete-directory root t))))

(ert-deftest flocal-status-rejects-an-overlong-diagnostic ()
  (let ((report (json-parse-string
                 (format "{\"schema\":1,\"source\":\"live\",\"observed_at\":\"2026-08-25T00:00:00Z\",\"daemon\":{\"state\":\"live\",\"diagnostic\":%S},\"shares\":[]}"
                         (make-string (1+ flocal--max-diagnostic-bytes) ?x))
                 :object-type 'alist :array-type 'list)))
    (should-error (flocal--validate-report report) :type 'error)))

(ert-deftest flocal-refresh-marks-and-redraws-an-open-status-buffer ()
  (let* ((root (make-temp-file "flocal-" t))
         (share (flocal-test--share root "share"))
         (output (generate-new-buffer " *flocal-test-status*"))
         (process (make-pipe-process :name "flocal test status" :buffer output :noquery t)))
    (unwind-protect
        (let ((flocal--report (flocal--report-create
                               :source "stored" :observed-at "old"
                               :daemon '((state . "unavailable") (diagnostic . nil))
                               :shares (list share)))
              (flocal--cache-valid t)
              (flocal--cache-updated-at (float-time))
              (flocal--refresh-process nil)
              (flocal--refresh-timer nil))
          (flocal--render-status)
          (with-current-buffer "*flocal status*"
            (should (string-match-p "Fresh: yes" (buffer-string))))
          (setq flocal--cache-updated-at
                (- (float-time) flocal-refresh-interval 1))
          (cl-letf (((symbol-function 'flocal--status-executable) (lambda () "/bin/true"))
                    ((symbol-function 'make-process) (lambda (&rest _) process)))
            (flocal-refresh))
          (with-current-buffer "*flocal status*"
            (should (string-match-p "Fresh: no" (buffer-string))))
          (with-current-buffer output
            (insert (flocal-test--report-json "live" share)))
          (cl-letf (((symbol-function 'process-status) (lambda (_) 'exit))
                    ((symbol-function 'process-exit-status) (lambda (_) 0)))
            (flocal--refresh-finished process "finished"))
          (with-current-buffer "*flocal status*"
            (should (string-match-p "Source: live" (buffer-string)))
            (should (string-match-p "Fresh: yes" (buffer-string)))))
      (when (timerp flocal--refresh-timer) (cancel-timer flocal--refresh-timer))
      (when (process-live-p process) (delete-process process))
      (when (buffer-live-p output) (kill-buffer output))
      (when (get-buffer "*flocal status*") (kill-buffer "*flocal status*"))
      (delete-directory root t))))

(ert-deftest flocal-rejects-a-symlinked-executable ()
  (let ((target (make-temp-file "flocal-executable-"))
        (link (make-temp-name (expand-file-name "flocal-link-" temporary-file-directory))))
    (unwind-protect
        (progn
          (set-file-modes target #o700)
          (make-symbolic-link target link)
          (let ((flocal-executable link))
            (should-error (flocal--status-executable) :type 'user-error)))
      (when (file-exists-p link) (delete-file link))
      (delete-file target))))

(ert-deftest flocal-refuses-replaced-share-roots ()
  (let* ((parent (make-temp-file "flocal-" t))
         (root (expand-file-name "root" parent))
         (moved (expand-file-name "moved" parent))
         (file (expand-file-name "note" root)))
    (unwind-protect
        (progn
          (make-directory root)
          (write-region "" nil file nil 'silent)
          (let* ((share (flocal-test--share root "test"))
                 (flocal--shares (list share))
                 (flocal--report (flocal--report-create :shares (list share))))
            (rename-file root moved)
            (make-directory root)
            (write-region "" nil file nil 'silent)
            (should-error (flocal--share-for-file file) :type 'error)))
      (delete-directory parent t))))

(ert-deftest flocal-refuses-symlinked-share-roots ()
  (let* ((parent (make-temp-file "flocal-" t))
         (root (expand-file-name "root" parent))
         (target (expand-file-name "target" parent))
         (file (expand-file-name "note" root)))
    (unwind-protect
        (progn
          (make-directory root)
          (let* ((share (flocal-test--share root "test"))
                 (flocal--shares (list share))
                 (flocal--report (flocal--report-create :shares (list share))))
            (rename-file root target)
            (make-symbolic-link target root)
            (should-error (flocal--share-for-file file) :type 'error)))
      (delete-directory parent t))))

(ert-deftest flocal-decodes-native-root-bytes ()
  (skip-unless (eq system-type 'gnu/linux))
  (let* ((parent (make-temp-file "flocal-" t))
         (root (concat (string-make-unibyte (file-name-as-directory parent))
                       (string-make-unibyte (string 255))))
         (attributes nil))
    (unwind-protect
        (progn
          (make-directory root)
          (setq attributes (file-attributes root))
          (let ((wire (flocal-test--wire-root
                       root
                       (number-to-string (file-attribute-device-number attributes))
                       (number-to-string (file-attribute-inode-number attributes)))))
            (should (equal (flocal--share-root
                            (flocal--validate-share
                             `((share . "native") (root . ,wire) (enabled . t)
                               (connection_state . "watching") (scheduling . "idle")
                               (role . "connector") (initial_complete . t)
                               (diagnostic . nil) (unsettled . 0))))
                           root))))
      (delete-directory parent t))))

(ert-deftest flocal-cold-cache-blocks-a-modified-save ()
  (with-temp-buffer
    (set-visited-file-name (make-temp-file "flocal-"))
    (unwind-protect
        (progn
          (insert "mine")
          (let ((flocal--cache-valid nil)
                (flocal--cache-updated-at nil))
            (cl-letf (((symbol-function 'flocal-refresh) #'ignore))
              (should-error (flocal--save-buffer #'ignore nil)
                            :type 'user-error))))
      (delete-file buffer-file-name))))

(ert-deftest flocal-refuses-a-custom-save-function ()
  (with-temp-buffer
    (set-visited-file-name (make-temp-file "flocal-"))
    (unwind-protect
        (progn
          (insert "mine")
          (setq-local flocal--share '("/tmp" . ((share . "test"))))
          (let ((flocal--cache-valid t)
                (flocal--cache-updated-at (float-time))
                (write-contents-functions (list #'ignore)))
            (should-error (flocal--save-buffer #'ignore nil)
                          :type 'user-error)))
      (delete-file buffer-file-name))))

(ert-deftest flocal-after-save-leaves-unrelated-buffers-alone ()
  (with-temp-buffer
    (setq-local flocal--share nil)
    (cl-letf (((symbol-function 'flocal--file-hash)
               (lambda (&rest _) (ert-fail "unrelated file was hashed"))))
      (flocal--after-save))))

(ert-deftest flocal-supersession-warning-needs-fresh-share-status ()
  (let ((flocal--share '("/tmp" . ((share . "test"))))
        (flocal--cache-valid t)
        (flocal--cache-updated-at (- (float-time) 30))
        called)
    (flocal--supersession-threat
     (lambda (&rest _) (setq called t)) nil)
    (should called)))

(ert-deftest flocal-identical-disk-replacement-saves-without-ediff ()
  (let ((file (make-temp-file "flocal-"))
        (flocal--cache-valid t)
        (flocal--cache-updated-at (float-time)))
    (unwind-protect
        (progn
          (with-temp-file file (insert "base\n"))
          (let ((buffer (find-file-noselect file))
                saved)
            (unwind-protect
                (with-current-buffer buffer
                  (setq-local flocal--share '("/tmp" . ((share . "test"))))
                  (flocal--capture-base)
                  (goto-char (point-max))
                  (insert "mine\n")
                  (with-temp-file file (insert "base\n"))
                  (cl-letf (((symbol-function 'ediff-buffers)
                             (lambda (&rest _) (ert-fail "unexpected Ediff"))))
                    (flocal--save-buffer
                     (lambda (&rest _) (setq saved t)) nil))
                  (should saved))
              (kill-buffer buffer))))
      (delete-file file))))

(ert-deftest flocal-first-save-of-a-new-protected-file-establishes-a-baseline ()
  (let ((file (make-temp-name (expand-file-name "flocal-new-"
                                                 temporary-file-directory)))
        (flocal--cache-valid t)
        (flocal--cache-updated-at (float-time)))
    (unwind-protect
        (let ((buffer (find-file-noselect file)))
          (unwind-protect
              (with-current-buffer buffer
                (setq-local flocal--share '("/tmp" . ((share . "test"))))
                (insert "first save\n")
                (cl-letf (((symbol-function 'ediff-buffers)
                           (lambda (&rest _) (ert-fail "new file should not open Ediff"))))
                  (flocal--save-buffer #'basic-save-buffer nil))
                ;; `save-buffer' runs the global after-save hook in normal use;
                ;; this direct unit call uses its lower-level save primitive.
                (flocal--after-save)
                (should flocal--base-hash)
                (should-not (buffer-modified-p)))
            (kill-buffer buffer)))
      (when (file-exists-p file) (delete-file file)))))

(ert-deftest flocal-save-opens-ediff-for-changed-disk ()
  (let ((file (make-temp-file "flocal-"))
        (flocal--cache-valid t)
        (flocal--cache-updated-at (float-time)))
    (unwind-protect
        (progn
          (with-temp-file file (insert "base\n"))
          (let ((buffer (find-file-noselect file))
                disk)
            (unwind-protect
                (with-current-buffer buffer
                  (setq-local flocal--share '("/tmp" . ((share . "test"))))
                  (flocal--capture-base)
                  (goto-char (point-max))
                  (insert "mine\n")
                  (with-temp-file file (insert "disk\n"))
                  (with-current-buffer buffer
                    (should (buffer-modified-p))
                    (should flocal--share)
                    (cl-letf (((symbol-function 'ediff-buffers)
                               (lambda (_a b) (setq disk b))))
                      (flocal--save-buffer #'ignore nil)))
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

(ert-deftest flocal-visit-race-keeps-the-buffer-version-as-its-baseline ()
  (let ((file (make-temp-file "flocal-"))
        (flocal--cache-valid t)
        (flocal--cache-updated-at (float-time)))
    (unwind-protect
        (progn
          (with-temp-file file (insert "seen\n"))
          (let ((buffer (find-file-noselect file)) disk)
            (unwind-protect
                (with-current-buffer buffer
                  (setq-local flocal--share '("/tmp" . ((share . "test"))))
                  ;; Model flocal replacing the file between Emacs reading it
                  ;; and find-file-hook recording the baseline.
                  (with-temp-file file (insert "replacement\n"))
                  (flocal--capture-base)
                  ;; Exercise the guard directly without Emacs's separately
                  ;; advised first-edit supersession question.
                  (set-visited-file-modtime)
                  (goto-char (point-max))
                  (insert "mine\n")
                  (cl-letf (((symbol-function 'ediff-buffers)
                             (lambda (_a b) (setq disk b))))
                    (flocal--save-buffer #'ignore nil))
                  (should (buffer-live-p disk))
                  (with-current-buffer disk
                    (should (equal (buffer-string) "replacement\n"))))
              (when (buffer-live-p disk) (kill-buffer disk))
              (kill-buffer buffer))))
      (delete-file file))))

(ert-deftest flocal-refresh-does-not-rebaseline-unsaved-edits ()
  (let ((file (make-temp-file "flocal-"))
        (flocal--cache-valid t)
        (flocal--cache-updated-at (float-time)))
    (unwind-protect
        (progn
          (with-temp-file file (insert "base\n"))
          (let ((buffer (find-file-noselect file)))
            (unwind-protect
                (with-current-buffer buffer
                  (setq-local flocal--share '("/tmp" . ((share . "test"))))
                  (flocal--capture-base)
                  (let ((baseline flocal--base-hash)
                        (flocal--report (flocal--report-create :source "live"))
                        (flocal--shares (list (flocal-test--share temporary-file-directory "test"))))
                    (goto-char (point-max))
                    (insert "mine\n")
                    (flocal--reclassify-buffers)
                    (should (equal flocal--base-hash baseline))
                    (cl-letf (((symbol-function 'ediff-buffers)
                               (lambda (&rest _) (ert-fail "refresh caused a false conflict"))))
                      (flocal--save-buffer #'ignore nil))))
              (kill-buffer buffer))))
      (delete-file file))))

(ert-deftest flocal-ediff-write-a-saves-only-the-resolved-buffer ()
  (let ((file (make-temp-file "flocal-"))
        (flocal--cache-valid t)
        (flocal--cache-updated-at (float-time)))
    (unwind-protect
        (progn
          (with-temp-file file (insert "base\n"))
          (let ((buffer (find-file-noselect file)))
            (unwind-protect
                (with-current-buffer buffer
                  (setq-local flocal--share '("/tmp" . ((share . "test"))))
                  (flocal--capture-base)
                  (goto-char (point-max))
                  (insert "merged\n")
                  (with-temp-file file (insert "disk\n"))
                  (setq-local flocal--pending-disk-hash
                              (flocal--disk-hash))
                  ;; The advice normally suppresses Emacs's mtime-only
                  ;; question for guarded buffers; pin the same precondition
                  ;; while exercising the save branch directly.
                  (set-visited-file-modtime)
                  (let ((flocal--ediff-writing t))
                    (flocal--save-buffer #'basic-save-buffer nil))
                  (should-not flocal--pending-disk-hash)
                  (should-not (buffer-modified-p))
                  (should (equal (with-temp-buffer
                                   (insert-file-contents file)
                                   (buffer-string))
                                 "base\nmerged\n")))
              (kill-buffer buffer))))
      (delete-file file))))

(ert-deftest flocal-aborted-conflict-reopens-ediff-on-save ()
  (let ((file (make-temp-file "flocal-"))
        (flocal--cache-valid t)
        (flocal--cache-updated-at (float-time)))
    (unwind-protect
        (progn
          (with-temp-file file (insert "base\n"))
          (let ((buffer (find-file-noselect file))
                (ediff-count 0)
                disk)
            (unwind-protect
                (with-current-buffer buffer
                  (setq-local flocal--share '("/tmp" . ((share . "test"))))
                  (flocal--capture-base)
                  (goto-char (point-max))
                  (insert "mine\n")
                  (with-temp-file file (insert "disk\n"))
                  (cl-letf (((symbol-function 'ediff-buffers)
                             (lambda (_a b)
                               (setq ediff-count (1+ ediff-count) disk b))))
                    (flocal--save-buffer #'ignore nil)
                    (flocal--save-buffer #'ignore nil))
                  (should (= ediff-count 2))
                  (should flocal--pending-disk-hash)
                  (should (buffer-modified-p)))
              (when (buffer-live-p disk) (kill-buffer disk))
              (kill-buffer buffer))))
      (delete-file file))))

;;; flocal-test.el ends here
