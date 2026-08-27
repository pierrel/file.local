;;; flocal.el --- Status and save protection for flocal shares -*- lexical-binding: t; -*-

;; Copyright (C) 2026 Pierre Bombay

;; Author: Pierre Bombay
;; Version: 0.1.0
;; Package-Requires: ((emacs "28.1"))
;; Keywords: files, tools

;;; Commentary:

;; `flocal-mode' presents read-only local flocal share status and marks eligible
;; visiting buffers.  It also handles save-time conflicts, but leaves flocal
;; lifecycle controls to the command-line client.

;;; Code:

(require 'json)
(require 'iso8601)
(require 'seq)
(require 'ediff)
(require 'cl-lib)

(defgroup flocal nil
  "Emacs affordances for files synchronized by flocal."
  :group 'files)

(defcustom flocal-executable (executable-find "flocal")
  "Absolute path to the flocal executable used for read-only status discovery."
  :type '(choice (const :tag "Not found" nil) file)
  :group 'flocal)

(defcustom flocal-max-conflict-bytes (* 1024 1024)
  "Largest file the guard will snapshot or present in Ediff."
  :type 'integer
  :group 'flocal)

(defcustom flocal-status-timeout 3
  "Seconds an asynchronous status request may run before it is killed."
  :type 'number
  :group 'flocal)

(defcustom flocal-status-max-bytes (* 64 1024)
  "Largest status response accepted from the flocal executable."
  :type 'integer
  :group 'flocal)

(defcustom flocal-refresh-interval 15
  "Seconds for which a successful status response remains fresh."
  :type 'number
  :group 'flocal)

(defconst flocal--max-diagnostic-bytes 4096)

(cl-defstruct (flocal--share (:constructor flocal--share-create))
  root details canonical-root)

(cl-defstruct (flocal--report (:constructor flocal--report-create))
  source observed-at daemon shares)

(defvar flocal--report nil)
(defvar flocal--shares nil)
(defvar flocal--refresh-process nil)
(defvar flocal--refresh-timer nil)
(defvar flocal--refresh-idle-timer nil)
(defvar flocal--cache-updated-at nil)
(defvar flocal--cache-valid nil)
(defvar flocal--refresh-error nil)
(defvar flocal--ediff-writing nil)
(defvar-local flocal--share nil)
(defvar-local flocal--state 'checking)
(defvar-local flocal--base-hash nil)
(defvar-local flocal--pending-disk-hash nil)
(defvar-local flocal--saving nil)
(defvar-local flocal--private-disk-buffer nil)

(defun flocal--mode-line ()
  (if buffer-file-name
      (let ((share (and flocal--share (flocal--share-details flocal--share))))
        (pcase flocal--state
          ((or 'guarded 'stored)
           (format " FLOCAL:%s/%s/%s" flocal--state
                   (or (alist-get 'connection_state share) "unknown")
                   (or (alist-get 'scheduling share) "unknown")))
          ('conflict " FLOCAL:conflict")
          ('checking " FLOCAL:checking")
          ('cannot-verify " FLOCAL:cannot-verify")
          (_ "")))
    ""))

(add-to-list 'minor-mode-alist
             '(flocal-mode (:eval (flocal--mode-line))))

(defun flocal--decode-root (root)
  (unless (and (equal (alist-get 'encoding root) "base64")
               (stringp (alist-get 'data root)))
    (error "flocal returned an unsupported root encoding"))
  (decode-coding-string (base64-decode-string (alist-get 'data root))
                        file-name-coding-system))

(defun flocal--identity-number (value)
  (unless (and (stringp value) (string-match-p "\\`[0-9]+\\'" value))
    (error "flocal returned an invalid root identity"))
  (let ((number (string-to-number value)))
    (unless (equal value (number-to-string number))
      (error "flocal returned an invalid root identity"))
    number))

(defun flocal--root-identity-matches-p (root details)
  "Return non-nil when ROOT still has DETAILS' registered identity."
  (let ((root-details (alist-get 'root details)))
    (and (file-directory-p root)
         (not (file-symlink-p root))
         (let ((attributes (file-attributes root)))
           (and attributes
                (= (file-attribute-device-number attributes)
                   (flocal--identity-number (alist-get 'device root-details)))
                (= (file-attribute-inode-number attributes)
                   (flocal--identity-number (alist-get 'inode root-details))))))))

(defun flocal--display (value)
  "Return VALUE as one escaped status field."
  (let ((print-escape-newlines t)
        (print-escape-control-characters t))
    (prin1-to-string value)))

(defun flocal--diagnostic (value)
  (and (stringp value) value))

(defun flocal--rfc3339-utc-p (value)
  (and (stringp value)
       (string-match-p
        "\\`[0-9]\\{4\\}-[0-9]\\{2\\}-[0-9]\\{2\\}T[0-9]\\{2\\}:[0-9]\\{2\\}:[0-9]\\{2\\}Z\\'"
        value)
       (condition-case nil
           (equal (format-time-string "%FT%TZ" (encode-time (iso8601-parse value)) t) value)
         (error nil))))

(defun flocal--under-root-p (file root)
  (let ((file (file-truename file))
        (root (file-name-as-directory (file-truename root))))
    (string-prefix-p root file)))

(defun flocal--under-configured-root-p (file root)
  (string-prefix-p (file-name-as-directory (expand-file-name root))
                   (expand-file-name file)))

(defun flocal--share-for-file (file)
  (when flocal--report
    (dolist (share (flocal--report-shares flocal--report))
      (when (and (eq (alist-get 'enabled (flocal--share-details share)) t)
                 (not (equal (alist-get 'connection_state
                                            (flocal--share-details share))
                             "stopped"))
                 (flocal--under-configured-root-p file (flocal--share-root share))
                 (not (flocal--root-identity-matches-p
                       (flocal--share-root share) (flocal--share-details share))))
        (error "flocal share root identity changed"))))
  (let ((matching (seq-filter
                   (lambda (share)
                     (flocal--under-configured-root-p
                      file (flocal--share-root share)))
                   flocal--shares)))
    (dolist (share matching)
      (unless (flocal--root-identity-matches-p
               (flocal--share-root share) (flocal--share-details share))
        (error "flocal share root identity changed")))
    (car (sort (seq-filter
                (lambda (share)
                  (flocal--under-root-p file (flocal--share-canonical-root share)))
                matching)
               (lambda (left right)
                 (> (length (flocal--share-canonical-root left))
                    (length (flocal--share-canonical-root right))))))))

(defun flocal--classify-buffer ()
  (when buffer-file-name
    ;; Capture the version Emacs showed at visit (or when the mode is enabled
    ;; for an already visiting, unmodified buffer).  Reclassification after a
    ;; status refresh must never turn unsaved edits into a disk baseline.
    (when (and (not flocal--base-hash) (not (buffer-modified-p)))
      (condition-case _error
          (flocal--capture-base)
        (error (setq flocal--state 'cannot-verify))))
    (if (not (flocal--cache-fresh-p))
        (setq flocal--share nil
              flocal--state 'checking)
      (condition-case _error
          (setq flocal--share (flocal--share-for-file buffer-file-name)
                flocal--state
                (cond ((not flocal--share) 'outside)
                      ((equal (flocal--report-source flocal--report) "stored") 'stored)
                      (t 'guarded)))
        (error
         (setq flocal--share nil
               flocal--state 'cannot-verify))))))

(defun flocal--visit-file ()
  (flocal--classify-buffer)
  (unless (flocal--cache-fresh-p)
    (condition-case error
        (flocal-refresh)
      (error
       (setq flocal--cache-valid nil
             flocal--refresh-error (error-message-string error))))))

(defun flocal--idle-refresh ()
  (condition-case error
      (flocal-refresh)
    (error
     (setq flocal--cache-valid nil
           flocal--refresh-error (error-message-string error))
     (flocal--reclassify-buffers)
     (flocal--redraw-status))))

(defun flocal--cache-fresh-p ()
  (and flocal--cache-valid flocal--cache-updated-at
       (< (- (float-time) flocal--cache-updated-at)
          flocal-refresh-interval)))

(defun flocal--capture-base ()
  "Remember the bytes in this buffer, not a later pathname replacement."
  (when (and buffer-file-name (file-regular-p buffer-file-name)
             (not (file-symlink-p buffer-file-name)))
    (setq flocal--base-hash (flocal--buffer-hash))))

(defun flocal--after-save ()
  (when flocal--share
    (condition-case _error
        (flocal--capture-base)
      (error (setq flocal--state 'cannot-verify)))))

(defun flocal--file-hash (file)
  "Return SHA-256 of FILE's literal bytes, without mtime-based caching."
  (when (> (file-attribute-size (file-attributes file))
           flocal-max-conflict-bytes)
    (user-error "flocal guard refuses to snapshot a file larger than %d bytes"
                flocal-max-conflict-bytes))
  (with-temp-buffer
    (flocal--insert-limited-disk-file file)
    (secure-hash 'sha256 (current-buffer))))

(defun flocal--insert-limited-disk-file (file)
  "Insert at most the configured disk snapshot limit plus one byte from FILE."
  ;; Request one byte over the limit so a replacement after the attribute check
  ;; cannot make Emacs accumulate an unbounded disk snapshot.
  (set-buffer-multibyte nil)
  (insert-file-contents-literally file nil 0 (1+ flocal-max-conflict-bytes))
  (when (> (buffer-size) flocal-max-conflict-bytes)
    (user-error "flocal guard refuses to snapshot a file larger than %d bytes"
                flocal-max-conflict-bytes)))

(defun flocal--buffer-hash ()
  "Return SHA-256 of the visiting buffer's normal file coding output."
  (let ((bytes (encode-coding-string
                (buffer-substring-no-properties (point-min) (point-max))
                buffer-file-coding-system)))
    (when (string-match-p "\0" bytes)
      (user-error "flocal guard cannot merge binary buffer contents"))
    (when (> (string-bytes bytes) flocal-max-conflict-bytes)
      (user-error "flocal guard refuses to protect a buffer larger than %d bytes"
                  flocal-max-conflict-bytes))
    (secure-hash 'sha256 bytes)))

(defun flocal--disk-hash ()
  (unless (and buffer-file-name (file-regular-p buffer-file-name)
               (not (file-symlink-p buffer-file-name)))
    (user-error "flocal guard cannot verify the file on disk"))
  (flocal--file-hash buffer-file-name))

(defun flocal--disk-buffer (expected-hash)
  "Return a private literal snapshot whose digest is EXPECTED-HASH.

The second digest check makes a replacement while the snapshot is being read
fail closed instead of presenting one disk version and authorizing another."
  (let* ((file buffer-file-name)
         (buffer (generate-new-buffer
                  (format "DISK: %s" (file-name-nondirectory file)))))
    (condition-case error
        (with-current-buffer buffer
          (unless (and (file-regular-p file) (not (file-symlink-p file)))
            (user-error "flocal guard cannot snapshot a non-regular disk file"))
          (flocal--insert-limited-disk-file file)
          (goto-char (point-min))
          (when (search-forward "\0" nil t)
            (user-error "flocal guard cannot merge a binary disk file"))
          (unless (equal (secure-hash 'sha256 (current-buffer)) expected-hash)
            (user-error "flocal guard observed the disk file change while reading it"))
          (setq buffer-read-only t)
          (setq-local flocal--private-disk-buffer t))
      (error (kill-buffer buffer) (signal (car error) (cdr error))))
    buffer))

(defun flocal--start-ediff (hash)
  (let ((disk (flocal--disk-buffer hash)))
    (setq flocal--pending-disk-hash hash
          flocal--state 'conflict)
    (ediff-buffers (current-buffer) disk)))

(defun flocal--save-buffer (original &optional arg)
  (cond
   ((or flocal--saving (not (buffer-modified-p)))
    (funcall original arg))
   ((not (flocal--cache-fresh-p))
    (flocal-refresh)
    (user-error "flocal guard is waiting for fresh share status; save again shortly"))
   ((eq flocal--state 'cannot-verify)
    (user-error "flocal guard cannot verify this file; disk and buffer were left unchanged"))
   ((not flocal--share)
    (funcall original arg))
   ((or write-contents-functions write-file-functions)
    (user-error "flocal guard cannot verify this buffer's custom save function"))
   ;; A new file has no disk baseline yet.  Its first ordinary save establishes
   ;; one; an existing file without a baseline is never silently accepted.
   ((not flocal--base-hash)
    (if (file-exists-p buffer-file-name)
        (user-error "flocal guard has no baseline for this file; revert it before saving")
      (let ((flocal--saving t))
        (funcall original arg))))
   (t
    (let ((disk-hash (flocal--disk-hash)))
      (cond
       ;; Ediff's `wa' is the only permitted write while a conflict is
       ;; pending.  The disk must still be exactly the snapshot that Ediff
       ;; showed; otherwise start over with a fresh snapshot.
       ((and flocal--pending-disk-hash flocal--ediff-writing
             (equal disk-hash flocal--pending-disk-hash))
        (let ((flocal--saving t)) (funcall original arg))
        (setq flocal--pending-disk-hash nil))
       ((and flocal--pending-disk-hash
             (not flocal--ediff-writing))
        (flocal--start-ediff disk-hash))
       ((equal disk-hash flocal--base-hash)
        (let ((flocal--saving t)) (funcall original arg))
        (setq flocal--pending-disk-hash nil))
       (t (flocal--start-ediff disk-hash)))))))

(defun flocal--ediff-save-buffer (original arg)
  (let ((flocal--ediff-writing t))
    (funcall original arg)))

(defun flocal--supersession-threat (original &rest arguments)
  "Defer the mtime-only warning to this buffer's content-aware save guard."
  (if (and flocal--share (flocal--cache-fresh-p))
      nil
    (apply original arguments)))

(defun flocal--ediff-quit ()
  (when (and (boundp 'ediff-buffer-B)
             (buffer-live-p ediff-buffer-B))
    (with-current-buffer ediff-buffer-B
      (when (bound-and-true-p flocal--private-disk-buffer)
        (kill-buffer ediff-buffer-B)))))

(define-derived-mode flocal-status-mode special-mode "flocal-status"
  "Mode for the read-only flocal status buffer."
  (define-key flocal-status-mode-map (kbd "g") #'flocal-refresh))

(defun flocal--render-status ()
  (with-current-buffer (get-buffer-create "*flocal status*")
    (let ((inhibit-read-only t))
      (erase-buffer)
      (if flocal--report
          (progn
            (insert (format "Source: %s\nObserved: %s\nDaemon: %s\n"
                            (flocal--report-source flocal--report)
                            (flocal--report-observed-at flocal--report)
                            (alist-get 'state (flocal--report-daemon flocal--report))))
            (when-let ((diagnostic
                        (flocal--diagnostic
                         (alist-get 'diagnostic (flocal--report-daemon flocal--report)))))
              (insert (format "Daemon diagnostic: %s\n" (flocal--display diagnostic))))
            (insert (if (flocal--cache-fresh-p) "Fresh: yes\n" "Fresh: no\n"))
            (dolist (share (flocal--report-shares flocal--report))
              (let ((details (flocal--share-details share)))
                (insert "\nRoot: " (flocal--display (flocal--share-root share)) "\n"
                        "Share: " (flocal--display (alist-get 'share details)) "\n"
                        "Enabled: " (flocal--display (alist-get 'enabled details)) "\n"
                        "Connection: " (flocal--display (alist-get 'connection_state details)) "\n"
                        "Scheduling: " (flocal--display (alist-get 'scheduling details)) "\n"
                        "Role: " (flocal--display (alist-get 'role details)) "\n"
                        "Initial complete: " (flocal--display (alist-get 'initial_complete details)) "\n"
                        "Unsettled: " (flocal--display (alist-get 'unsettled details)) "\n")
                (when-let ((diagnostic (flocal--diagnostic
                                        (alist-get 'diagnostic details))))
                  (insert "Diagnostic: " (flocal--display diagnostic) "\n")))))
        (insert "Source: unavailable\nFresh: no\n"))
      (when flocal--refresh-error
        (insert "Status error: " (flocal--display flocal--refresh-error) "\n"))
      (flocal-status-mode))))

(defun flocal--redraw-status ()
  (when (get-buffer "*flocal status*")
    (flocal--render-status)))

(defun flocal-status ()
  "Show every cached flocal share without offering controls."
  (interactive)
  (flocal--render-status)
  (pop-to-buffer "*flocal status*"))

(defun flocal--valid-diagnostic-p (value)
  (or (memq value '(nil :null))
      (and (stringp value)
           (<= (string-bytes value) flocal--max-diagnostic-bytes))))

(defun flocal--validate-share (share)
  (unless (and (stringp (alist-get 'share share))
               (memq (alist-get 'enabled share) '(t :false))
               (member (alist-get 'connection_state share)
                       '("starting" "watching" "reconnecting" "stopping"
                         "blocked" "registering" "removing" "unknown" "stopped"))
               (member (alist-get 'scheduling share)
                       '("idle" "queued" "active" "unknown"))
               (member (alist-get 'role share)
                       '("connector" "responder" "registering" "removing"))
               (memq (alist-get 'initial_complete share) '(t :false))
               (flocal--valid-diagnostic-p (alist-get 'diagnostic share))
               (let ((unsettled (alist-get 'unsettled share)))
                 (and (integerp unsettled) (>= unsettled 0))))
    (error "flocal returned an invalid share status"))
  (let* ((details (alist-get 'root share))
         (root (flocal--decode-root details)))
    (flocal--identity-number (alist-get 'device details))
    (flocal--identity-number (alist-get 'inode details))
    (unless (and (> (length root) 0)
                 (file-name-absolute-p root)
                 (not (string-match-p "\0" root)))
      (error "flocal returned an invalid share root"))
    (flocal--share-create :root root :details share)))

(defun flocal--eligible-shares (shares)
  (delq nil
        (mapcar
         (lambda (share)
           (when (and (eq (alist-get 'enabled (flocal--share-details share)) t)
                      (not (equal (alist-get 'connection_state
                                             (flocal--share-details share))
                                  "stopped"))
                      (flocal--root-identity-matches-p
                       (flocal--share-root share) (flocal--share-details share)))
             (let ((canonical (file-name-as-directory
                               (file-truename (flocal--share-root share)))))
               (when (and (flocal--root-identity-matches-p
                           (flocal--share-root share) (flocal--share-details share))
                          (flocal--root-identity-matches-p
                           canonical (flocal--share-details share)))
                 (setf (flocal--share-canonical-root share) canonical)
                 share))))
         shares)))

(defun flocal--validate-report (report)
  (unless (let ((source (alist-get 'source report))
                (observed-at (alist-get 'observed_at report))
                (daemon (alist-get 'daemon report)))
            (and (equal (alist-get 'schema report) 1)
                 (member source '("live" "stored"))
                 (flocal--rfc3339-utc-p observed-at)
                 (and (listp daemon)
                      (equal (alist-get 'state daemon)
                             (if (equal source "live") "live" "unavailable"))
                      (flocal--valid-diagnostic-p (alist-get 'diagnostic daemon)))
                 (listp (alist-get 'shares report))))
    (error "flocal returned an unsupported status report"))
  (flocal--report-create
   :source (alist-get 'source report)
   :observed-at (alist-get 'observed_at report)
   :daemon (alist-get 'daemon report)
   :shares (mapcar #'flocal--validate-share (alist-get 'shares report))))

(defun flocal--reclassify-buffers ()
  (dolist (buffer (buffer-list))
    (with-current-buffer buffer (flocal--classify-buffer))))

(defun flocal--refresh-timeout (process)
  (when (process-live-p process)
    (process-put process 'flocal-error "flocal status timed out")
    (delete-process process)))

(defun flocal--refresh-filter (process output)
  (with-current-buffer (process-buffer process)
    (if (> (+ (string-bytes (buffer-string)) (string-bytes output))
           flocal-status-max-bytes)
        (progn
          (process-put process 'flocal-error "flocal status output exceeded its limit")
          (delete-process process))
      (goto-char (point-max))
      (insert output))))

(defun flocal--refresh-finished (process _event)
  (when (eq process flocal--refresh-process)
    (when (timerp flocal--refresh-timer)
      (cancel-timer flocal--refresh-timer))
    (setq flocal--refresh-timer nil
          flocal--refresh-process nil)
    (unwind-protect
        (condition-case error
            (let ((message (process-get process 'flocal-error)))
              (when message (error "%s" message))
              (unless (and (eq (process-status process) 'exit)
                           (zerop (process-exit-status process)))
                (error "flocal status exited unsuccessfully"))
              (let* ((raw-report (json-parse-string
                                  (with-current-buffer (process-buffer process) (buffer-string))
                                  :object-type 'alist :array-type 'list))
                     (report (flocal--validate-report raw-report)))
                (setq flocal--report report
                      flocal--shares (flocal--eligible-shares
                                      (flocal--report-shares report))
                      flocal--cache-updated-at (float-time)
                      flocal--cache-valid t
                      flocal--refresh-error nil)))
          (error
           (setq flocal--cache-valid nil
                 flocal--refresh-error (error-message-string error))))
      (when (buffer-live-p (process-buffer process))
        (kill-buffer (process-buffer process))))
    (flocal--reclassify-buffers)
    (flocal--redraw-status)))

(defun flocal--status-executable ()
  (let ((configured flocal-executable))
    (unless (and (stringp configured)
                 (file-name-absolute-p configured)
                 (file-regular-p configured)
                 (not (file-symlink-p configured))
                 (file-executable-p configured))
      (user-error "flocal executable is unavailable or unsafe"))
    (file-truename configured)))

(defun flocal-refresh ()
  "Refresh the read-only flocal share cache asynchronously."
  (interactive)
  (unless (process-live-p flocal--refresh-process)
    (let ((buffer (generate-new-buffer " *flocal-status*"))
          (executable (flocal--status-executable)))
      (setq flocal--refresh-process
            (make-process :name "flocal status" :buffer buffer :noquery t
                          :connection-type 'pipe
                          :command (list executable "status" "--list" "--json")
                          :filter #'flocal--refresh-filter
                          :sentinel #'flocal--refresh-finished)
            flocal--refresh-timer
            (run-at-time flocal-status-timeout nil
                         #'flocal--refresh-timeout
                         flocal--refresh-process)))
    (flocal--redraw-status)
    (when (called-interactively-p 'interactive)
      (message "Refreshing flocal status..."))))

;;;###autoload
(define-minor-mode flocal-mode
  "Show flocal status and protect eligible visiting buffers."
  :global t :group 'flocal
  (if flocal-mode
      (progn (add-hook 'find-file-hook #'flocal--visit-file)
             (add-hook 'after-save-hook #'flocal--after-save)
             (advice-add 'save-buffer :around #'flocal--save-buffer)
             (advice-add 'ediff-save-buffer :around #'flocal--ediff-save-buffer)
             (advice-add 'ask-user-about-supersession-threat :around
                         #'flocal--supersession-threat)
             (add-hook 'ediff-quit-hook #'flocal--ediff-quit)
             (setq flocal--cache-valid nil
                   flocal--refresh-error nil)
             (flocal--reclassify-buffers)
             (setq flocal--refresh-idle-timer
                   (run-with-idle-timer flocal-refresh-interval t
                                        #'flocal--idle-refresh))
             (flocal--idle-refresh))
    (remove-hook 'find-file-hook #'flocal--visit-file)
    (remove-hook 'after-save-hook #'flocal--after-save)
    (advice-remove 'save-buffer #'flocal--save-buffer)
    (advice-remove 'ediff-save-buffer #'flocal--ediff-save-buffer)
    (advice-remove 'ask-user-about-supersession-threat
                   #'flocal--supersession-threat)
    (remove-hook 'ediff-quit-hook #'flocal--ediff-quit)
    (when (timerp flocal--refresh-timer)
      (cancel-timer flocal--refresh-timer))
    (when (timerp flocal--refresh-idle-timer)
      (cancel-timer flocal--refresh-idle-timer))
    (let ((process flocal--refresh-process))
      (setq flocal--refresh-process nil)
      (when (processp process)
        (when (process-live-p process)
          (delete-process process))
        (when (buffer-live-p (process-buffer process))
          (kill-buffer (process-buffer process)))))
    (setq flocal--shares nil
          flocal--report nil
          flocal--refresh-timer nil
          flocal--refresh-idle-timer nil
          flocal--cache-valid nil)))

(provide 'flocal)
;;; flocal.el ends here
