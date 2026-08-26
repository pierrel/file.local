;;; flocal-guard.el --- Save protection for active flocal shares -*- lexical-binding: t; -*-

;; Copyright (C) 2026 Pierre Bombay

;; Author: Pierre Bombay
;; Version: 0.1.0
;; Package-Requires: ((emacs "28.1"))
;; Keywords: files, tools

;;; Commentary:

;; `flocal-guard-mode' discovers active local flocal shares and marks visiting
;; buffers below them.  Save-time conflict handling is added in this package;
;; this library deliberately has no flocal control commands.

;;; Code:

(require 'json)
(require 'iso8601)
(require 'seq)
(require 'ediff)

(defgroup flocal-guard nil
  "Emacs affordances for files synchronized by flocal."
  :group 'files)

(defcustom flocal-guard-executable (executable-find "flocal")
  "Absolute path to the flocal executable used for read-only status discovery."
  :type '(choice (const :tag "Not found" nil) file)
  :group 'flocal-guard)

(defcustom flocal-guard-max-conflict-bytes (* 1024 1024)
  "Largest file the guard will snapshot or present in Ediff."
  :type 'integer
  :group 'flocal-guard)

(defcustom flocal-guard-status-timeout 3
  "Seconds an asynchronous status request may run before it is killed."
  :type 'number
  :group 'flocal-guard)

(defcustom flocal-guard-status-max-bytes (* 64 1024)
  "Largest status response accepted from the flocal executable."
  :type 'integer
  :group 'flocal-guard)

(defcustom flocal-guard-refresh-interval 15
  "Seconds for which a successful status response remains fresh."
  :type 'number
  :group 'flocal-guard)

(defvar flocal-guard--shares nil)
(defvar flocal-guard--refresh-process nil)
(defvar flocal-guard--refresh-timer nil)
(defvar flocal-guard--refresh-idle-timer nil)
(defvar flocal-guard--cache-source nil)
(defvar flocal-guard--cache-updated-at nil)
(defvar flocal-guard--cache-valid nil)
(defvar flocal-guard--refresh-error nil)
(defvar flocal-guard--ediff-writing nil)
(defvar-local flocal-guard--share nil)
(defvar-local flocal-guard--state 'checking)
(defvar-local flocal-guard--base-hash nil)
(defvar-local flocal-guard--pending-disk-hash nil)
(defvar-local flocal-guard--saving nil)
(defvar-local flocal-guard--private-disk-buffer nil)

(defun flocal-guard--mode-line ()
  (if buffer-file-name
      (let ((share (cdr flocal-guard--share)))
        (pcase flocal-guard--state
          ((or 'guarded 'stored)
           (format " FLOCAL:%s/%s/%s" flocal-guard--state
                   (or (alist-get 'connection_state share) "unknown")
                   (or (alist-get 'scheduling share) "unknown")))
          ('conflict " FLOCAL:conflict")
          ('checking " FLOCAL:checking")
          ('cannot-verify " FLOCAL:cannot-verify")
          (_ "")))
    ""))

(add-to-list 'minor-mode-alist
             '(flocal-guard-mode (:eval (flocal-guard--mode-line))))

(defun flocal-guard--decode-root (root)
  (unless (equal (alist-get 'encoding root) "base64")
    (error "flocal returned an unsupported root encoding"))
  (decode-coding-string (base64-decode-string (alist-get 'data root))
                        file-name-coding-system))

(defun flocal-guard--rfc3339-utc-p (value)
  (and (stringp value)
       (string-match-p
        "\\`[0-9]\\{4\\}-[0-9]\\{2\\}-[0-9]\\{2\\}T[0-9]\\{2\\}:[0-9]\\{2\\}:[0-9]\\{2\\}Z\\'"
        value)
       (condition-case nil
           (equal (format-time-string "%FT%TZ" (encode-time (iso8601-parse value)) t) value)
         (error nil))))

(defun flocal-guard--under-root-p (file root)
  (let ((file (file-truename file))
        (root (file-name-as-directory (file-truename root))))
    (string-prefix-p root file)))

(defun flocal-guard--share-for-file (file)
  (car (sort (seq-filter (lambda (share) (flocal-guard--under-root-p file (car share)))
                         flocal-guard--shares)
             (lambda (left right) (> (length (car left)) (length (car right)))))))

(defun flocal-guard--classify-buffer ()
  (when buffer-file-name
    ;; Capture the version Emacs showed at visit (or when the mode is enabled
    ;; for an already visiting, unmodified buffer).  Reclassification after a
    ;; status refresh must never turn unsaved edits into a disk baseline.
    (when (and (not flocal-guard--base-hash) (not (buffer-modified-p)))
      (condition-case _error
          (flocal-guard--capture-base)
        (error (setq flocal-guard--state 'cannot-verify))))
    (if (not (flocal-guard--cache-fresh-p))
        (setq flocal-guard--share nil
              flocal-guard--state 'checking)
      (setq flocal-guard--share (flocal-guard--share-for-file buffer-file-name)
            flocal-guard--state
            (cond ((not flocal-guard--share) 'outside)
                  ((equal flocal-guard--cache-source "stored") 'stored)
                  (t 'guarded))))))

(defun flocal-guard--visit-file ()
  (flocal-guard--classify-buffer)
  (unless (flocal-guard--cache-fresh-p)
    (condition-case error
        (flocal-guard-refresh)
      (error
       (setq flocal-guard--cache-valid nil
             flocal-guard--refresh-error (error-message-string error))))))

(defun flocal-guard--idle-refresh ()
  (condition-case error
      (flocal-guard-refresh)
    (error
     (setq flocal-guard--cache-valid nil
           flocal-guard--refresh-error (error-message-string error))
     (flocal-guard--reclassify-buffers))))

(defun flocal-guard--cache-fresh-p ()
  (and flocal-guard--cache-valid flocal-guard--cache-updated-at
       (< (- (float-time) flocal-guard--cache-updated-at)
          flocal-guard-refresh-interval)))

(defun flocal-guard--capture-base ()
  "Remember the bytes in this buffer, not a later pathname replacement."
  (when (and buffer-file-name (file-regular-p buffer-file-name)
             (not (file-symlink-p buffer-file-name)))
    (setq flocal-guard--base-hash (flocal-guard--buffer-hash))))

(defun flocal-guard--after-save ()
  (when flocal-guard--share
    (condition-case _error
        (flocal-guard--capture-base)
      (error (setq flocal-guard--state 'cannot-verify)))))

(defun flocal-guard--file-hash (file)
  "Return SHA-256 of FILE's literal bytes, without mtime-based caching."
  (when (> (file-attribute-size (file-attributes file))
           flocal-guard-max-conflict-bytes)
    (user-error "flocal guard refuses to snapshot a file larger than %d bytes"
                flocal-guard-max-conflict-bytes))
  (with-temp-buffer
    (flocal-guard--insert-limited-disk-file file)
    (secure-hash 'sha256 (current-buffer))))

(defun flocal-guard--insert-limited-disk-file (file)
  "Insert at most the configured disk snapshot limit plus one byte from FILE."
  ;; Request one byte over the limit so a replacement after the attribute check
  ;; cannot make Emacs accumulate an unbounded disk snapshot.
  (set-buffer-multibyte nil)
  (insert-file-contents-literally file nil 0 (1+ flocal-guard-max-conflict-bytes))
  (when (> (buffer-size) flocal-guard-max-conflict-bytes)
    (user-error "flocal guard refuses to snapshot a file larger than %d bytes"
                flocal-guard-max-conflict-bytes)))

(defun flocal-guard--buffer-hash ()
  "Return SHA-256 of the visiting buffer's normal file coding output."
  (let ((bytes (encode-coding-string
                (buffer-substring-no-properties (point-min) (point-max))
                buffer-file-coding-system)))
    (when (string-match-p "\\0" bytes)
      (user-error "flocal guard cannot merge binary buffer contents"))
    (when (> (string-bytes bytes) flocal-guard-max-conflict-bytes)
      (user-error "flocal guard refuses to protect a buffer larger than %d bytes"
                  flocal-guard-max-conflict-bytes))
    (secure-hash 'sha256 bytes)))

(defun flocal-guard--disk-hash ()
  (unless (and buffer-file-name (file-regular-p buffer-file-name)
               (not (file-symlink-p buffer-file-name)))
    (user-error "flocal guard cannot verify the file on disk"))
  (flocal-guard--file-hash buffer-file-name))

(defun flocal-guard--disk-buffer (expected-hash)
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
          (flocal-guard--insert-limited-disk-file file)
          (goto-char (point-min))
          (when (search-forward "\0" nil t)
            (user-error "flocal guard cannot merge a binary disk file"))
          (unless (equal (secure-hash 'sha256 (current-buffer)) expected-hash)
            (user-error "flocal guard observed the disk file change while reading it"))
          (setq buffer-read-only t)
          (setq-local flocal-guard--private-disk-buffer t))
      (error (kill-buffer buffer) (signal (car error) (cdr error))))
    buffer))

(defun flocal-guard--start-ediff (hash)
  (let ((disk (flocal-guard--disk-buffer hash)))
    (setq flocal-guard--pending-disk-hash hash
          flocal-guard--state 'conflict)
    (ediff-buffers (current-buffer) disk)))

(defun flocal-guard--save-buffer (original &optional arg)
  (cond
   ((or flocal-guard--saving (not (buffer-modified-p)))
    (funcall original arg))
   ((not (flocal-guard--cache-fresh-p))
    (flocal-guard-refresh)
    (user-error "flocal guard is waiting for fresh share status; save again shortly"))
   ((eq flocal-guard--state 'cannot-verify)
    (user-error "flocal guard cannot verify this file; disk and buffer were left unchanged"))
   ((not flocal-guard--share)
    (funcall original arg))
   ((or write-contents-functions write-file-functions)
    (user-error "flocal guard cannot verify this buffer's custom save function"))
   ;; A new file has no disk baseline yet.  Its first ordinary save establishes
   ;; one; an existing file without a baseline is never silently accepted.
   ((not flocal-guard--base-hash)
    (if (file-exists-p buffer-file-name)
        (user-error "flocal guard has no baseline for this file; revert it before saving")
      (let ((flocal-guard--saving t))
        (funcall original arg))))
   (t
    (let ((disk-hash (flocal-guard--disk-hash)))
      (cond
       ;; Ediff's `wa' is the only permitted write while a conflict is
       ;; pending.  The disk must still be exactly the snapshot that Ediff
       ;; showed; otherwise start over with a fresh snapshot.
       ((and flocal-guard--pending-disk-hash flocal-guard--ediff-writing
             (equal disk-hash flocal-guard--pending-disk-hash))
        (let ((flocal-guard--saving t)) (funcall original arg))
        (setq flocal-guard--pending-disk-hash nil))
       ((and flocal-guard--pending-disk-hash
             (not flocal-guard--ediff-writing))
        (flocal-guard--start-ediff disk-hash))
       ((equal disk-hash flocal-guard--base-hash)
        (let ((flocal-guard--saving t)) (funcall original arg))
        (setq flocal-guard--pending-disk-hash nil))
       (t (flocal-guard--start-ediff disk-hash)))))))

(defun flocal-guard--ediff-save-buffer (original arg)
  (let ((flocal-guard--ediff-writing t))
    (funcall original arg)))

(defun flocal-guard--supersession-threat (original &rest arguments)
  "Defer the mtime-only warning to this buffer's content-aware save guard."
  (if (and flocal-guard--share (flocal-guard--cache-fresh-p))
      nil
    (apply original arguments)))

(defun flocal-guard--ediff-quit ()
  (when (and (boundp 'ediff-buffer-B)
             (buffer-live-p ediff-buffer-B))
    (with-current-buffer ediff-buffer-B
      (when (bound-and-true-p flocal-guard--private-disk-buffer)
        (kill-buffer ediff-buffer-B)))))

(defun flocal-guard-status ()
  "Show the discovered active flocal shares without offering controls."
  (interactive)
  (with-current-buffer (get-buffer-create "*flocal status*")
    (setq buffer-read-only nil)
    (erase-buffer)
    (insert (format "Source: %s\n" (or flocal-guard--cache-source "unavailable")))
    (when flocal-guard--refresh-error
      (insert (format "Status error: %s\n" flocal-guard--refresh-error)))
    (insert "Root\tConnection\tScheduling\n")
    (dolist (entry flocal-guard--shares)
      (insert (prin1-to-string (car entry)) "\t"
              (alist-get 'connection_state (cdr entry)) "\t"
              (alist-get 'scheduling (cdr entry)) "\n"))
    (special-mode))
  (pop-to-buffer "*flocal status*"))

(defun flocal-guard--validate-report (report)
  (unless (let ((source (alist-get 'source report))
                (observed-at (alist-get 'observed_at report))
                (daemon (alist-get 'daemon report)))
            (and (equal (alist-get 'schema report) 1)
                 (member source '("live" "stored"))
                 (flocal-guard--rfc3339-utc-p observed-at)
                 (and (listp daemon)
                      (equal (alist-get 'state daemon)
                             (if (equal source "live") "live" "unavailable"))
                      (let ((diagnostic (alist-get 'diagnostic daemon)))
                        (or (memq diagnostic '(nil :null)) (stringp diagnostic))))
                 (listp (alist-get 'shares report))))
    (error "flocal returned an unsupported status report"))
  (seq-filter
   (lambda (entry)
     (and (eq (alist-get 'enabled (cdr entry)) t)
          (not (equal (alist-get 'connection_state (cdr entry)) "stopped"))))
   (seq-map
    (lambda (share)
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
                   (let ((diagnostic (alist-get 'diagnostic share)))
                     (or (memq diagnostic '(nil :null)) (stringp diagnostic)))
                   (let ((unsettled (alist-get 'unsettled share)))
                     (and (integerp unsettled) (>= unsettled 0))))
        (error "flocal returned an invalid share status"))
      (let ((root (flocal-guard--decode-root (alist-get 'root share))))
        (unless (and (> (length root) 0)
                     (file-name-absolute-p root)
                     (not (string-match-p "\\0" root)))
          (error "flocal returned an invalid share root"))
        (cons root share)))
    (alist-get 'shares report))))

(defun flocal-guard--reclassify-buffers ()
  (dolist (buffer (buffer-list))
    (with-current-buffer buffer (flocal-guard--classify-buffer))))

(defun flocal-guard--refresh-timeout (process)
  (when (process-live-p process)
    (process-put process 'flocal-guard-error "flocal status timed out")
    (delete-process process)))

(defun flocal-guard--refresh-filter (process output)
  (with-current-buffer (process-buffer process)
    (if (> (+ (string-bytes (buffer-string)) (string-bytes output))
           flocal-guard-status-max-bytes)
        (progn
          (process-put process 'flocal-guard-error "flocal status output exceeded its limit")
          (delete-process process))
      (goto-char (point-max))
      (insert output))))

(defun flocal-guard--refresh-finished (process _event)
  (when (eq process flocal-guard--refresh-process)
    (when (timerp flocal-guard--refresh-timer)
      (cancel-timer flocal-guard--refresh-timer))
    (setq flocal-guard--refresh-timer nil
          flocal-guard--refresh-process nil)
    (unwind-protect
        (condition-case error
            (let ((message (process-get process 'flocal-guard-error)))
              (when message (error "%s" message))
              (unless (and (eq (process-status process) 'exit)
                           (zerop (process-exit-status process)))
                (error "flocal status exited unsuccessfully"))
              (let* ((report (json-parse-string
                              (with-current-buffer (process-buffer process) (buffer-string))
                              :object-type 'alist :array-type 'list))
                     (shares (flocal-guard--validate-report report)))
                (setq flocal-guard--shares shares
                      flocal-guard--cache-source (alist-get 'source report)
                      flocal-guard--cache-updated-at (float-time)
                      flocal-guard--cache-valid t
                      flocal-guard--refresh-error nil)))
          (error
           (setq flocal-guard--cache-valid nil
                 flocal-guard--refresh-error (error-message-string error))))
      (when (buffer-live-p (process-buffer process))
        (kill-buffer (process-buffer process))))
    (flocal-guard--reclassify-buffers)))

(defun flocal-guard--status-executable ()
  (let ((executable (and (stringp flocal-guard-executable)
                         (file-truename flocal-guard-executable))))
    (unless (and executable
                 (file-name-absolute-p executable)
                 (file-regular-p executable)
                 (not (file-symlink-p executable))
                 (file-executable-p executable))
      (user-error "flocal executable is unavailable or unsafe"))
    executable))

(defun flocal-guard-refresh ()
  "Refresh the read-only flocal share cache asynchronously."
  (interactive)
  (unless (process-live-p flocal-guard--refresh-process)
    (let ((buffer (generate-new-buffer " *flocal-status*"))
          (executable (flocal-guard--status-executable)))
      (setq flocal-guard--refresh-process
            (make-process :name "flocal status" :buffer buffer :noquery t
                          :connection-type 'pipe
                          :command (list executable "status" "--list" "--json")
                          :filter #'flocal-guard--refresh-filter
                          :sentinel #'flocal-guard--refresh-finished)
            flocal-guard--refresh-timer
            (run-at-time flocal-guard-status-timeout nil
                         #'flocal-guard--refresh-timeout
                         flocal-guard--refresh-process)))))

;;;###autoload
(define-minor-mode flocal-guard-mode
  "Discover active flocal shares for file visiting buffers."
  :global t :group 'flocal-guard
  (if flocal-guard-mode
      (progn (add-hook 'find-file-hook #'flocal-guard--visit-file)
             (add-hook 'after-save-hook #'flocal-guard--after-save)
             (advice-add 'save-buffer :around #'flocal-guard--save-buffer)
             (advice-add 'ediff-save-buffer :around #'flocal-guard--ediff-save-buffer)
             (advice-add 'ask-user-about-supersession-threat :around
                         #'flocal-guard--supersession-threat)
             (add-hook 'ediff-quit-hook #'flocal-guard--ediff-quit)
             (setq flocal-guard--cache-valid nil
                   flocal-guard--refresh-error nil)
             (flocal-guard--reclassify-buffers)
             (setq flocal-guard--refresh-idle-timer
                   (run-with-idle-timer flocal-guard-refresh-interval t
                                        #'flocal-guard--idle-refresh))
             (flocal-guard--idle-refresh))
    (remove-hook 'find-file-hook #'flocal-guard--visit-file)
    (remove-hook 'after-save-hook #'flocal-guard--after-save)
    (advice-remove 'save-buffer #'flocal-guard--save-buffer)
    (advice-remove 'ediff-save-buffer #'flocal-guard--ediff-save-buffer)
    (advice-remove 'ask-user-about-supersession-threat
                   #'flocal-guard--supersession-threat)
    (remove-hook 'ediff-quit-hook #'flocal-guard--ediff-quit)
    (when (timerp flocal-guard--refresh-timer)
      (cancel-timer flocal-guard--refresh-timer))
    (when (timerp flocal-guard--refresh-idle-timer)
      (cancel-timer flocal-guard--refresh-idle-timer))
    (let ((process flocal-guard--refresh-process))
      (setq flocal-guard--refresh-process nil)
      (when (processp process)
        (when (process-live-p process)
          (delete-process process))
        (when (buffer-live-p (process-buffer process))
          (kill-buffer (process-buffer process)))))
    (setq flocal-guard--shares nil
          flocal-guard--refresh-timer nil
          flocal-guard--refresh-idle-timer nil
          flocal-guard--cache-valid nil)))

(provide 'flocal-guard)
;;; flocal-guard.el ends here
