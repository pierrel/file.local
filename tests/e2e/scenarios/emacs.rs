//! The first editor client consumes the same read-only client status contract
//! intended for future web and editor integrations.

use anyhow::Result;

use crate::harness as e2e;

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn emacs_shows_an_active_flocal_share_as_guarded() -> Result<()> {
    let (a, _b) = e2e::managed_pair()?;
    a.write("emacs.txt", "base")?;
    let form = r#"(progn
      (require 'flocal-guard)
      (setq flocal-guard-executable "/usr/local/bin/flocal")
      (flocal-guard-mode 1)
      (while (process-live-p flocal-guard--refresh-process) (accept-process-output nil 0.1))
      (find-file "/home/peer/share/emacs.txt")
      (princ (flocal-guard--mode-line))
      (flocal-guard-status)
      (princ (with-current-buffer "*flocal status*" (buffer-string))))"#;
    let output = a.emacs_ok(form)?;
    anyhow::ensure!(
        String::from_utf8_lossy(&output.stdout).contains("FLOCAL:guarded"),
        "Emacs did not render guarded status: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    anyhow::ensure!(
        String::from_utf8_lossy(&output.stdout).contains("Source: live"),
        "Emacs status buffer did not show the live source: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    Ok(())
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn emacs_snapshots_a_real_remote_change_before_saving() -> Result<()> {
    let (a, b) = e2e::managed_pair()?;
    a.write("emacs-conflict.txt", "base\n")?;
    b.wait_for_file("emacs-conflict.txt", "base\n")?;
    let form = r#"(progn
      (require 'cl-lib)
      (require 'flocal-guard)
      (setq flocal-guard-executable "/usr/local/bin/flocal")
      (flocal-guard-mode 1)
      (while (process-live-p flocal-guard--refresh-process) (accept-process-output nil 0.1))
      (find-file "/home/peer/share/emacs-conflict.txt")
      (goto-char (point-max))
      (insert "mine\n")
      (write-region "ready" nil "/home/peer/share/.flocal-tmp-emacs-ready" nil 'silent)
      (while (not (file-exists-p "/home/peer/share/.flocal-tmp-emacs-go"))
        (sleep-for 0.05))
      (let ((result "/home/peer/share/.flocal-tmp-emacs-result"))
        (cl-letf (((symbol-function 'ediff-buffers)
                   (lambda (a b)
                     (with-temp-file result
                       (insert "A=" (with-current-buffer a (buffer-string))
                               "B=" (with-current-buffer b (buffer-string)))))))
          (save-buffer)))
      (kill-emacs 0))"#;
    a.emacs_start(form)?;
    a.wait_for_file(".flocal-tmp-emacs-ready", "ready")?;
    b.write("emacs-conflict.txt", "remote\n")?;
    a.wait_for_file("emacs-conflict.txt", "remote\n")?;
    a.write(".flocal-tmp-emacs-go", "go")?;
    a.wait_for_file(".flocal-tmp-emacs-result", "A=base\nmine\nB=remote\n")?;
    a.assert_file("emacs-conflict.txt", "remote\n")?;
    Ok(())
}
