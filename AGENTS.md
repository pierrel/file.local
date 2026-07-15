# file.local development guide

This repository builds `file.local`, a local-first directory synchronizer for
Linux and macOS. Keep the implementation small and make filesystem and network
failure modes explicit. `README.org` is the durable high-level product and
architecture contract. Update it in the same pull request whenever a design or
implementation change no longer aligns with it. Dated files in `docs/` retain
the detailed decisions and state for individual features.

## Development cycle

Every non-trivial feature follows three phases:

1. **Design.** An architect subagent writes a numbered plan with concrete
   references, user flow, interfaces, trade-offs, failure modes, security
   boundaries, and acceptance criteria in a dated `docs/` state document. The
   primary agent reads and redirects it. Then run independent design-review
   subagents in parallel, one per lens: simplicity; platform and dependency
   fit; user guidance and intention; end-to-end user experience; clean
   interfaces; filesystem correctness; and threat model/attack surface. Apply
   the most scrutiny to network and untrusted-filesystem changes. Reconcile
   findings into the design before implementing.
2. **Code.** Implement against the accepted design and test the observable
   behavior. Then run independent code-review subagents in parallel through the
   lenses of simplicity; clean code and interfaces; readability; existing
   patterns; design adherence; shared-logic and line-count reduction;
   cross-platform filesystem behavior; and adversarial security. Add a
   dedicated documentation-alignment pass. Fix every blocker and important
   finding, rerun validation, and repeat the affected reviews until clean.
3. **Review.** Run the complete local review loop to convergence. Reconcile the
   design document and `README.org` against the actual diff, run the complete
   local test and lint suite, and record any changed decision and why. Then push
   the feature branch and open a draft pull request. Address review feedback by
   rerunning the affected local-review lenses before pushing. After the fix is
   pushed, resolve each GitHub thread that it fully addresses. If a thread needs
   a user decision or clarification, reply in that thread instead of resolving
  it and sign the reply `— Codex` so automated comments are distinguishable
  from the user's own comments. Once local review is clean and current feedback
  is addressed, mark the pull request ready for review before requesting a
  Copilot review. Never request Copilot while the pull request is still a draft.
  The user merges.

Branch from `main`, never from another feature branch. Use one feature per
branch. Commit and push freely on feature branches, but never merge or commit
directly to `main` without explicit approval.

## Engineering principles

- Simplicity is the default. Prefer deleting code, narrow interfaces, and
  representations that make invalid states impossible.
- Build only for an accepted story or an observed failure. Do not add
  theoretical abstractions or defensive branches without a concrete need.
- Fix correctness problems by construction, not by making them less likely.
- Test the symptom and the real boundary. Do not mock away filesystem,
  concurrency, interruption, or cross-platform behavior that the test claims
  to verify.
- Treat paths, filenames, file contents, peer messages, ignore rules, and
  symbolic-link targets as untrusted input.
- Never follow a synchronized symbolic link while applying remote changes.
- Never place secrets, real hostnames, IP addresses, or personal filesystem
  paths in tracked files. Use synthetic examples.
- Keep durable project guidance here. Tool-specific files should point here
  instead of duplicating these rules.
- After a change, check every affected comment, example, and design statement
  for drift. Documentation mismatch is a defect.

## Pull requests

Stage only files that belong to the feature. A pull request description must
state what changed, why, user impact, security implications, and validation
performed. Design-only pull requests should identify unresolved decisions and
must not imply that the feature is implemented.
