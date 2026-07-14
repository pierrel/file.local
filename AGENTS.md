# file.local development guide

This repository builds `file.local`, a local-first directory synchronizer for
Linux and macOS. Keep the implementation small and make filesystem and network
failure modes explicit. The current product contract lives in
`docs/2026-07-14-v0.0.1-design.org`.

## Development cycle

Every non-trivial feature follows three phases:

1. **Design.** Write or update a dated state document in `docs/`. Describe the
   user flow, interfaces, trade-offs, failure modes, security boundaries, and
   acceptance criteria. Review the design through the lenses of simplicity,
   user experience, clean interfaces, and threat model before implementing.
2. **Code.** Implement against the accepted design and test the observable
   behavior. Review for simplicity, readability, design adherence, shared-logic
   reduction, filesystem correctness, and adversarial security. Network and
   untrusted-filesystem changes receive the most scrutiny.
3. **Review.** Reconcile the design document against the actual diff, run the
   complete local test and lint suite, and review until clean. Then push the
   feature branch and open a draft pull request. The user merges it.

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
