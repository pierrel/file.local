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

## Review subagents and lenses

Use independent subagents so each review has a narrow mandate and findings can
be traced to a specific lens. A reviewer reports blockers, important findings,
and minor findings with file or design references; it does not edit the work it
is reviewing. Combine closely related lenses only when agent capacity requires
it, and explicitly name every lens in the assignment.

- **Architect (authoring role, not a review lens):** receives the accepted user
  story, repository constraints, research, and prior decisions. It produces the
  numbered dated design plan required by the Design phase, including concrete
  interfaces, data/state flow, alternatives, failure and security boundaries,
  acceptance tests, and unresolved decisions. It must not approve its own
  design; the primary agent redirects the draft and independent reviewers test
  it through the lenses below.
- **Simplicity:** looks for unnecessary concepts, duplicated paths, excess
  dependencies, quadratic behavior, and opportunities to make correctness
  follow from a smaller representation or interface.
- **Platform and dependency fit:** verifies Linux and macOS behavior, Rust and
  system dependency choices, filesystem/API availability, packaging, and CI
  coverage on every supported platform.
- **User guidance and intention:** checks that the work answers the stated user
  stories, terminology and examples are understandable, prerequisites are
  explicit, and deferred behavior is clearly distinguished from implemented
  behavior.
- **End-to-end user experience:** walks installation, pairing, initial sync,
  everyday editing, offline recovery, conflicts, status, and failure recovery
  as a user would, looking for ambiguity or surprising state transitions.
- **Interfaces and clean code:** reviews CLI, protocol, module, state-schema,
  and filesystem boundaries for narrow contracts, validated types, clear
  ownership, and useful errors. Findings identify the leaking or over-broad
  interface and propose the smallest clearer contract.
- **Readability:** reads the change as a future maintainer, checking names,
  control flow, error context, comments, locality, and whether invariants are
  apparent without reconstructing them across files. Findings cite the passage
  that requires avoidable inference and explain the intended reading.
- **Existing patterns:** compares the change with established repository
  conventions and analogous implementations. It flags unjustified new idioms,
  inconsistent error/state handling, and missed reuse, while rejecting a local
  pattern when it is itself unsafe or unsuitable.
- **Design adherence:** maps every accepted design decision and acceptance
  criterion to implementation and tests. It reports missing behavior, silent
  scope changes, and implementation discoveries that require README or design
  updates; it does not treat the design as correct when evidence disproves it.
- **Shared logic and line-count reduction:** looks for duplicated validation,
  framing, traversal, state transitions, and platform branches. It proposes
  deletion or one well-named shared primitive when that reduces both code and
  divergent behavior, but does not create abstractions solely to reduce lines.
- **Filesystem correctness:** examines scans, ignores, symlinks, permissions,
  atomic replacement, interruption recovery, durability, races, and Linux/macOS
  differences using real filesystem behavior rather than mocks.
- **Threat model and adversarial security:** treats peers, protocol frames,
  paths, ignore rules, object contents, SSH configuration, and concurrent local
  filesystem changes as hostile; checks containment, resource limits, identity
  binding, verification, and safe failure.
- **Documentation alignment:** compares README.org, dated design/implementation
  documents, examples, CLI help, and the actual diff; any mismatch is reported
  as a defect rather than left as follow-up prose.

The exact **design-review** mapping is: simplicity; platform and dependency fit;
user guidance and intention; end-to-end user experience; interfaces and clean
code; filesystem correctness; and threat model and adversarial security. The architect authors
the input but is not one of its reviewers. The exact **code-review** mapping is:
simplicity; interfaces and clean code; readability; existing patterns; design
adherence; shared-logic and line-count reduction; cross-platform filesystem
correctness; adversarial security; and documentation alignment. Each reviewer
must cite evidence and return categorized findings or an explicit clear result.
Rerun every affected lens after a fix until it reports no blocker or important
finding.

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
