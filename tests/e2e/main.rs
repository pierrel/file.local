//! Two-container end-to-end acceptance scenarios. Every test is
//! `#[ignore]`d: run them with `make e2e` on a machine with Docker. The
//! scenario modules read as the catalog in
//! docs/2026-07-16-e2e-harness-design.org section 7.

mod harness;
mod scenarios;
