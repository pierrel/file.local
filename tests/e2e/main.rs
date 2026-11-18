//! Two-container end-to-end acceptance scenarios. Every test is `#[ignore]`d:
//! run general scenarios with `make e2e` on a machine with Docker, the large
//! initial sync with `make e2e-large-initial`, and the `legacy_` upgrade scenarios
//! with the deployed-alpha CI command. The scenario
//! modules read as the catalog in
//! docs/2026-07-16-e2e-harness-design.org section 7.

mod harness;
mod scenarios;
