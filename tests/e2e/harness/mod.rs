//! The scenario vocabulary for the two-container end-to-end harness. See
//! docs/2026-07-16-e2e-harness-design.org; scenarios use only what this
//! module re-exports and never issue raw docker commands.

mod docker;
mod dump;
mod peer;

pub use peer::{assert_trees_equal, containers, known_failure, pair};
