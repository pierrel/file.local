//! The scenario vocabulary for the two-container end-to-end harness. See
//! docs/2026-07-16-e2e-harness-design.org; scenarios use only what this
//! module re-exports and never issue raw docker commands.

mod docker;
mod dump;
mod peer;

pub use peer::{
    Config, assert_trees_equal, containers, known_failure, managed_pair, pair, pair_with,
};

/// With every catalog pin promoted, no scenario currently wraps
/// `known_failure` — but it is standing vocabulary (the AGENTS.md bug
/// workflow pins newly reported bugs with it where possible), so its
/// semantics stay
/// exercised here without Docker.
#[test]
fn known_failure_passes_only_when_its_body_fails() {
    assert!(known_failure(|| anyhow::bail!("the pinned bug")).is_ok());
    assert!(known_failure(|| Ok(())).is_err());
    let infra = anyhow::Error::new(docker::InfraError("daemon gone".into()));
    let reraised = known_failure(|| Err(infra)).expect_err("infra failures re-raise");
    assert!(reraised.is::<docker::InfraError>());
}
