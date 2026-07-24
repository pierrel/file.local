use std::fmt;
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const MAX_WATCH_ERROR_CHARS: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundedError(String);

impl BoundedError {
    pub fn new(error: impl fmt::Display) -> Self {
        Self(
            error
                .to_string()
                .escape_default()
                .take(MAX_WATCH_ERROR_CHARS)
                .collect(),
        )
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BoundedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug)]
pub struct WatchState {
    inner: Arc<Mutex<WatchStatus>>,
}

#[derive(Debug, Default)]
struct WatchStatus {
    generation: u64,
    lost: Option<BoundedError>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WatchSnapshot {
    Healthy { generation: u64 },
    Lost(BoundedError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Completion {
    Current,
    Pending,
}

impl Default for WatchState {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(WatchStatus::default())),
        }
    }
}

impl WatchState {
    pub fn snapshot(&self) -> WatchSnapshot {
        let state = self.lock();
        match &state.lost {
            Some(error) => WatchSnapshot::Lost(error.clone()),
            None => WatchSnapshot::Healthy {
                generation: state.generation,
            },
        }
    }

    /// Records a filesystem event before attempting the best-effort wake.
    /// A saturated wake channel is safe because `snapshot` remains authoritative.
    pub fn changed<T>(&self, wake: &SyncSender<T>, event: T) {
        let mut state = self.lock();
        if state.lost.is_none() {
            match state.generation.checked_add(1) {
                Some(generation) => state.generation = generation,
                None => {
                    state.lost = Some(BoundedError::new("watch generation exhausted"));
                }
            }
        }
        drop(state);
        best_effort_wake(wake, event);
    }

    /// Stores watcher loss before waking, so channel saturation cannot erase it.
    pub fn lost<T>(&self, error: impl fmt::Display, wake: &SyncSender<T>, event: T) {
        let mut state = self.lock();
        if state.lost.is_none() {
            state.lost = Some(BoundedError::new(error));
        }
        drop(state);
        best_effort_wake(wake, event);
    }

    /// Atomically validates watcher health and advances coordinator-owned
    /// completion only through the frozen generation.
    pub fn complete(
        &self,
        frozen_generation: u64,
        completed_generation: &mut u64,
    ) -> Result<Completion, BoundedError> {
        let state = self.lock();
        if let Some(error) = &state.lost {
            return Err(error.clone());
        }
        *completed_generation = (*completed_generation).max(frozen_generation);
        Ok(if state.generation > *completed_generation {
            Completion::Pending
        } else {
            Completion::Current
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, WatchStatus> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

pub fn wake_channel<T>() -> (SyncSender<T>, Receiver<T>) {
    sync_channel(1)
}

fn best_effort_wake<T>(wake: &SyncSender<T>, event: T) {
    match wake.try_send(event) {
        Ok(()) | Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {}
    }
}

#[derive(Clone, Copy, Debug)]
pub struct WatchConfig {
    pub debounce: Duration,
    pub max_burst: Duration,
    pub heartbeat: Duration,
    pub pong_timeout: Duration,
    pub safety_audit: Duration,
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            debounce: Duration::from_millis(250),
            max_burst: Duration::from_secs(2),
            heartbeat: Duration::from_secs(30),
            pong_timeout: Duration::from_secs(30),
            safety_audit: Duration::from_secs(15 * 60),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Debounce {
    first: Option<Instant>,
    latest: Option<Instant>,
}

impl Debounce {
    pub fn notify(&mut self, now: Instant) {
        self.first.get_or_insert(now);
        self.latest = Some(now);
    }

    pub fn deadline(&self, config: &WatchConfig) -> Option<Instant> {
        Some(std::cmp::min(
            self.latest?.checked_add(config.debounce)?,
            self.first?.checked_add(config.max_burst)?,
        ))
    }

    pub fn take_due(&mut self, now: Instant, config: &WatchConfig) -> bool {
        if self
            .deadline(config)
            .is_some_and(|deadline| now >= deadline)
        {
            *self = Self::default();
            true
        } else {
            false
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct AuditSchedule {
    last_full_round: Instant,
}

impl AuditSchedule {
    pub fn new(now: Instant) -> Self {
        Self {
            last_full_round: now,
        }
    }

    pub fn deadline(&self, config: &WatchConfig) -> Option<Instant> {
        self.last_full_round.checked_add(config.safety_audit)
    }

    pub fn is_due(&self, now: Instant, config: &WatchConfig) -> bool {
        self.deadline(config).is_none_or(|deadline| now >= deadline)
    }

    pub fn completed_full_round(&mut self, now: Instant) {
        self.last_full_round = now;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeartbeatAction {
    Ping { nonce: u64 },
    TimedOut,
}

#[derive(Clone, Copy, Debug)]
pub struct Heartbeat {
    last_activity: Instant,
    awaiting: Option<(u64, Instant)>,
    next_nonce: u64,
}

impl Heartbeat {
    pub fn new(now: Instant) -> Self {
        Self {
            last_activity: now,
            awaiting: None,
            next_nonce: 1,
        }
    }

    pub fn deadline(&self, config: &WatchConfig) -> Option<Instant> {
        match self.awaiting {
            Some((_, deadline)) => Some(deadline),
            None => self.last_activity.checked_add(config.heartbeat),
        }
    }

    pub fn due(&mut self, now: Instant, config: &WatchConfig) -> Option<HeartbeatAction> {
        if let Some((_, deadline)) = self.awaiting {
            return (now >= deadline).then_some(HeartbeatAction::TimedOut);
        }
        if now < self.deadline(config)? {
            return None;
        }
        let nonce = self.next_nonce;
        let Some(next_nonce) = nonce.checked_add(1) else {
            return Some(HeartbeatAction::TimedOut);
        };
        let Some(deadline) = now.checked_add(config.pong_timeout) else {
            return Some(HeartbeatAction::TimedOut);
        };
        self.next_nonce = next_nonce;
        self.awaiting = Some((nonce, deadline));
        Some(HeartbeatAction::Ping { nonce })
    }

    pub fn pong(&mut self, nonce: u64, now: Instant) -> bool {
        if !matches!(self.awaiting, Some((expected, _)) if expected == nonce) {
            return false;
        }
        self.awaiting = None;
        self.last_activity = now;
        true
    }

    /// Any validated round traffic proves liveness and restarts the idle clock.
    pub fn activity(&mut self, now: Instant) {
        self.awaiting = None;
        self.last_activity = now;
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    delays: [Duration; 7],
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            delays: [1, 2, 4, 8, 16, 30, 60].map(Duration::from_secs),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RetryBackoff {
    failures: usize,
}

impl RetryBackoff {
    /// Returns the next delay with caller-supplied deterministic jitter in
    /// basis points, constrained to the design's +/-20 percent range.
    pub fn failed(&mut self, policy: &RetryPolicy, jitter_basis_points: i16) -> Duration {
        let index = self.failures.min(policy.delays.len() - 1);
        self.failures = self.failures.saturating_add(1);
        apply_jitter(
            policy.delays[index],
            jitter_basis_points.clamp(-2_000, 2_000),
        )
    }

    pub fn startup_round_succeeded(&mut self) {
        self.failures = 0;
    }
}

fn apply_jitter(delay: Duration, basis_points: i16) -> Duration {
    let nanos = delay.as_nanos();
    let magnitude = nanos.saturating_mul(basis_points.unsigned_abs() as u128) / 10_000;
    let adjusted = if basis_points.is_negative() {
        nanos.saturating_sub(magnitude)
    } else {
        nanos.saturating_add(magnitude)
    };
    Duration::from_nanos(adjusted.min(u64::MAX as u128) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saturated_wake_preserves_generation_and_loss() {
        let state = WatchState::default();
        let (wake, receiver) = wake_channel();
        state.changed(&wake, ());
        state.changed(&wake, ());
        assert_eq!(state.snapshot(), WatchSnapshot::Healthy { generation: 2 });
        state.lost("backend\nloss", &wake, ());
        assert_eq!(
            state.snapshot(),
            WatchSnapshot::Lost(BoundedError::new("backend\nloss"))
        );
        assert_eq!(receiver.try_iter().count(), 1);
    }

    #[test]
    fn completion_cannot_erase_racing_change_or_loss() {
        let state = WatchState::default();
        let (wake, _receiver) = wake_channel();
        state.changed(&wake, ());
        let WatchSnapshot::Healthy { generation: frozen } = state.snapshot() else {
            panic!("watch unexpectedly lost");
        };
        state.changed(&wake, ());
        let mut completed = 0;
        assert_eq!(
            state.complete(frozen, &mut completed).unwrap(),
            Completion::Pending
        );
        assert_eq!(completed, frozen);
        state.lost("overflow", &wake, ());
        assert_eq!(
            state.complete(2, &mut completed),
            Err(BoundedError::new("overflow"))
        );
        assert_eq!(completed, frozen);
    }

    #[test]
    fn bounded_errors_are_single_line_and_bounded() {
        let error = BoundedError::new(format!("{}\n", "é".repeat(5_000)));
        assert_eq!(error.as_str().chars().count(), MAX_WATCH_ERROR_CHARS);
        assert!(error.as_str().is_ascii());
        assert!(!error.as_str().contains(['\n', '\r']));
    }

    #[test]
    fn debounce_uses_latest_event_but_caps_continuous_burst() {
        let start = Instant::now();
        let config = WatchConfig::default();
        let mut debounce = Debounce::default();
        debounce.notify(start);
        debounce.notify(start + Duration::from_millis(200));
        assert_eq!(
            debounce.deadline(&config),
            Some(start + Duration::from_millis(450))
        );
        debounce.notify(start + Duration::from_secs(3));
        assert_eq!(debounce.deadline(&config), Some(start + config.max_burst));
        assert!(debounce.take_due(start + config.max_burst, &config));
        assert!(debounce.deadline(&config).is_none());
    }

    #[test]
    fn audit_is_measured_from_last_completed_full_round() {
        let start = Instant::now();
        let config = WatchConfig::default();
        let mut audit = AuditSchedule::new(start);
        assert!(!audit.is_due(
            start + config.safety_audit - Duration::from_nanos(1),
            &config
        ));
        assert!(audit.is_due(start + config.safety_audit, &config));
        audit.completed_full_round(start + config.safety_audit);
        assert!(!audit.is_due(start + config.safety_audit, &config));
    }

    #[test]
    fn heartbeat_requires_matching_pong_and_traffic_proves_liveness() {
        let start = Instant::now();
        let config = WatchConfig::default();
        let mut heartbeat = Heartbeat::new(start);
        assert_eq!(
            heartbeat.due(start + config.heartbeat, &config),
            Some(HeartbeatAction::Ping { nonce: 1 })
        );
        assert!(!heartbeat.pong(2, start + config.heartbeat));
        assert_eq!(
            heartbeat.due(start + config.heartbeat + config.pong_timeout, &config),
            Some(HeartbeatAction::TimedOut)
        );
        heartbeat.activity(start + Duration::from_secs(100));
        assert!(
            heartbeat
                .due(
                    start + Duration::from_secs(100) + config.heartbeat - Duration::from_nanos(1),
                    &config
                )
                .is_none()
        );
    }

    #[test]
    fn retry_sequence_is_capped_jittered_and_reset_only_explicitly() {
        let policy = RetryPolicy::default();
        let mut retry = RetryBackoff::default();
        let observed: Vec<_> = (0..8).map(|_| retry.failed(&policy, 0)).collect();
        assert_eq!(
            observed,
            [1, 2, 4, 8, 16, 30, 60, 60].map(Duration::from_secs)
        );
        assert_eq!(retry.failed(&policy, -2_000), Duration::from_secs(48));
        assert_eq!(retry.failed(&policy, 2_000), Duration::from_secs(72));
        retry.startup_round_succeeded();
        assert_eq!(retry.failed(&policy, 0), Duration::from_secs(1));
    }
}
