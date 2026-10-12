use std::collections::VecDeque;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::Purpose;

const SCALE: u64 = 1_000_000;
const RANKS: usize = 5;

struct Waiter {
    id: u64,
    tx: oneshot::Sender<()>,
}

pub(crate) enum AcquireFail {
    Daily,
    Throttled,
}

pub(crate) struct Budget {
    balance: u64,
    burst: u64,
    rps: u32,
    updated: Instant,
    not_before: Instant,
    daily_limit: Option<u64>,
    daily_used: u64,
    daily_day: u64,
    next_id: u64,
    waiters: [VecDeque<Waiter>; RANKS],
    refill_armed: bool,
}

pub(crate) struct Parked {
    pub(crate) id: u64,
    pub(crate) rx: oneshot::Receiver<()>,
    pub(crate) deadline: Option<Instant>,
}

impl Budget {
    pub(crate) fn new(rps: u32, burst: u32, daily_limit: Option<u64>, now: Instant) -> Self {
        Self {
            balance: u64::from(burst) * SCALE,
            burst: u64::from(burst) * SCALE,
            rps,
            updated: now,
            not_before: now,
            daily_limit,
            daily_used: 0,
            daily_day: 0,
            next_id: 1,
            waiters: std::array::from_fn(|_| VecDeque::new()),
            refill_armed: false,
        }
    }

    pub(crate) fn tokens(&self) -> f64 {
        self.balance as f64 / SCALE as f64
    }

    pub(crate) fn hold_until(&mut self, until: Instant) {
        if until > self.not_before {
            self.not_before = until;
        }
    }

    pub(crate) fn hold_delay(&self, now: Instant) -> Option<Duration> {
        let delay = self.not_before.saturating_duration_since(now);
        if delay.is_zero() { None } else { Some(delay) }
    }

    pub(crate) fn refill(&mut self, now: Instant) {
        if self.balance >= self.burst {
            self.updated = now;
            return;
        }
        let elapsed_ns = now.saturating_duration_since(self.updated).as_nanos();
        if elapsed_ns == 0 {
            return;
        }
        let earned = elapsed_ns * u128::from(self.rps) * u128::from(SCALE) / 1_000_000_000;
        if earned == 0 {
            return;
        }
        let room = u128::from(self.burst - self.balance);
        let add = earned.min(room);
        self.balance += u64::try_from(add).unwrap_or(0);
        if self.balance >= self.burst {
            self.balance = self.burst;
            self.updated = now;
        } else {
            let consumed_ns = add * 1_000_000_000 / (u128::from(self.rps) * u128::from(SCALE));
            self.updated += Duration::from_nanos(u64::try_from(consumed_ns).unwrap_or(0));
        }
    }

    /// `Ok(None)` means a token was taken. `Ok(Some)` means the caller is queued.
    pub(crate) fn try_acquire(
        &mut self,
        purpose: Purpose,
        deadline: Option<Instant>,
        now: Instant,
        today: u64,
        woken: bool,
    ) -> Result<Option<Parked>, AcquireFail> {
        self.roll_day(today);
        if self.daily_exhausted() {
            return Err(AcquireFail::Daily);
        }
        let higher = self.has_higher(purpose);
        let same = self.has_same(purpose);
        if higher && self.balance >= SCALE {
            self.wake_highest();
        }
        if self.balance >= SCALE && !higher && (woken || !same) {
            self.balance -= SCALE;
            self.daily_used += 1;
            if self.balance >= SCALE {
                self.wake_highest();
            }
            return Ok(None);
        }
        if deadline.is_some_and(|deadline| now >= deadline) {
            return Err(AcquireFail::Throttled);
        }
        Ok(Some(self.enqueue(purpose, deadline)))
    }

    /// `true` when this id was still queued.
    pub(crate) fn remove(&mut self, purpose: Purpose, id: u64) -> bool {
        let queue = &mut self.waiters[purpose.rank()];
        let before = queue.len();
        queue.retain(|waiter| waiter.id != id);
        queue.len() != before
    }

    /// Keeps a timer running for whoever is still queued. The token that woke
    /// the first waiter may be gone before the next waiter is polled.
    pub(crate) fn arm_delay(&mut self) -> Option<Duration> {
        if self.refill_armed || !self.has_waiters() {
            return None;
        }
        self.refill_armed = true;
        Some(self.delay_until_token())
    }

    pub(crate) fn refill_fired(&mut self, now: Instant) -> Option<Duration> {
        self.refill_armed = false;
        self.refill(now);
        self.wake_highest();
        self.arm_delay()
    }

    pub(crate) fn wake_highest(&mut self) {
        for queue in &mut self.waiters {
            while let Some(waiter) = queue.pop_front() {
                if waiter.tx.send(()).is_ok() {
                    return;
                }
            }
        }
    }

    fn roll_day(&mut self, today: u64) {
        if self.daily_day != today {
            self.daily_day = today;
            self.daily_used = 0;
        }
    }

    fn daily_exhausted(&self) -> bool {
        self.daily_limit
            .is_some_and(|limit| self.daily_used >= limit)
    }

    fn has_higher(&self, purpose: Purpose) -> bool {
        self.waiters[..purpose.rank()]
            .iter()
            .any(|queue| !queue.is_empty())
    }

    fn has_same(&self, purpose: Purpose) -> bool {
        !self.waiters[purpose.rank()].is_empty()
    }

    fn has_waiters(&self) -> bool {
        self.waiters.iter().any(|queue| !queue.is_empty())
    }

    fn enqueue(&mut self, purpose: Purpose, deadline: Option<Instant>) -> Parked {
        let (tx, rx) = oneshot::channel();
        let id = self.next_id;
        self.next_id += 1;
        self.waiters[purpose.rank()].push_back(Waiter { id, tx });
        Parked { id, rx, deadline }
    }

    fn delay_until_token(&self) -> Duration {
        let need = if self.balance >= SCALE.saturating_mul(2) {
            return Duration::from_millis(1);
        } else if self.balance >= SCALE {
            SCALE
        } else {
            SCALE - self.balance
        };
        let ns = u128::from(need) * 1_000_000_000 / (u128::from(self.rps) * u128::from(SCALE));
        Duration::from_nanos(u64::try_from(ns.max(1_000_000)).unwrap_or(u64::MAX))
    }
}
