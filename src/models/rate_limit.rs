use serde::{Deserialize, Serialize};
use std::{num::NonZeroU32, sync::Arc};
use tokio::{
    runtime::Handle,
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Duration,
};

/// Represents a rate limit configuration for API requests.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RateLimit {
    /// The maximum number of requests allowed within the specified duration.
    pub max_requests: NonZeroU32,
    /// The duration over which the rate limit is applied.
    ///
    /// [`RateLimiter`] measures this window from when each request
    /// *completes*, not when it starts, so uneven network latency can never
    /// push more than [`max_requests`](Self::max_requests) arrivals into one
    /// window on the server side.
    pub duration: Duration,
}

impl Default for RateLimit {
    fn default() -> Self {
        RateLimit::KEYCDN
    }
}

impl RateLimit {
    /// The rate limit to use for the KeyCDN API: **1 request per 350ms**.
    ///
    /// KeyCDN documents its limit only as "3r/s". Probing the IP Location
    /// Finder API (2026-11-21) showed that this is nginx `limit_req` notation
    /// with a burst of 2, rather than "3 requests per 1-second window":
    ///
    /// - Requests are admitted at a steady one per 333ms.
    /// - Up to 2 requests arriving sooner than that are *queued* by the server
    ///   and released 333ms apart, adding up to ~666ms of latency.
    /// - Any request beyond that has its connection closed without an HTTP
    ///   response. There is no 429 status, `Retry-After` header or error body,
    ///   so it surfaces as a connection error in
    ///   [`Error::Http`](crate::error::Error::Http).
    ///
    /// Allowing 3 requests per second would stay within the limit, but bursts
    /// of 3 would be held by the server for up to ~666ms. Spacing requests
    /// 350ms apart (333ms plus a margin) means they are never queued, and
    /// leaves the burst allowance spare to absorb network jitter. Since
    /// [`RateLimiter`] starts each window when a request completes,
    /// consecutive requests are in fact spaced 350ms plus one request's
    /// duration apart.
    pub const KEYCDN: RateLimit = RateLimit {
        max_requests: NonZeroU32::MIN,
        duration: Duration::from_millis(350),
    };

    /// Creates a new [`RateLimit`] instance with the specified maximum requests and duration.
    pub const fn new(max_requests: NonZeroU32, duration: Duration) -> Self {
        RateLimit {
            max_requests,
            duration,
        }
    }

    /// Creates a new [`RateLimit`] instance with the specified maximum requests per minute.
    pub const fn new_per_minute(max_requests: NonZeroU32) -> Self {
        Self::new(max_requests, Duration::from_secs(60))
    }

    /// Creates a new [`RateLimit`] instance with the specified maximum requests per hour.
    pub const fn new_per_hour(max_requests: NonZeroU32) -> Self {
        Self::new(max_requests, Duration::from_secs(3600))
    }
}

/// A rate limiter that enforces the specified [`RateLimit`] for API requests.
///
/// A single limiter should be shared (e.g. behind an [`Arc`]) by everything
/// that talks to the same API, since the limit applies to the client as a
/// whole.
#[derive(Debug)]
pub struct RateLimiter {
    semaphore: Arc<Semaphore>,
    rate_limit: RateLimit,
}

impl From<RateLimit> for RateLimiter {
    fn from(rate_limit: RateLimit) -> Self {
        RateLimiter {
            semaphore: Arc::new(Semaphore::new(rate_limit.max_requests.get() as usize)),
            rate_limit,
        }
    }
}

impl RateLimiter {
    /// Waits until a request is allowed under the rate limit, and returns a
    /// [`RateLimitPermit`] representing that request's slot.
    ///
    /// Hold the permit while making the request. Once it is dropped (after
    /// the request completes, on panic, or because the future holding it was
    /// cancelled), the slot becomes available again after
    /// [`RateLimit::duration`] has elapsed.
    ///
    /// # Panics
    ///
    /// Panics if not called from within a Tokio runtime.
    pub async fn acquire(&self) -> RateLimitPermit {
        let handle = Handle::current();
        tracing::trace!(
            "Acquiring permit for rate limit: {:?} requests per {:?}",
            self.rate_limit.max_requests,
            self.rate_limit.duration
        );
        let permit = Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .expect("the rate limiter's semaphore is never closed");
        tracing::trace!(
            "Permit acquired for rate limit: {:?} requests per {:?}",
            self.rate_limit.max_requests,
            self.rate_limit.duration
        );
        RateLimitPermit {
            permit: Some(permit),
            delay: self.rate_limit.duration,
            handle,
        }
    }

    /// Waits until a request is allowed under the rate limit, then executes
    /// the provided future and returns its output.
    ///
    /// The output is returned as soon as `f` completes; the slot is released
    /// [`RateLimit::duration`] after that in the background. If this future is
    /// cancelled while `f` is running, the slot is still held for the full
    /// duration, since the request may already have reached the server.
    ///
    /// # Panics
    ///
    /// Panics if not called from within a Tokio runtime.
    pub async fn acquire_and_execute<F, T>(&self, f: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        let _permit = self.acquire().await;
        f.await
    }
}

/// A slot under a [`RateLimiter`], obtained from [`RateLimiter::acquire`].
///
/// Dropping the permit schedules the slot to be released after
/// [`RateLimit::duration`]; it does not release it immediately.
#[derive(Debug)]
#[must_use = "dropping the permit starts its release timer immediately; bind it with `let _permit = ...` until the request completes"]
pub struct RateLimitPermit {
    permit: Option<OwnedSemaphorePermit>,
    delay: Duration,
    /// Captured at acquisition so that the release can be scheduled even if
    /// the permit is dropped outside of the runtime's context.
    handle: Handle,
}

impl Drop for RateLimitPermit {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            let delay = self.delay;
            // If the runtime has shut down, the task never runs and the
            // semaphore permit is simply dropped along with it, which is fine
            // since nothing can be waiting on the limiter any more.
            self.handle.spawn(async move {
                tokio::time::sleep(delay).await;
                drop(permit);
                tracing::trace!("Rate limit duration {:?} elapsed, permit released", delay);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    //! All async tests run on Tokio's paused clock (`start_paused = true`):
    //! time only advances when every task is idle, and jumps straight to the
    //! next timer. Durations are therefore exact and the tests take no real
    //! time, regardless of how loaded the machine is.

    use super::*;
    use crate::logging;
    use futures::future::join_all;
    use tokio::time::Instant;

    const WINDOW: Duration = Duration::from_millis(100);

    fn limiter(max_requests: u32) -> Arc<RateLimiter> {
        let max_requests = NonZeroU32::new(max_requests).unwrap();
        Arc::new(RateLimiter::from(RateLimit::new(max_requests, WINDOW)))
    }

    /// Submits `num_requests` requests at once, each taking `task_duration`,
    /// and returns how long it took until every caller had its result.
    async fn run_concurrent(
        rate_limiter: &Arc<RateLimiter>,
        num_requests: usize,
        task_duration: Duration,
    ) -> Duration {
        let start = Instant::now();
        let handles = (0..num_requests).map(|_| {
            let rate_limiter = Arc::clone(rate_limiter);
            tokio::spawn(async move {
                // `sleep` fixes its deadline when created, so it must be created
                // inside the async block, i.e. only once the limiter lets it run.
                rate_limiter
                    .acquire_and_execute(async move { tokio::time::sleep(task_duration).await })
                    .await
            })
        });
        for result in join_all(handles).await {
            result.expect("task panicked");
        }
        start.elapsed()
    }

    /// Returns the largest number of `times` that fall within any half-open
    /// interval `[t, t + window)`.
    fn max_in_any_window(mut times: Vec<Duration>, window: Duration) -> usize {
        times.sort();
        let mut max = 0;
        let mut lo = 0;
        for hi in 0..times.len() {
            while times[hi] - times[lo] >= window {
                lo += 1;
            }
            max = max.max(hi - lo + 1);
        }
        max
    }

    #[test]
    fn max_in_any_window_counts_half_open_windows() {
        let ms = |v: &[u64]| v.iter().copied().map(Duration::from_millis).collect();
        assert_eq!(max_in_any_window(ms(&[]), WINDOW), 0);
        assert_eq!(max_in_any_window(ms(&[0, 0, 0]), WINDOW), 3);
        // 100ms apart is not within the same 100ms window.
        assert_eq!(max_in_any_window(ms(&[0, 100, 200]), WINDOW), 1);
        assert_eq!(max_in_any_window(ms(&[0, 99, 100, 150, 199]), WINDOW), 3);
        // Order of input does not matter.
        assert_eq!(max_in_any_window(ms(&[300, 0, 250, 50]), WINDOW), 2);
    }

    /// The core guarantee: no matter how long requests take or when they are
    /// submitted, no window of length [`RateLimit::duration`] ever contains
    /// more than [`RateLimit::max_requests`] request starts or completions.
    ///
    /// Since the server sees each request at some point between its start and
    /// its completion, bounding both bounds what the server sees.
    #[tokio::test(start_paused = true)]
    async fn no_window_exceeds_the_limit() {
        logging::init_test_tracing();

        const MAX_REQUESTS: u32 = 3;
        const NUM_REQUESTS: u32 = 60;
        // Zero-length, shorter than, equal to, and longer than the window.
        const TASK_DURATIONS_MS: [u64; 7] = [0, 10, 50, 99, 100, 101, 250];
        // Staggered so requests don't move in lockstep batches.
        const SUBMIT_INTERVAL: Duration = Duration::from_millis(7);

        let rate_limiter = limiter(MAX_REQUESTS);
        let start = Instant::now();
        let handles = (0..NUM_REQUESTS).map(|i| {
            let rate_limiter = Arc::clone(&rate_limiter);
            let task_duration =
                Duration::from_millis(TASK_DURATIONS_MS[i as usize % TASK_DURATIONS_MS.len()]);
            tokio::spawn(async move {
                tokio::time::sleep(SUBMIT_INTERVAL * i).await;
                rate_limiter
                    .acquire_and_execute(async {
                        let started = start.elapsed();
                        tokio::time::sleep(task_duration).await;
                        (started, start.elapsed())
                    })
                    .await
            })
        });
        let (starts, ends): (Vec<_>, Vec<_>) = join_all(handles)
            .await
            .into_iter()
            .map(|result| result.expect("task panicked"))
            .unzip();

        let max_starts = max_in_any_window(starts, WINDOW);
        let max_ends = max_in_any_window(ends, WINDOW);
        tracing::info!(
            "{NUM_REQUESTS} requests took {:?}; max starts per window: {max_starts}, max completions per window: {max_ends}",
            start.elapsed()
        );

        // Requests are submitted faster than the limit allows, so the limiter
        // must be saturated; this also proves the test exercises contention.
        assert_eq!(
            max_starts, MAX_REQUESTS as usize,
            "request starts per window"
        );
        assert!(
            max_ends <= MAX_REQUESTS as usize,
            "{max_ends} request completions within one window, limit is {MAX_REQUESTS}"
        );
    }

    mod latency {
        //! The limiter must not delay callers more than necessary.

        use super::*;

        #[tokio::test(start_paused = true)]
        async fn uncontended_acquire_is_immediate() {
            logging::init_test_tracing();

            let rate_limiter = limiter(3);
            let start = Instant::now();
            let _permit = rate_limiter.acquire().await;
            assert_eq!(start.elapsed(), Duration::ZERO);
        }

        #[tokio::test(start_paused = true)]
        async fn result_is_returned_when_task_completes() {
            logging::init_test_tracing();

            // All 3 run at once and return after 50ms; the callers do not wait
            // for their slots to be released at 150ms.
            let elapsed = run_concurrent(&limiter(3), 3, Duration::from_millis(50)).await;
            assert_eq!(elapsed, Duration::from_millis(50));
        }
    }

    mod window_start {
        //! The window for each request starts when that request completes, so
        //! a slot is reused no sooner than (task duration + window) after it
        //! was taken. With 3 slots and N requests submitted at once, the last
        //! batch starts at (ceil(N / 3) - 1) * (T + D) and ends T later.

        use super::*;

        #[tokio::test(start_paused = true)]
        async fn fourth_request_waits_for_window_after_completion() {
            logging::init_test_tracing();

            // The 4th request starts at 50 + 100 = 150ms and ends at 200ms.
            let elapsed = run_concurrent(&limiter(3), 4, Duration::from_millis(50)).await;
            assert_eq!(elapsed, Duration::from_millis(200));
        }

        #[tokio::test(start_paused = true)]
        async fn short_tasks() {
            logging::init_test_tracing();

            // 5 batches: the last starts at 4 * 150ms = 600ms and ends at 650ms.
            // If the window started when the request starts, this would be
            // 4 * 100ms + 50ms = 450ms.
            let elapsed = run_concurrent(&limiter(3), 14, Duration::from_millis(50)).await;
            assert_eq!(elapsed, Duration::from_millis(650));
        }

        #[tokio::test(start_paused = true)]
        async fn tasks_longer_than_window() {
            logging::init_test_tracing();

            // 5 batches: the last starts at 4 * 250ms = 1000ms and ends at 1150ms.
            let elapsed = run_concurrent(&limiter(3), 14, Duration::from_millis(150)).await;
            assert_eq!(elapsed, Duration::from_millis(1150));
        }
    }

    mod release {
        //! Slots must be held for the full window however the permit is dropped.

        use super::*;

        #[tokio::test(start_paused = true)]
        async fn cancelled_request_holds_slot_for_full_window() {
            logging::init_test_tracing();

            let rate_limiter = limiter(1);
            let start = Instant::now();

            // The request may already have reached the server when it is
            // cancelled, so its slot must still be held for the full window.
            let cancelled = tokio::time::timeout(
                Duration::from_millis(10),
                rate_limiter.acquire_and_execute(std::future::pending::<()>()),
            )
            .await;
            assert!(cancelled.is_err(), "request should have timed out");

            let _permit = rate_limiter.acquire().await;
            assert_eq!(start.elapsed(), Duration::from_millis(10) + WINDOW);
        }

        /// A permit dropped on a thread without a Tokio runtime must neither
        /// panic nor release its slot early.
        #[test]
        fn permit_dropped_outside_runtime() {
            logging::init_test_tracing();

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .unwrap();
            let rate_limiter = limiter(1);

            let (permit, start) = runtime.block_on(async {
                let permit = rate_limiter.acquire().await;
                (permit, Instant::now())
            });
            std::thread::spawn(move || drop(permit))
                .join()
                .expect("dropping the permit outside the runtime panicked");

            let elapsed = runtime.block_on(async {
                let _permit = rate_limiter.acquire().await;
                start.elapsed()
            });
            assert_eq!(elapsed, WINDOW);
        }
    }
}
