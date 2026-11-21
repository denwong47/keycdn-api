use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::{sync::Semaphore, time::Duration};

/// Represents a rate limit configuration for API requests.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RateLimit {
    /// The maximum number of requests allowed within the specified duration.
    pub max_requests: u32,
    /// The duration over which the rate limit is applied.
    pub duration: Duration,
}

impl Default for RateLimit {
    fn default() -> Self {
        RateLimit::FREE_TIER
    }
}

impl RateLimit {
    /// A predefined rate limit for the free tier of the KeyCDN API.
    pub const FREE_TIER: RateLimit = RateLimit {
        max_requests: 3,
        duration: Duration::from_secs(60),
    };

    /// Creates a new [`RateLimit`] instance with the specified maximum requests and duration.
    pub const fn new(max_requests: u32, duration: Duration) -> Self {
        RateLimit {
            max_requests,
            duration,
        }
    }

    /// Creates a new [`RateLimit`] instance with the specified maximum requests per minute.
    pub const fn new_per_minute(max_requests: u32) -> Self {
        Self::new(max_requests, Duration::from_secs(60))
    }

    /// Creates a new [`RateLimit`] instance with the specified maximum requests per hour.
    pub const fn new_per_hour(max_requests: u32) -> Self {
        Self::new(max_requests, Duration::from_secs(3600))
    }
}

/// A rate limiter that enforces the specified [`RateLimit`] for API requests.
#[derive(Debug)]
pub struct RateLimiter {
    semaphore: Semaphore,
    rate_limit: RateLimit,
}

impl From<RateLimit> for RateLimiter {
    fn from(rate_limit: RateLimit) -> Self {
        if rate_limit.max_requests == 0 {
            // To reduce the number of dependencies, we don't use `anyhow` here.
            // `RateLimit` should be static, so this panic should only occur
            // during development if the `RateLimit` is misconfigured.
            panic!("RateLimit max_requests must be greater than 0");
        }
        RateLimiter {
            semaphore: Semaphore::new(rate_limit.max_requests as usize),
            rate_limit,
        }
    }
}

impl RateLimiter {
    /// Acquires a permit from the rate limiter, blocking until one is available.
    ///
    /// Internally, this simply acquires a permit from the semaphore and then
    /// sleeps for the duration of the rate limit; since the Semaphore is
    /// initialized with the maximum number of requests, this effectively
    /// enforces the rate limit.
    pub async fn acquire(&self) {
        let _permit = self.semaphore.acquire().await.unwrap();
        tokio::time::sleep(self.rate_limit.duration).await;
    }

    /// Acquires a permit from the rate limiter, executes the provided future,
    /// and then releases the permit after the rate limit duration has passed.
    ///
    /// The future `f` can be any asynchronous operation that returns a value of
    /// type `T`.  It does not need to execute within the rate limit duration;
    /// the permit will be held for the duration of the rate limit regardless of
    /// how long `f` takes to complete.
    pub async fn acquire_and_execute<F, T>(&self, f: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        let task_finished = Arc::new(tokio::sync::Notify::new());
        tracing::trace!(
            "Acquiring permit for rate limit: {:?} requests per {:?}",
            self.rate_limit.max_requests,
            self.rate_limit.duration
        );
        let permit = self.semaphore.acquire().await.unwrap();
        tracing::trace!(
            "Permit acquired for rate limit: {:?} requests per {:?}",
            self.rate_limit.max_requests,
            self.rate_limit.duration
        );

        let future_notify = Arc::clone(&task_finished);
        let timeout_notify = task_finished;

        let (result, _) = tokio::join!(
            async move {
                let result = f.await;
                future_notify.notify_waiters();
                result
            },
            async move {
                tokio::select! {
                    _ = timeout_notify.notified() => {
                        tracing::trace!("Task finished before rate limit duration elapsed");
                    }
                    _ = tokio::time::sleep(self.rate_limit.duration) => {
                        tracing::trace!("Rate limit duration {:?} elapsed before task finished", self.rate_limit.duration);
                    }
                }
                drop(permit); // Release the permit after the sleep duration
            }
        );
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging;

    mod test_simple {
        use super::*;
        macro_rules! create_test {
            (
                $test_name:ident({
                    max_requests: $max_requests:literal,
                    num_requests: $num_requests:literal,
                    min_time_factor: $min_time_factor:literal,
                    max_time_factor: $max_time_factor:literal
                })
            ) => {
                #[tokio::test]
                async fn $test_name() {
                    logging::init_test_tracing();

                    let max_requests: u32 = $max_requests;
                    let num_requests: u32 = $num_requests;
                    let min_time_factor: f32 = $min_time_factor;
                    let max_time_factor: f32 = $max_time_factor;

                    let duration: Duration = Duration::from_millis(100);

                    let rate_limit = RateLimit::new(max_requests, duration);
                    let rate_limiter = Arc::new(RateLimiter::from(rate_limit));

                    let mut handles = Vec::new();
                    for _ in 0..num_requests {
                        let rl_ref = Arc::clone(&rate_limiter);
                        handles.push(tokio::spawn(async move {
                            rl_ref.acquire().await;
                        }));
                    }

                    let start = std::time::Instant::now();
                    let _ = futures::future::join_all(handles).await;
                    let elapsed = start.elapsed();

                    let expected_min_duration = duration.mul_f32(min_time_factor);
                    let expected_max_duration = duration.mul_f32(max_time_factor);

                    tracing::info!(
                        "Elapsed time for {} requests with a limit of {} requests per {:.2}s: {:.2}s, (min: {:.2}s, max: {:.2}s)",
                        num_requests,
                        max_requests,
                        duration.as_secs_f32(),
                        elapsed.as_secs_f32(),
                        expected_min_duration.as_secs_f32(),
                        expected_max_duration.as_secs_f32()
                    );

                    assert!(
                        elapsed >= expected_min_duration,
                        "Elapsed time {:?} is less than expected minimum duration {:?}",
                        elapsed,
                        expected_min_duration
                    );
                    assert!(
                        elapsed <= expected_max_duration,
                        "Elapsed time {:?} is greater than expected maximum duration {:?}",
                        elapsed,
                        expected_max_duration
                    );
                }
            };
        }

        create_test!(single_item_single_cycle({
            max_requests: 3,
            num_requests: 1,
            min_time_factor: 0.,
            max_time_factor: 1.1
        }));
        create_test!(multiple_items_single_cycle({
            max_requests: 3,
            num_requests: 3,
            min_time_factor: 0.,
            max_time_factor: 1.1
        }));
        create_test!(multiple_items_two_cycle_generous({
            max_requests: 3,
            num_requests: 4,
            min_time_factor: 1.,
            max_time_factor: 2.2
        }));
        create_test!(multiple_items_two_cycle_tight({
            max_requests: 3,
            num_requests: 6,
            min_time_factor: 1.,
            max_time_factor: 2.2
        }));
        create_test!(multiple_items_five_cycle_tight({
            max_requests: 3,
            num_requests: 14,
            min_time_factor: 4.,
            max_time_factor: 5.9
        }));
    }

    mod test_task {
        use super::*;

        async fn dummy_task(duration: Duration) -> &'static str {
            tracing::trace!("Dummy task started, sleeping for {:?}", duration);
            tokio::time::sleep(duration).await;
            tracing::trace!("Dummy task completed after sleeping for {:?}", duration);
            "task completed"
        }

        macro_rules! create_test {
            (
                $test_name:ident({
                    max_requests: $max_requests:literal,
                    num_requests: $num_requests:literal,
                    min_time_factor: $min_time_factor:literal,
                    max_time_factor: $max_time_factor:literal,
                    task_duration: $task_duration:expr
                })
            ) => {
                #[tokio::test]
                async fn $test_name() {
                    logging::init_test_tracing();

                    let max_requests: u32 = $max_requests;
                    let num_requests: u32 = $num_requests;
                    let min_time_factor: f32 = $min_time_factor;
                    let max_time_factor: f32 = $max_time_factor;
                    let task_duration: Duration = $task_duration;

                    let duration: Duration = Duration::from_millis(100);

                    let rate_limit = RateLimit::new(max_requests, duration);
                    let rate_limiter = Arc::new(RateLimiter::from(rate_limit));

                    let start = std::time::Instant::now();
                    let mut handles = Vec::new();
                    for _ in 0..num_requests {
                        let rl_ref = Arc::clone(&rate_limiter);
                        handles.push(tokio::spawn(async move {
                            rl_ref.acquire_and_execute(dummy_task(task_duration)).await
                        }));
                    }
                    let _ = futures::future::join_all(handles).await;
                    let elapsed = start.elapsed();

                    let expected_min_duration = duration.mul_f32(min_time_factor);
                    let expected_max_duration = duration.mul_f32(max_time_factor);

                    tracing::info!(
                        "Elapsed time for {} task(s) with a limit of {} requests per {:.2}s: {:.2}s, (min: {:.2}s, max: {:.2}s)",
                        num_requests,
                        max_requests,
                        duration.as_secs_f32(),
                        elapsed.as_secs_f32(),
                        expected_min_duration.as_secs_f32(),
                        expected_max_duration.as_secs_f32()
                    );

                    assert!(
                        elapsed >= expected_min_duration,
                        "Elapsed time {:?} is less than expected minimum duration {:?}",
                        elapsed,
                        expected_min_duration
                    );
                    assert!(
                        elapsed <= expected_max_duration,
                        "Elapsed time {:?} is greater than expected maximum duration {:?}",
                        elapsed,
                        expected_max_duration
                    );
                }
            };
        }

        create_test!(single_task_single_cycle({
            max_requests: 3,
            num_requests: 1,
            min_time_factor: 0.,
            // This MUST be shorter than 1 because our task duration is shorter
            // than the rate limit duration, so it should print "Task finished
            // before rate limit duration elapsed" and not wait the full
            // duration.
            max_time_factor: 0.9,
            task_duration: Duration::from_millis(1)
        }));
        create_test!(multiple_tasks_single_cycle({
            max_requests: 3,
            num_requests: 3,
            min_time_factor: 0.,
            // This MUST be shorter than 1 because our task duration is shorter
            // than the rate limit duration, so it should print "Task finished
            // before rate limit duration elapsed" and not wait the full
            // duration.
            max_time_factor: 0.9,
            // Task duration is now long enough not to allow all 3 tasks to run
            // sequentially, so this test should prove that the concurrency is
            // working correctly.
            task_duration: Duration::from_millis(50)
        }));
        create_test!(multiple_tasks_two_cycle_generous({
            max_requests: 3,
            num_requests: 4,
            min_time_factor: 1.,
            // Again, this MUST be shorter than 2 because our task duration is shorter
            // than the rate limit duration, so it should print "Task finished
            // before rate limit duration elapsed" and not wait the full
            // duration.
            max_time_factor: 1.9,
            task_duration: Duration::from_millis(50)
        }));
        create_test!(multiple_tasks_two_cycle_tight({
            max_requests: 3,
            num_requests: 6,
            min_time_factor: 1.,
            // Again, this MUST be shorter than 2 because our task duration is shorter
            // than the rate limit duration, so it should print "Task finished
            // before rate limit duration elapsed" and not wait the full
            // duration.
            max_time_factor: 1.9,
            task_duration: Duration::from_millis(50)
        }));
        create_test!(multiple_tasks_five_cycle_tight({
            max_requests: 3,
            num_requests: 14,
            // Since the early tasks will release their permits before the rate
            // limit duration has elapsed, there is some variance in how long the total time
            // will take. Since our task is exactly half the rate limit
            // duration, we can expect it to be around 14/3/2 = 2.33 cycles, so
            // we can set the min and max time factors accordingly.
            min_time_factor: 2.33,
            max_time_factor: 4.9,
            task_duration: Duration::from_millis(50)
        }));
        create_test!(multiple_tasks_five_cycle_tight_long_task({
            max_requests: 3,
            num_requests: 14,
            // For long tasks, the permit will be released before the task ends,
            // so the total time will be depends on rate limit duration instead
            // of task duration.
            // The last task running will add its full 150ms to the total time,
            // so we have to add (150-100)/100 = 0.5 cycles here.
            min_time_factor: 5.5,
            max_time_factor: 6.4,
            task_duration: Duration::from_millis(150)
        }));
    }

    mod test_complete {
        use super::*;

        use std::sync::atomic::{AtomicU32, Ordering};

        struct RateEnforcer {
            current_count: AtomicU32,
            max_requests: u32,
        }

        impl RateEnforcer {
            fn new(max_requests: u32) -> Self {
                RateEnforcer {
                    current_count: AtomicU32::new(0),
                    max_requests,
                }
            }

            fn increment(&self) -> Result<(), String> {
                let count = self.current_count.fetch_add(1, Ordering::SeqCst) + 1;
                if count >= self.max_requests {
                    return Err(format!("Exceeded max requests: {} > {}", count, self.max_requests));
                }
                tracing::trace!("Current count incremented to {}", count +1);
                Ok(())
            }

            fn decrement(&self) -> Result<(), String> {
                self.current_count.fetch_sub(1, Ordering::SeqCst);

                if self.current_count.load(Ordering::SeqCst) > u32::MAX/2 {
                    return Err(format!("Current count underflowed: {}", self.current_count.load(Ordering::SeqCst)));
                }
                Ok(())
            }
        }

        #[tokio::test]
        async fn test_rate_limiter_with_concurrent_tasks() {
            logging::init_test_tracing();

            const MAX_REQUESTS: u32 = 5;
            const NUM_REQUESTS: u32 = 101;

            let enforcer = Arc::new(RateEnforcer::new(MAX_REQUESTS));

            let rate_limit = RateLimit::new(MAX_REQUESTS, Duration::from_millis(100));
            let rate_limiter = Arc::new(RateLimiter::from(rate_limit));

            let mut handles = Vec::new();
            for _ in 0..NUM_REQUESTS {
                let rl_ref = Arc::clone(&rate_limiter);
                let enforcer_ref = Arc::clone(&enforcer);
                handles.push(tokio::spawn(async move {
                    rl_ref.acquire_and_execute::<_, Result<(), String>>(async {
                        let inc_result = enforcer_ref.increment();
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        let dec_result = enforcer_ref.decrement();
                        inc_result.and(dec_result)
                    }).await
                }));
            }

            let start = std::time::Instant::now();
            let _ = futures::future::try_join_all(handles).await.expect(
                "One of the tasks failed, which indicates that the rate limit was exceeded or there was an underflow in the count."
            );
            let elapsed = start.elapsed();

            tracing::info!(
                "Elapsed time for {} concurrent tasks with a limit of {} requests per {:.2}s: {:.2}s",
                NUM_REQUESTS,
                MAX_REQUESTS,
                rate_limit.duration.as_secs_f32(),
                elapsed.as_secs_f32()
            );
        }
    }
}
