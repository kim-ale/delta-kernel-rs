use std::fmt;
use std::future::{poll_fn, Future};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use async_trait::async_trait;
use delta_kernel::object_store::azure::AzureCredential;
use delta_kernel::object_store::{CredentialProvider, Error, Result};
use delta_kernel_default_engine::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel_default_engine::executor::TaskExecutor;
use tokio::sync::Notify;
use tokio::time::{timeout_at, Instant};

struct Context {
    releases: Arc<AtomicUsize>,
}

impl Drop for Context {
    fn drop(&mut self) {
        self.releases.fetch_add(1, Ordering::SeqCst);
    }
}

enum Outcome {
    Pending,
    Completed(Arc<AzureCredential>),
    TimedOut,
    Canceled,
}

struct RequestState {
    outcome: Outcome,
    waiters: usize,
}

struct Request {
    deadline: Instant,
    state: Mutex<RequestState>,
    changed: Notify,
    _context: Arc<Context>,
}

struct Interest<'request>(&'request Request);

impl Drop for Interest<'_> {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap();
        state.waiters -= 1;
        if state.waiters == 0 && matches!(state.outcome, Outcome::Pending) {
            state.outcome = Outcome::Canceled;
        }
        drop(state);
        self.0.changed.notify_waiters();
    }
}

impl Request {
    fn complete(&self, token: String) -> bool {
        let credential = Arc::new(AzureCredential::BearerToken(token));
        let mut state = self.state.lock().unwrap();
        let accepted = matches!(state.outcome, Outcome::Pending) && Instant::now() < self.deadline;
        if accepted {
            state.outcome = Outcome::Completed(credential);
        } else if matches!(state.outcome, Outcome::Pending) {
            state.outcome = Outcome::TimedOut;
        }
        drop(state);
        self.changed.notify_waiters();
        accepted
    }

    async fn wait(&self) -> Result<Arc<AzureCredential>> {
        self.state.lock().unwrap().waiters += 1;
        let _interest = Interest(self);
        let completion = async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                {
                    let mut state = self.state.lock().unwrap();
                    if Instant::now() >= self.deadline
                        && matches!(state.outcome, Outcome::Pending | Outcome::Completed(_))
                    {
                        state.outcome = Outcome::TimedOut;
                    }
                    match &state.outcome {
                        Outcome::Completed(credential) => return Some(credential.clone()),
                        Outcome::TimedOut | Outcome::Canceled => return None,
                        Outcome::Pending => {}
                    }
                }
                changed.await;
            }
        };
        if let Ok(Some(credential)) = timeout_at(self.deadline, completion).await {
            return Ok(credential);
        }
        let mut state = self.state.lock().unwrap();
        if matches!(state.outcome, Outcome::Pending) {
            state.outcome = Outcome::TimedOut;
        }
        drop(state);
        self.changed.notify_waiters();
        Err(Error::Generic {
            store: "AzureCredentialProvider",
            source: "Credential acquisition timed out".into(),
        })
    }
}

struct Provider {
    request: Arc<Request>,
}

impl fmt::Debug for Provider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Provider").finish_non_exhaustive()
    }
}

#[async_trait]
impl CredentialProvider for Provider {
    type Credential = AzureCredential;

    async fn get_credential(&self) -> Result<Arc<AzureCredential>> {
        self.request.wait().await
    }
}

fn request(deadline: Instant, releases: Arc<AtomicUsize>) -> Arc<Request> {
    Arc::new(Request {
        deadline,
        state: Mutex::new(RequestState {
            outcome: Outcome::Pending,
            waiters: 0,
        }),
        changed: Notify::new(),
        _context: Arc::new(Context { releases }),
    })
}

fn run_on_executor<Operation>(background: bool, operation: Operation) -> Operation::Output
where
    Operation: Future + Send + 'static,
    Operation::Output: Send + 'static,
{
    if background {
        TokioBackgroundExecutor::new().block_on(operation)
    } else {
        TokioMultiThreadExecutor::new_owned_runtime(Some(2), None)
            .unwrap()
            .block_on(operation)
    }
}

#[rstest::rstest]
#[case(false)]
#[case(true)]
fn stalled_foreign_completion_does_not_disable_native_timeout_or_release_context(
    #[case] background: bool,
) {
    let releases = Arc::new(AtomicUsize::new(0));
    let request_releases = releases.clone();
    let (dispatch, queued) = mpsc::channel::<Arc<Request>>();
    let (worker_ready, worker_started) = mpsc::channel();
    let (ready, admitted) = mpsc::channel();
    let (resume, stalled) = mpsc::channel();
    let completion = std::thread::spawn(move || {
        assert!(tokio::runtime::Handle::try_current().is_err());
        worker_ready.send(()).unwrap();
        let ticket = queued.recv_timeout(Duration::from_secs(5)).unwrap();
        ready.send(Instant::now() < ticket.deadline).unwrap();
        let _ = stalled.recv_timeout(Duration::from_secs(10));
        ticket.complete("late-token".into())
    });
    worker_started.recv_timeout(Duration::from_secs(5)).unwrap();
    let (result, native_request) = run_on_executor(background, async move {
        let native_request = request(Instant::now() + Duration::from_secs(1), request_releases);
        let provider = Provider {
            request: native_request.clone(),
        };
        dispatch.send(native_request.clone()).unwrap();
        let mut acquisition = provider.get_credential();
        poll_fn(|context| {
            assert!(acquisition.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        let result = acquisition.await;
        (result, native_request)
    });
    assert!(admitted.try_recv().unwrap());
    assert!(result.unwrap_err().to_string().contains("timed out"));
    assert!(matches!(
        native_request.state.lock().unwrap().outcome,
        Outcome::TimedOut
    ));
    drop(native_request);
    assert_eq!(releases.load(Ordering::SeqCst), 0);
    resume.send(()).unwrap();
    assert!(!completion.join().unwrap());
    assert_eq!(releases.load(Ordering::SeqCst), 1);
}

#[rstest::rstest]
#[case(false)]
#[case(true)]
fn completion_before_notification_registration_remains_visible(#[case] background: bool) {
    let releases = Arc::new(AtomicUsize::new(0));
    let native_request = request(Instant::now() + Duration::from_secs(5), releases.clone());
    assert!(native_request.complete("ready-token".into()));
    let provider = Provider {
        request: native_request,
    };
    let result = run_on_executor(background, async move { provider.get_credential().await });
    assert!(
        matches!(&*result.unwrap(), AzureCredential::BearerToken(token) if token == "ready-token")
    );
    assert_eq!(releases.load(Ordering::SeqCst), 1);
}

#[rstest::rstest]
#[case(false)]
#[case(true)]
fn expired_deadline_rejects_pending_and_already_ready_completion(#[case] completed: bool) {
    let executor = TokioMultiThreadExecutor::new_owned_runtime(Some(2), None).unwrap();
    let mut native_request = request(
        Instant::now() + Duration::from_secs(5),
        Arc::new(AtomicUsize::new(0)),
    );
    if completed {
        assert!(native_request.complete("ready-token".into()));
    }
    Arc::get_mut(&mut native_request).unwrap().deadline = Instant::now() - Duration::from_secs(1);
    let provider = Provider {
        request: native_request,
    };
    let result = executor.block_on(async move { provider.get_credential().await });
    assert!(result.unwrap_err().to_string().contains("timed out"));
}

#[rstest::rstest]
#[case(1)]
#[case(2)]
fn waiter_cancellation_retires_only_after_last_interest(#[case] waiter_count: usize) {
    let executor = TokioMultiThreadExecutor::new_owned_runtime(Some(2), None).unwrap();
    let releases = Arc::new(AtomicUsize::new(0));
    let native_request = request(Instant::now() + Duration::from_secs(5), releases.clone());
    let provider = Provider {
        request: native_request.clone(),
    };
    executor.block_on(async move {
        let mut first = provider.get_credential();
        poll_fn(|context| {
            assert!(first.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        let mut second = (waiter_count == 2).then(|| provider.get_credential());
        if let Some(waiter) = &mut second {
            poll_fn(|context| {
                assert!(waiter.as_mut().poll(context).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        drop(first);
        assert_eq!(
            provider.request.state.lock().unwrap().waiters,
            waiter_count - 1
        );
        if let Some(waiter) = second {
            assert!(provider.request.complete("shared-token".into()));
            assert!(waiter.await.is_ok());
        } else {
            assert!(matches!(
                provider.request.state.lock().unwrap().outcome,
                Outcome::Canceled
            ));
            assert!(!provider.request.complete("retired-token".into()));
        }
    });
    drop(native_request);
    assert_eq!(releases.load(Ordering::SeqCst), 1);
}
