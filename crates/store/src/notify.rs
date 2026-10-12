use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgListener;
use tokio::sync::{OnceCell, broadcast};
use tokio::time::Instant;

use crate::{Store, StoreError};

pub(crate) const FILL_DONE: &str = "fill_done";
pub(crate) const HEADS: &str = "heads";

/// A lost notification costs a waiter at most this long, because it rereads state on every slice.
const RECHECK: Duration = Duration::from_secs(1);
const FANOUT: usize = 1024;

#[derive(Debug, Clone)]
pub(crate) enum Wake {
    Note {
        channel: String,
        payload: String,
    },
    /// The listener reconnected and may have dropped notifications.
    Resync,
}

/// One `LISTEN` connection per [`Store`], shared by every waiter.
#[derive(Debug, Default)]
pub(crate) struct Notifier {
    sender: OnceCell<broadcast::Sender<Wake>>,
}

impl Store {
    async fn subscribe(&self) -> Result<broadcast::Receiver<Wake>, StoreError> {
        let sender = self
            .notifier
            .sender
            .get_or_try_init(|| start(&self.pool))
            .await?;
        Ok(sender.subscribe())
    }

    /// Runs `check` until it returns a value or `timeout` passes. It runs again on each wake
    /// that `relevant` accepts, and at least every [`RECHECK`].
    pub(crate) async fn wait_until<T, F, Fut>(
        &self,
        timeout: Duration,
        relevant: impl Fn(&str, &str) -> bool,
        mut check: F,
    ) -> Result<Option<T>, StoreError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<Option<T>, StoreError>>,
    {
        let mut wakes = self.subscribe().await?;
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(value) = check().await? {
                return Ok(Some(value));
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            let slice_end = deadline.min(now + RECHECK);
            loop {
                match tokio::time::timeout_at(slice_end, wakes.recv()).await {
                    Err(_) => break,
                    Ok(Ok(Wake::Note { channel, payload })) => {
                        if relevant(&channel, &payload) {
                            break;
                        }
                    }
                    Ok(Ok(Wake::Resync)) | Ok(Err(broadcast::error::RecvError::Lagged(_))) => {
                        break;
                    }
                    Ok(Err(broadcast::error::RecvError::Closed)) => {
                        tokio::time::sleep_until(slice_end).await;
                        break;
                    }
                }
            }
        }
    }
}

async fn start(pool: &PgPool) -> Result<broadcast::Sender<Wake>, StoreError> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen_all([FILL_DONE, HEADS]).await?;
    let (sender, _) = broadcast::channel(FANOUT);
    tokio::spawn(forward(listener, sender.clone()));
    Ok(sender)
}

async fn forward(mut listener: PgListener, sender: broadcast::Sender<Wake>) {
    loop {
        match listener.try_recv().await {
            Ok(Some(note)) => {
                let _ = sender.send(Wake::Note {
                    channel: note.channel().to_owned(),
                    payload: note.payload().to_owned(),
                });
            }
            Ok(None) => {
                let _ = sender.send(Wake::Resync);
            }
            Err(sqlx::Error::PoolClosed) => return,
            Err(_) => {
                let _ = sender.send(Wake::Resync);
                tokio::time::sleep(RECHECK).await;
            }
        }
    }
}
