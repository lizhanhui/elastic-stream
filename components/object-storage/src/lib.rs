#![feature(async_fn_in_trait)]
#![feature(map_try_insert)]
#![warn(clippy::pedantic)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::must_use_candidate)]

pub mod object_manager;
pub mod object_storage;
mod range_accumulator;
pub mod range_fetcher;
mod range_offload;

use std::cell::UnsafeCell;

use model::{error::EsError, object::ObjectMetadata};

use mockall::automock;

use tokio::sync::{broadcast, mpsc};

pub type OffloadProgress = Vec<((u64, u32), u64)>;
pub type OffloadProgressListener = mpsc::UnboundedReceiver<OffloadProgress>;

#[automock]
pub trait ObjectStorage {
    /// new record commit notify
    fn new_commit(&self, stream_id: u64, range_index: u32, record_size: u32);

    async fn get_objects(
        &self,
        stream_id: u64,
        range_index: u32,
        start_offset: u64,
        end_offset: u64,
        size_hint: u32,
    ) -> (Vec<ObjectMetadata>, bool);

    async fn get_offloading_range(&self) -> Vec<RangeKey>;

    async fn watch_offload_progress(&self) -> OffloadProgressListener;

    async fn close(&self);
}

pub type OwnerListener = mpsc::UnboundedReceiver<OwnerEvent>;

#[cfg_attr(test, automock)]
pub trait ObjectManager {
    /// Returns a channel that receives the owner change event.
    /// Firstly, the channel will receive the ownership status of all ranges on the range server in a random order.
    /// Then, the channel will receive owner change events in time order.
    /// The channel will be closed when the object manager is closed.
    fn owner_watcher(&self) -> OwnerListener;

    async fn commit_object(&self, object_metadata: ObjectMetadata) -> Result<(), EsError>;

    fn get_objects(
        &self,
        stream_id: u64,
        range_index: u32,
        start_offset: u64,
        end_offset: u64,
        size_hint: u32,
    ) -> (Vec<ObjectMetadata>, bool);

    fn get_offloading_range(&self) -> Vec<RangeKey>;

    /// Watch the range offload progress which range is held by current server.
    fn watch_offload_progress(&self) -> OffloadProgressListener;
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Owner {
    pub start_offset: u64,
    pub epoch: u16,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RangeKey {
    pub stream_id: u64,
    pub range_index: u32,
}

impl RangeKey {
    pub fn new(stream_id: u64, range_index: u32) -> Self {
        Self {
            stream_id,
            range_index,
        }
    }
}

/// The event of owner change.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OwnerEvent {
    /// The key of the range that the owner changed.
    pub range_key: RangeKey,

    /// The new owner of the range.
    /// If `None`, it indicates that the ownership of the range has been removed.
    pub owner: Option<Owner>,
}

/// New Shutdown channel (`ShutdownTx`, `ShutdownRx`).
/// - `ShutdownTx`: send shutdown signal to all task and await task complete(detected by all `ShutdownRx` are dropped).
/// - `ShutdownRx`: subscribe shutdown signal.
///
/// # Example:
///
/// ```
/// use object_storage::shutdown_chan;
///
/// tokio_uring::start(async move {
///     let (tx, rx) = shutdown_chan();
///     for _i in 0..2 {
///         let rx = rx.clone();
///         tokio_uring::spawn(async move {
///             let mut notify_rx = rx.subscribe();
///             loop {
///                 tokio::select! {
///                 _ = notify_rx.recv() => {
///                     println!("task recv shutdown signal");
///                     break;
///                 }
///                 _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
///                     println!("task tick");
///                     }
///                 }
///             }
///             println!("task exit");
///         });
///     }
///     drop(rx);
///     tokio::time::sleep(std::time::Duration::from_secs(1)).await;
///     println!("shutting");
///     tx.shutdown().await;
///     println!("shutdown");
/// })
/// ```
pub fn shutdown_chan() -> (ShutdownTx, ShutdownRx) {
    let (notify_tx, _notify_rx) = broadcast::channel(1);
    let (await_tx, await_rx) = mpsc::channel(1);
    (
        ShutdownTx::new(notify_tx.clone(), await_rx),
        ShutdownRx::new(notify_tx, await_tx),
    )
}

#[derive(Clone)]
pub struct ShutdownRx {
    notify_tx: broadcast::Sender<()>,
    _await_tx: mpsc::Sender<()>,
}

impl ShutdownRx {
    pub fn new(notify_tx: broadcast::Sender<()>, await_tx: mpsc::Sender<()>) -> Self {
        Self {
            notify_tx,
            _await_tx: await_tx,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.notify_tx.subscribe()
    }
}

pub struct ShutdownTx {
    notify_tx: broadcast::Sender<()>,
    await_rx: UnsafeCell<mpsc::Receiver<()>>,
}

impl ShutdownTx {
    pub fn new(notify_tx: broadcast::Sender<()>, await_rx: mpsc::Receiver<()>) -> Self {
        Self {
            notify_tx,
            await_rx: UnsafeCell::new(await_rx),
        }
    }

    pub async fn shutdown(&self) {
        let _ = self.notify_tx.send(());
        // receive a error, when all await_rx is dropped
        let _ = unsafe { &mut *self.await_rx.get() }.recv().await;
    }
}
