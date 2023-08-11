use super::session_manager::SessionManager;

use crate::{composite_session::CompositeSession, heartbeat::HeartbeatData};

use bytes::Bytes;
use log::{error, trace, warn};
use model::{
    error::EsError,
    object::ObjectMetadata,
    range::RangeMetadata,
    replica::RangeProgress,
    request::fetch::FetchRequest,
    response::{
        fetch::FetchResultSet,
        resource::{ListResourceResult, WatchResourceResult},
    },
    stream::StreamMetadata,
    AppendResultEntry, ListRangeCriteria,
};
use monoio::time;
use observation::metrics::{
    store_metrics::RangeServerStatistics,
    sys_metrics::{DiskStatistics, MemoryStatistics},
    uring_metrics::UringStatistics,
};
use protocol::rpc::header::{ErrorCode, RangeServerState, ResourceType, SealKind};
use std::{cell::UnsafeCell, rc::Rc, sync::Arc, time::Duration};

#[cfg(any(test, feature = "mock"))]
use mockall::automock;

/// Definition of core storage trait.
#[cfg_attr(any(test, feature = "mock"), automock)]
pub trait Client {
    async fn allocate_id(&self, host: &str) -> Result<i32, EsError>;

    async fn list_ranges(&self, criteria: ListRangeCriteria)
        -> Result<Vec<RangeMetadata>, EsError>;

    async fn broadcast_heartbeat(&self, data: &HeartbeatData);

    async fn create_stream(
        &self,
        stream_metadata: StreamMetadata,
    ) -> Result<StreamMetadata, EsError>;

    async fn describe_stream(&self, stream_id: u64) -> Result<StreamMetadata, EsError>;

    async fn update_stream_epoch(&self, stream_id: u64, epoch: u64) -> Result<(), EsError>;

    async fn create_range(&self, range_metadata: RangeMetadata) -> Result<RangeMetadata, EsError>;

    async fn create_range_replica(
        &self,
        target: &str,
        range_metadata: RangeMetadata,
    ) -> Result<(), EsError>;

    async fn seal<'a>(
        &self,
        target: Option<&'a str>,
        kind: SealKind,
        range: RangeMetadata,
    ) -> Result<RangeMetadata, EsError>;

    async fn append(
        &self,
        target: &str,
        buf: Vec<Bytes>,
    ) -> Result<Vec<AppendResultEntry>, EsError>;

    async fn fetch(&self, target: &str, request: FetchRequest) -> Result<FetchResultSet, EsError>;

    async fn report_metrics(
        &self,
        target: &str,
        state: RangeServerState,
        uring_statistics: &UringStatistics,
        range_server_statistics: &RangeServerStatistics,
        disk_statistics: &DiskStatistics,
        memory_statistics: &MemoryStatistics,
    ) -> Result<(), EsError>;

    async fn report_range_progress(&self, progress: Vec<RangeProgress>) -> Result<(), EsError>;

    async fn commit_object(&self, metadata: ObjectMetadata) -> Result<(), EsError>;

    async fn list_resource(
        &self,
        types: &[ResourceType],
        limit: i32,
        continuation: &Option<Bytes>,
    ) -> Result<ListResourceResult, EsError>;

    async fn watch_resource(
        &self,
        types: &[ResourceType],
        version: i64,
        timeout: Duration,
    ) -> Result<WatchResourceResult, EsError>;

    async fn target_go_away(&self, target: &str) -> Result<bool, EsError>;
}

/// `Client` is used to send
pub struct DefaultClient {
    pub(crate) session_manager: Rc<UnsafeCell<SessionManager>>,
    pub(crate) config: Arc<config::Configuration>,
}

impl Client for DefaultClient {
    async fn allocate_id(&self, host: &str) -> Result<i32, EsError> {
        let composite_session = self.get_pd_session().await?;
        let future = composite_session.allocate_id(host, None);
        time::timeout(self.config.client_io_timeout(), future)
            .await
            .map_err(|e| {
                warn!("Timeout when allocate ID. {}", e);
                EsError::new(ErrorCode::RPC_TIMEOUT, "allocate ID rpc timeout")
            })?
            .map_err(|e| {
                error!(
                    "Failed to receive response from composite session. Cause: {:?}",
                    e
                );
                EsError::new(ErrorCode::ERROR_CODE_UNSPECIFIED, "todo")
            })
    }

    async fn list_ranges(
        &self,
        criteria: ListRangeCriteria,
    ) -> Result<Vec<RangeMetadata>, EsError> {
        let session = self.get_pd_session().await?;
        let future = session.list_range(criteria);
        time::timeout(self.config.client_io_timeout(), future)
            .await
            .map_err(|elapsed| {
                warn!("Timeout when list range. {}", elapsed);
                EsError::new(ErrorCode::RPC_TIMEOUT, "list ranges rpc timeout")
            })?
            .map_err(|e| {
                error!(
                    "Failed to receive response from broken channel. Cause: {:?}",
                    e
                );
                EsError::new(ErrorCode::ERROR_CODE_UNSPECIFIED, "todo")
            })
    }

    /// Broadcast heartbeats to all sessions in the `CompositeSession`.
    ///
    /// # Arguments
    /// `target` - Placement driver access URL.
    ///
    /// # Returns
    ///
    async fn broadcast_heartbeat(&self, data: &HeartbeatData) {
        let session_manager = unsafe { &mut *self.session_manager.get() };
        session_manager.broadcast_heartbeat(data).await;
    }

    async fn create_stream(
        &self,
        stream_metadata: StreamMetadata,
    ) -> Result<StreamMetadata, EsError> {
        let composite_session = self.get_pd_session().await?;
        let future = composite_session.create_stream(stream_metadata);
        time::timeout(self.config.client_io_timeout(), future)
            .await
            .map_err(|e| {
                error!("Timeout when create stream. {}", e);
                EsError::new(ErrorCode::RPC_TIMEOUT, "create stream rpc timeout")
            })?
    }

    async fn describe_stream(&self, stream_id: u64) -> Result<StreamMetadata, EsError> {
        let composite_session = self.get_pd_session().await?;
        let future = composite_session.describe_stream(stream_id);
        time::timeout(self.config.client_io_timeout(), future)
            .await
            .map_err(|e| {
                error!("Timeout when describe stream[stream-id={stream_id}]. {}", e);
                EsError::new(ErrorCode::RPC_TIMEOUT, "describe stream rpc timeout")
            })?
    }

    async fn update_stream_epoch(&self, stream_id: u64, epoch: u64) -> Result<(), EsError> {
        let composite_session = self.get_pd_session().await?;
        let future = composite_session.update_stream_epoch(stream_id, epoch);
        time::timeout(self.config.client_io_timeout(), future)
            .await
            .map_err(|e| {
                error!(
                    "Timeout when update stream epoch[stream-id={stream_id}]. {}",
                    e
                );
                EsError::new(ErrorCode::RPC_TIMEOUT, "describe stream rpc timeout")
            })?
    }

    /// Create a new range by send request to placement driver.
    async fn create_range(&self, range_metadata: RangeMetadata) -> Result<RangeMetadata, EsError> {
        let composite_session = self.get_pd_session().await?;
        self.create_range0(composite_session, range_metadata).await
    }

    /// Create a new range replica by send request to range server.
    async fn create_range_replica(
        &self,
        target: &str,
        range_metadata: RangeMetadata,
    ) -> Result<(), EsError> {
        let session_manager = unsafe { &mut *self.session_manager.get() };
        let composite_session = session_manager.get_composite_session(target).await?;
        trace!("Create range replica to composite-channel={}", target);
        self.create_range0(composite_session, range_metadata)
            .await
            .map(|_| ())
    }

    async fn seal<'a>(
        &self,
        target: Option<&'a str>,
        kind: SealKind,
        range: RangeMetadata,
    ) -> Result<RangeMetadata, EsError> {
        // Validate request
        match kind {
            SealKind::RANGE_SERVER => {
                if target.is_none() {
                    error!("Target is required while seal range against range servers");
                    return Err(EsError::new(
                        ErrorCode::UNEXPECTED,
                        "target is required when seal range against range servers",
                    ));
                }
            }
            SealKind::PLACEMENT_DRIVER => {
                if range.end().is_none() {
                    error!(
                        "SealRange.range.end MUST be present while seal against placement driver"
                    );
                    return Err(EsError::new(
                        ErrorCode::UNEXPECTED,
                        "end_offset is required when seal range against range servers",
                    ));
                }
            }
            _ => {
                error!("Seal request kind must specify");
                return Err(EsError::new(ErrorCode::UNEXPECTED, "seal kind is empty"));
            }
        }

        let session_manager = unsafe { &mut *self.session_manager.get() };
        let composite_session = match target {
            None => {
                session_manager
                    .get_composite_session(&self.config.placement_driver)
                    .await?
            }

            Some(addr) => session_manager.get_composite_session(addr).await?,
        };
        let future = composite_session.seal(kind, range);
        time::timeout(self.config.client_io_timeout(), future)
            .await
            .map_err(|_| {
                error!("Timeout when seal range");
                EsError::new(ErrorCode::RPC_TIMEOUT, "seal range rpc timeout")
            })?
    }

    /// Append data to a range.
    async fn append(
        &self,
        target: &str,
        buf: Vec<Bytes>,
    ) -> Result<Vec<AppendResultEntry>, EsError> {
        let session_manager = unsafe { &mut *self.session_manager.get() };
        let session = session_manager.get_composite_session(target).await?;
        let future = session.append(buf);
        time::timeout(self.config.client_io_timeout(), future)
            .await
            .map_err(|_e| EsError::new(ErrorCode::RPC_TIMEOUT, "append rpc timeout"))?
    }

    /// Fetch data from a range replica.
    async fn fetch(&self, target: &str, request: FetchRequest) -> Result<FetchResultSet, EsError> {
        let session_manager = unsafe { &mut *self.session_manager.get() };
        let session = session_manager.get_composite_session(target).await?;
        let future = session.fetch(request);
        time::timeout(self.config.client_io_timeout(), future)
            .await
            .map_err(|_e| EsError::new(ErrorCode::RPC_TIMEOUT, "fetch rpc timeout"))?
    }

    /// Report metrics to placement driver
    ///
    /// # Arguments
    /// `target` - Placement driver access URL.
    ///
    /// # Returns
    ///
    async fn report_metrics(
        &self,
        target: &str,
        state: RangeServerState,
        uring_statistics: &UringStatistics,
        range_server_statistics: &RangeServerStatistics,
        disk_statistics: &DiskStatistics,
        memory_statistics: &MemoryStatistics,
    ) -> Result<(), EsError> {
        let session_manager = unsafe { &mut *self.session_manager.get() };
        let composite_session = session_manager.get_composite_session(target).await?;
        trace!("Report metrics to composite-channel={}", target);

        composite_session
            .report_metrics(
                state,
                uring_statistics,
                range_server_statistics,
                disk_statistics,
                memory_statistics,
            )
            .await
    }

    async fn report_range_progress(&self, progress: Vec<RangeProgress>) -> Result<(), EsError> {
        let composite_session = self.get_pd_session().await?;
        let future = composite_session.report_range_progress(progress);
        time::timeout(self.config.client_io_timeout(), future)
            .await
            .map_err(|_| EsError::new(ErrorCode::RPC_TIMEOUT, "watch resource timeout"))?
    }

    async fn commit_object(&self, metadata: ObjectMetadata) -> Result<(), EsError> {
        let composite_session = self.get_pd_session().await?;
        let future = composite_session.commit_object(metadata);
        time::timeout(self.config.client_io_timeout(), future)
            .await
            .map_err(|_| EsError::new(ErrorCode::RPC_TIMEOUT, "commit object timeout"))?
    }

    /// List resources from PD of given types.
    ///
    /// # Arguments
    /// * `types` - Resource types to list.
    /// * `limit` - Maximum number of resources to list. If zero, no limit.
    /// * `continuation` - Optional. Continuation token from previous list.
    ///
    /// # Returns
    /// * `ListResourceResult` - List of resources, and a continuation token if the result is truncated.
    ///
    /// TODO: Error handling
    ///
    async fn list_resource(
        &self,
        types: &[ResourceType],
        limit: i32,
        continuation: &Option<Bytes>,
    ) -> Result<ListResourceResult, EsError> {
        let composite_session = self.get_pd_session().await?;
        let future = composite_session.list_resource(types, limit, continuation);
        time::timeout(self.config.client_io_timeout(), future)
            .await
            .map_err(|_| EsError::new(ErrorCode::RPC_TIMEOUT, "list resource timeout"))?
    }

    /// Watch resources from PD of given types on given version.
    ///
    /// # Arguments
    /// * `types` - Resource types to watch.
    /// * `version` - Resource version to watch. All changes with a version greater than this version will be returned.
    /// * `timeout` - If no resource change after this timeout, the watch will be cancelled.
    ///
    /// # Returns
    /// * `WatchResourceResult` - List of resource events.
    ///
    /// TODO: Error handling
    ///
    async fn watch_resource(
        &self,
        types: &[ResourceType],
        version: i64,
        timeout: Duration,
    ) -> Result<WatchResourceResult, EsError> {
        let composite_session = self.get_pd_session().await?;
        let future = composite_session.watch_resource(types, version, timeout);
        // Use the watch timeout instead of client timeout, as this is a long polling request
        time::timeout(timeout, future)
            .await
            .map_err(|_| EsError::new(ErrorCode::RPC_TIMEOUT, "watch resource timeout"))?
    }

    /// Check if the peer server has notified it would go away for maintenance.
    ///
    /// Note that overhead of this method is minimal.
    ///
    /// # Return
    /// * Ok(true) - if the target has already notified its scheduled shutdown;
    /// * Ok(false) - otherwise.
    async fn target_go_away(&self, target: &str) -> Result<bool, EsError> {
        let session_manager = unsafe { &mut *self.session_manager.get() };
        let session = session_manager.get_composite_session(target).await?;
        Ok(session.go_away())
    }
}

impl DefaultClient {
    pub fn new(config: Arc<config::Configuration>) -> Self {
        let session_manager = Rc::new(UnsafeCell::new(SessionManager::new(&config)));

        Self {
            session_manager,
            config,
        }
    }

    async fn create_range0(
        &self,
        composite_session: Rc<CompositeSession>,
        range: RangeMetadata,
    ) -> Result<RangeMetadata, EsError> {
        let future = composite_session.create_range(range);
        time::timeout(self.config.client_io_timeout(), future)
            .await
            .map_err(|e| {
                error!("Timeout when create range. {}", e);
                EsError::new(ErrorCode::RPC_TIMEOUT, "create range rpc timeout")
            })?
    }

    async fn get_pd_session(&self) -> Result<Rc<CompositeSession>, EsError> {
        let session_manager = unsafe { &mut *self.session_manager.get() };
        session_manager
            .get_composite_session(&self.config.placement_driver)
            .await
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use log::trace;
    use mock_server::run_listener;
    use model::error::EsError;
    use model::resource::{EventType, Resource};
    use model::stream::StreamMetadata;
    use model::ListRangeCriteria;
    use model::{
        range::RangeMetadata,
        record::{flat_record::FlatRecordBatch, RecordBatchBuilder},
    };
    use observation::metrics::{
        store_metrics::RangeServerStatistics,
        sys_metrics::{DiskStatistics, MemoryStatistics},
        uring_metrics::UringStatistics,
    };
    use protocol::rpc::header::{ClientRole, RangeServerState, ResourceType, SealKind};
    use std::{error::Error, sync::Arc};

    use crate::{client::Client, heartbeat::HeartbeatData, DefaultClient};

    #[monoio::test]
    async fn test_broadcast_heartbeat() -> Result<(), Box<dyn Error>> {
        #[allow(unused_variables)]
        let port = 2378;
        let port = run_listener().await;
        let mut config = config::Configuration {
            placement_driver: format!("127.0.0.1:{}", port),
            ..Default::default()
        };
        config.check_and_apply().unwrap();
        let config = Arc::new(config);
        let client = DefaultClient::new(Arc::clone(&config));
        let heartbeat_data = HeartbeatData {
            mandatory: false,
            role: ClientRole::CLIENT_ROLE_RANGE_SERVER,
            state: Some(RangeServerState::RANGE_SERVER_STATE_READ_WRITE),
        };
        client.broadcast_heartbeat(&heartbeat_data).await;
        Ok(())
    }

    #[monoio::test]
    async fn test_allocate_id() -> Result<(), Box<dyn Error>> {
        ulog::try_init_log();
        #[allow(unused_variables)]
        let port = 2378;
        let port = run_listener().await;
        let config = config::Configuration {
            placement_driver: format!("127.0.0.1:{}", port),
            ..Default::default()
        };
        let config = Arc::new(config);
        let client = DefaultClient::new(config);
        let id = client.allocate_id("localhost").await?;
        assert_eq!(1, id);
        Ok(())
    }

    #[monoio::test]
    async fn test_create_stream() -> Result<(), Box<dyn Error>> {
        ulog::try_init_log();
        #[allow(unused_variables)]
        let port = 12378;
        let port = run_listener().await;
        let config = config::Configuration {
            placement_driver: format!("127.0.0.1:{}", port),
            ..Default::default()
        };
        let config = Arc::new(config);
        let client = DefaultClient::new(config);
        let stream_metadata = client
            .create_stream(StreamMetadata {
                stream_id: None,
                replica: 1,
                ack_count: 1,
                retention_period: std::time::Duration::from_secs(1),
                start_offset: 0,
            })
            .await?;
        dbg!(&stream_metadata);
        assert!(stream_metadata.stream_id.is_some());
        assert_eq!(1, stream_metadata.replica);
        assert_eq!(
            std::time::Duration::from_secs(1),
            stream_metadata.retention_period
        );
        Ok(())
    }

    #[test]
    fn test_describe_stream() -> Result<(), EsError> {
        ulog::try_init_log();
        tokio_uring::start(async move {
            #[allow(unused_variables)]
            let port = 12378;
            let port = run_listener().await;
            let config = config::Configuration {
                placement_driver: format!("127.0.0.1:{}", port),
                ..Default::default()
            };
            let config = Arc::new(config);
            let (tx, _rx) = broadcast::channel(1);
            let client = DefaultClient::new(config, tx);

            let stream_metadata = client
                .describe_stream(1)
                .await
                .expect("Describe stream should not fail");
            assert_eq!(Some(1), stream_metadata.stream_id);
            assert_eq!(1, stream_metadata.replica);
            assert_eq!(
                std::time::Duration::from_secs(3600 * 24),
                stream_metadata.retention_period
            );
            Ok(())
        })
    }

    #[test]
    fn test_create_range() -> Result<(), EsError> {
        ulog::try_init_log();
        tokio_uring::start(async {
            #[allow(unused_variables)]
            let port = 12378;
            let port = run_listener().await;
            let config = config::Configuration {
                placement_driver: format!("127.0.0.1:{}", port),
                ..Default::default()
            };
            let target = config.placement_driver.clone();
            let config = Arc::new(config);
            let (tx, _rx) = broadcast::channel(1);
            let client = DefaultClient::new(config, tx);
            let range = RangeMetadata::new(100, 0, 0, 0, None);
            client.create_range_replica(&target, range).await
        })
    }

    #[monoio::test]
    async fn test_create_range_range_server() -> Result<(), EsError> {
        ulog::try_init_log();
        #[allow(unused_variables)]
        let placement_driver_port = 12378;
        let placement_driver_port = run_listener().await;

        #[allow(unused_variables)]
        let range_server_port = 10911;
        let range_server_port = run_listener().await;

        let config = config::Configuration {
            placement_driver: format!("127.0.0.1:{}", placement_driver_port),
            ..Default::default()
        };

        let target = format!("127.0.0.1:{}", range_server_port);

        let config = Arc::new(config);
        let client = DefaultClient::new(config);
        let range = RangeMetadata::new_range(203, 0, 0, 0, None, 1, 1);
        client.create_range_replica(&target, range).await
    }

    #[monoio::test]
    async fn test_list_range_by_stream() -> Result<(), EsError> {
        ulog::try_init_log();
        #[allow(unused_variables)]
        let port = 2378;
        let port = run_listener().await;
        let config = config::Configuration {
            placement_driver: format!("127.0.0.1:{}", port),
            ..Default::default()
        };
        let config = Arc::new(config);
        let (tx, _rx) = broadcast::channel(1);
        let client = DefaultClient::new(config, tx);

        for i in 1..2 {
            let criteria = ListRangeCriteria::new(None, Some(i as u64));
            let ranges = client.list_ranges(criteria).await.unwrap();
            assert!(
                !ranges.is_empty(),
                "Test server should have fed some mocking ranges"
            );
            for range in ranges.iter() {
                trace!("{}", range)
            }
        }

        Ok(())
    }

    #[test]
    fn test_list_range_by_range_server() -> Result<(), EsError> {
        ulog::try_init_log();
        tokio_uring::start(async {
            #[allow(unused_variables)]
            let port = 2378;
            let port = run_listener().await;
            let config = config::Configuration {
                placement_driver: format!("127.0.0.1:{}", port),
                ..Default::default()
            };
            let config = Arc::new(config);
            let (tx, _rx) = broadcast::channel(1);
            let client = DefaultClient::new(config, tx);

            for _i in 1..2 {
                let criteria = ListRangeCriteria::new(None, None);
                let ranges = client.list_ranges(criteria).await.unwrap();
                assert!(
                    !ranges.is_empty(),
                    "Test server should have fed some mocking ranges"
                );
                for range in ranges.iter() {
                    trace!("{}", range)
                }
            }

            Ok(())
        })
    }

    /// Test seal range server without end. This RPC is used when the single writer takes over a stream from a failed writer.
    #[test]
    fn test_seal_range_server() -> Result<(), EsError> {
        ulog::try_init_log();
        tokio_uring::start(async move {
            #[allow(unused_variables)]
            let port = 2378;
            let port = run_listener().await;
            let mut config = config::Configuration {
                placement_driver: format!("127.0.0.1:{}", port),
                ..Default::default()
            };
            config.check_and_apply().unwrap();
            let config = Arc::new(config);
            let (tx, _rx) = broadcast::channel(1);
            let client = DefaultClient::new(Arc::clone(&config), tx);
            let range = RangeMetadata::new(0, 0, 0, 0, None);
            let range = client
                .seal(
                    Some(&config.placement_driver),
                    SealKind::RANGE_SERVER,
                    range,
                )
                .await?;
            assert_eq!(0, range.stream_id());
            assert_eq!(0, range.index());
            assert_eq!(0, range.epoch());
            assert_eq!(0, range.start());
            assert_eq!(Some(100), range.end());
            Ok(())
        })
    }

    /// Test seal range server with end. This RPC is used when the single writer takes over a stream from a graceful closed writer.
    #[test]
    fn test_seal_range_server_with_end() -> Result<(), EsError> {
        ulog::try_init_log();
        tokio_uring::start(async move {
            #[allow(unused_variables)]
            let port = 2378;
            let port = run_listener().await;
            let mut config = config::Configuration {
                placement_driver: format!("127.0.0.1:{}", port),
                ..Default::default()
            };
            config.check_and_apply().unwrap();
            let config = Arc::new(config);
            let (tx, _rx) = broadcast::channel(1);
            let client = DefaultClient::new(Arc::clone(&config), tx);
            let range = RangeMetadata::new(0, 0, 0, 0, Some(1));
            let range = client
                .seal(
                    Some(&config.placement_driver),
                    SealKind::RANGE_SERVER,
                    range,
                )
                .await?;
            assert_eq!(0, range.stream_id());
            assert_eq!(0, range.index());
            assert_eq!(0, range.epoch());
            assert_eq!(0, range.start());
            assert_eq!(Some(1), range.end());
            Ok(())
        })
    }

    /// Test seal placement driver.
    #[test]
    fn test_seal_placement_driver() -> Result<(), EsError> {
        ulog::try_init_log();
        tokio_uring::start(async move {
            #[allow(unused_variables)]
            let port = 2378;
            let port = run_listener().await;
            let mut config = config::Configuration {
                placement_driver: format!("127.0.0.1:{}", port),
                ..Default::default()
            };
            config.check_and_apply().unwrap();
            let config = Arc::new(config);
            let (tx, _rx) = broadcast::channel(1);
            let client = DefaultClient::new(Arc::clone(&config), tx);
            let range = RangeMetadata::new(0, 0, 0, 0, Some(1));
            let range = client
                .seal(
                    Some(&config.placement_driver),
                    SealKind::PLACEMENT_DRIVER,
                    range,
                )
                .await?;
            assert_eq!(0, range.stream_id());
            assert_eq!(0, range.index());
            assert_eq!(0, range.epoch());
            assert_eq!(0, range.start());
            assert_eq!(Some(1), range.end());
            Ok(())
        })
    }

    #[test]
    fn test_append() -> Result<(), Box<dyn Error>> {
        ulog::try_init_log();
        tokio_uring::start(async move {
            #[allow(unused_variables)]
            let port = 2378;
            let port = run_listener().await;
            let mut config = config::Configuration {
                placement_driver: format!("127.0.0.1:{}", port),
                ..Default::default()
            };
            config.check_and_apply().unwrap();
            let config = Arc::new(config);
            let (tx, _rx) = broadcast::channel(1);
            let client = DefaultClient::new(Arc::clone(&config), tx);
            let stream_id = 0;
            let index = 0;

            const BATCH: i64 = 100;
            const PAYLOAD_LENGTH: usize = 1024;

            let mut payload = BytesMut::with_capacity(PAYLOAD_LENGTH);
            payload.resize(PAYLOAD_LENGTH, 65);
            let payload = payload.freeze();

            let mut buf = BytesMut::new();
            for i in 0..BATCH {
                let batch = RecordBatchBuilder::default()
                    .with_stream_id(stream_id)
                    .with_base_offset(i * 10)
                    .with_last_offset_delta(10)
                    .with_range_index(index)
                    .with_payload(payload.clone())
                    .build()?;
                let flat_batch = Into::<FlatRecordBatch>::into(batch);
                let (slices, total) = flat_batch.encode();
                assert_eq!(
                    total as usize,
                    slices.iter().map(|s| s.len()).sum::<usize>()
                );
                for slice in &slices {
                    buf.extend_from_slice(slice);
                }
            }

            let response = client
                .append(&config.placement_driver, vec![buf.freeze()])
                .await?;

            assert_eq!(response.len(), BATCH as usize);
            Ok(())
        })
    }

    #[test]
    fn test_report_metrics() -> Result<(), EsError> {
        ulog::try_init_log();
        tokio_uring::start(async {
            #[allow(unused_variables)]
            let port = 2378;
            let port = run_listener().await;
            let mut config = config::Configuration {
                placement_driver: format!("127.0.0.1:{}", port),
                ..Default::default()
            };
            config.check_and_apply().unwrap();
            let config = Arc::new(config);
            let (tx, _rx) = broadcast::channel(1);
            let client = DefaultClient::new(Arc::clone(&config), tx);
            let uring_statistics = UringStatistics::new();
            let range_server_statistics = RangeServerStatistics::new();
            let disk_statistics = DiskStatistics::new();
            let memory_statistics = MemoryStatistics::new();
            client
                .report_metrics(
                    &config.placement_driver,
                    RangeServerState::RANGE_SERVER_STATE_READ_WRITE,
                    &uring_statistics,
                    &range_server_statistics,
                    &disk_statistics,
                    &memory_statistics,
                )
                .await
        })
    }

    #[test]
    fn test_list_resource() -> Result<(), EsError> {
        ulog::try_init_log();
        tokio_uring::start(async {
            #[allow(unused_variables)]
            let port = 2378;
            let port = run_listener().await;
            let mut config = config::Configuration {
                placement_driver: format!("127.0.0.1:{}", port),
                ..Default::default()
            };
            config.check_and_apply().unwrap();
            let config = Arc::new(config);
            let (tx, _rx) = broadcast::channel(1);
            let client = DefaultClient::new(Arc::clone(&config), tx);

            let result = client
                .list_resource(
                    vec![
                        ResourceType::RANGE_SERVER,
                        ResourceType::STREAM,
                        ResourceType::RANGE,
                        ResourceType::OBJECT,
                    ]
                    .as_ref(),
                    100,
                    &Option::None::<Bytes>,
                )
                .await
                .expect("List resource should not fail");

            assert_eq!(400, result.version);
            assert_eq!(None, result.continuation);
            assert_eq!(4, result.resources.len());
            check_range_server(&result.resources[0]);
            check_stream(&result.resources[1]);
            check_range(&result.resources[2]);
            check_object(&result.resources[3]);

            Ok(())
        })
    }

    #[test]
    fn test_watch_resource() -> Result<(), EsError> {
        ulog::try_init_log();
        tokio_uring::start(async {
            #[allow(unused_variables)]
            let port = 2378;
            let port = run_listener().await;
            let mut config = config::Configuration {
                placement_driver: format!("127.0.0.1:{}", port),
                ..Default::default()
            };
            config.check_and_apply().unwrap();
            let config = Arc::new(config);
            let (tx, _rx) = broadcast::channel(1);
            let client = DefaultClient::new(Arc::clone(&config), tx);

            let version = 100;
            let result = client
                .watch_resource(
                    vec![
                        ResourceType::RANGE_SERVER,
                        ResourceType::STREAM,
                        ResourceType::RANGE,
                        ResourceType::OBJECT,
                    ]
                    .as_ref(),
                    version,
                    std::time::Duration::from_secs(100),
                )
                .await
                .expect("Watch resource should not fail");

            assert_eq!(version + 3, result.version);
            assert_eq!(4, result.events.len());
            assert_eq!(EventType::Added, result.events[0].event_type);
            check_range_server(&result.events[0].resource);
            assert_eq!(EventType::Modified, result.events[1].event_type);
            check_stream(&result.events[1].resource);
            assert_eq!(EventType::Deleted, result.events[2].event_type);
            check_range(&result.events[2].resource);
            assert_eq!(EventType::Added, result.events[3].event_type);
            check_object(&result.events[3].resource);
            Ok(())
        })
    }

    fn check_range_server(resource: &Resource) {
        match resource {
            Resource::RangeServer(range_server) => {
                assert_eq!(42, range_server.server_id);
                assert_eq!(
                    String::from("127.0.0.1:10911"),
                    range_server.advertise_address
                );
            }
            _ => {
                panic!("Should be range server")
            }
        }
    }

    fn check_stream(resource: &Resource) {
        match resource {
            Resource::Stream(stream) => {
                assert_eq!(42, stream.stream_id.unwrap());
                assert_eq!(2, stream.replica);
                assert_eq!(1, stream.ack_count);
                assert_eq!(
                    std::time::Duration::from_secs(3600 * 24),
                    stream.retention_period
                );
            }
            _ => {
                panic!("Should be stream")
            }
        }
    }

    fn check_range(resource: &Resource) {
        match resource {
            Resource::Range(range) => {
                assert_eq!(42, range.stream_id());
                assert_eq!(0, range.index());
                assert_eq!(13, range.epoch());
                assert_eq!(0, range.start());
                assert_eq!(100, range.end().unwrap());
                assert_eq!(1, range.replica().len());
                assert_eq!(42, range.replica()[0].server_id);
                assert_eq!(2, range.replica_count());
                assert_eq!(1, range.ack_count());
            }
            _ => {
                panic!("Should be range")
            }
        }
    }

    fn check_object(resource: &Resource) {
        match resource {
            Resource::Object(object) => {
                assert_eq!(42, object.stream_id);
                assert_eq!(0, object.range_index);
                assert_eq!(1, object.epoch);
                assert_eq!(10, object.start_offset);
                assert_eq!(10, object.end_offset_delta);
                assert_eq!(1024, object.data_len);
            }
            _ => {
                panic!("Should be object")
            }
        }
    }
}
