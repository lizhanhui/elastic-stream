use super::{lb_policy::LbPolicy, session::Session};
use crate::{
    heartbeat::HeartbeatData, invocation_context::InvocationContext, request::Request,
    response::Response, state::SessionState,
};
use bytes::Bytes;
use config::Configuration;
use itertools::Itertools;
use local_sync::oneshot;
use log::{debug, error, info, trace, warn};
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
    AppendResultEntry, ListRangeCriteria, PlacementDriverNode,
};
use observation::metrics::{
    store_metrics::RangeServerStatistics,
    sys_metrics::{DiskStatistics, MemoryStatistics},
    uring_metrics::UringStatistics,
};
use protocol::rpc::header::{
    ErrorCode, PlacementDriverCluster, RangeServerState, ResourceType, StreamT,
};
use protocol::rpc::header::{OperationCode, SealKind};
use std::{
    cell::RefCell,
    collections::HashMap,
    io::ErrorKind,
    net::{SocketAddr, ToSocketAddrs},
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::broadcast,
    time::{self, timeout},
};
use tokio_uring::net::TcpStream;

use crate::{request, response, NodeRole};

pub(crate) struct CompositeSession {
    target: String,
    config: Arc<Configuration>,
    lb_policy: LbPolicy,
    endpoints: RefCell<Vec<SocketAddr>>,
    sessions: Rc<RefCell<HashMap<SocketAddr, Session>>>,
    shutdown: broadcast::Sender<()>,
    refresh_cluster_instant: RefCell<Instant>,
}

impl CompositeSession {
    pub(crate) async fn new<T>(
        target: T,
        config: Arc<Configuration>,
        lb_policy: LbPolicy,
        shutdown: broadcast::Sender<()>,
    ) -> Result<Self, EsError>
    where
        T: ToSocketAddrs + ToString,
    {
        let endpoints = RefCell::new(Vec::new());
        let sessions = Rc::new(RefCell::new(HashMap::new()));
        // For now, we just resolve one session out of the target.
        // In the future, we would support multiple internal connection and load balancing among them.
        for socket_addr in target
            .to_socket_addrs()
            .map_err(|e| EsError::new(ErrorCode::BAD_ADDRESS, &format!("bad address, {}", e)))?
        {
            let res = Self::connect(
                socket_addr,
                config.client_connect_timeout(),
                Arc::clone(&config),
                Rc::clone(&sessions),
                shutdown.clone(),
            )
            .await;
            match res {
                Ok(session) => {
                    endpoints.borrow_mut().push(socket_addr);
                    sessions.borrow_mut().insert(socket_addr, session);
                    info!("Connection to {} established", target.to_string());
                    break;
                }
                Err(_e) => {
                    info!("Failed to connect to {}", socket_addr);
                }
            }
        }

        Ok(Self {
            target: target.to_string(),
            config,
            lb_policy,
            endpoints,
            sessions,
            shutdown,
            refresh_cluster_instant: RefCell::new(Instant::now()),
        })
    }

    /// Check if we need to refresh placement driver cluster topology and leadership.
    ///
    /// # Returns
    /// `true` - if the interval has elapsed or the cluster has only one node;
    /// `false` - otherwise
    fn need_refresh_placement_driver_cluster(&self) -> bool {
        if self.target != self.config.placement_driver {
            return false;
        }

        let cluster_size = self.sessions.borrow().len();
        if cluster_size <= 1 {
            debug!("Placement Driver Cluster size is {} which is rare in production, flag refresh-cluster true", cluster_size);
            return true;
        }

        let now = Instant::now();
        *self.refresh_cluster_instant.borrow()
            + self
                .config
                .client_refresh_placement_driver_cluster_interval()
            <= now
    }

    /// Synchronize leadership of each placement driver node according to the specified description.
    ///
    /// # Parameter
    /// `nodes` - Placement driver nodes description from `DescribePlacementDriverClusterResponse`.
    fn refresh_leadership(&self, nodes: &[PlacementDriverNode]) {
        debug!("Refresh placement driver cluster leadership");
        // Sync leader/follower state
        for node in nodes.iter() {
            node.advertise_addr
                .to_socket_addrs()
                .into_iter()
                .flatten()
                .for_each(|addr| {
                    if let Some((_, session)) = self
                        .sessions
                        .borrow_mut()
                        .iter()
                        .find(|&entry| *entry.0 == addr)
                    {
                        session.set_role(if node.leader {
                            NodeRole::Leader
                        } else {
                            NodeRole::Follower
                        })
                    }
                });
        }
    }

    async fn refresh_sessions(&self, nodes: &Vec<PlacementDriverNode>) {
        if nodes.is_empty() {
            trace!("Placement Driver Cluster is empty, no need to refresh sessions");
            return;
        }

        // Update last-refresh-cluster-instant.
        *self.refresh_cluster_instant.borrow_mut() = Instant::now();

        let mut addrs = nodes
            .iter()
            .flat_map(|node| node.advertise_addr.to_socket_addrs().into_iter())
            .flatten()
            .filter(|socket_addr| socket_addr.is_ipv4())
            .dedup()
            .collect::<Vec<_>>();

        // Remove sessions that are no longer valid
        self.sessions
        .borrow_mut()
        .extract_if(|k, _v| !addrs.contains(k))
        .for_each(|(k, _v)| {
            self.endpoints.borrow_mut().retain(|e| {
                e != &k
            });
            info!("Session to {} will be disconnected because latest Placement Driver Cluster does not contain it any more", k);
        });

        addrs.retain(|addr| !self.sessions.borrow().contains_key(addr));

        addrs.iter().for_each(|addr| {
            trace!(
                "Create a new session for new Placement Driver Cluster member: {}",
                addr
            );
        });

        let futures = addrs.into_iter().map(|addr| {
            Self::connect(
                addr,
                self.config.client_connect_timeout(),
                Arc::clone(&self.config),
                Rc::clone(&self.sessions),
                self.shutdown.clone(),
            )
        });

        let res: Vec<Result<Session, EsError>> = futures::future::join_all(futures).await;
        for item in res {
            match item {
                Ok(session) => {
                    info!(
                        "Insert a session to composite-session {} using socket: {}",
                        self.target, session.target
                    );
                    self.endpoints.borrow_mut().push(session.target);
                    self.sessions.borrow_mut().insert(session.target, session);
                }
                Err(e) => {
                    error!("Failed to connect. {:?}", e);
                }
            }
        }
        self.refresh_leadership(nodes);
    }

    pub(crate) async fn refresh_placement_driver_cluster(&self) -> Result<(), EsError> {
        match self.describe_placement_driver_cluster().await? {
            Some(nodes) => {
                self.refresh_sessions(&nodes).await;
                Ok(())
            }
            None => {
                warn!("Placement driver returns an unexpected empty cluster. Skip refreshing sessions");
                Err(EsError::new(ErrorCode::UNEXPECTED, "Empty cluster"))
            }
        }
    }

    async fn pick_session(&self, lb_policy: LbPolicy) -> Option<Session> {
        match lb_policy {
            LbPolicy::LeaderOnly => {
                match self.pick_leader_session() {
                    Some(session) => Some(session),
                    None => {
                        trace!("No leader session found, try to describe placement driver cluster again");
                        if self.refresh_placement_driver_cluster().await.is_ok() {
                            return self.pick_leader_session();
                        }
                        None
                    }
                }
            }
            LbPolicy::PickFirst => self
                .sessions
                .borrow()
                .iter()
                .map(|(_, session)| session.clone())
                .next(),
        }
    }

    fn pick_leader_session(&self) -> Option<Session> {
        self.sessions
            .borrow()
            .iter()
            .filter(|&(_, session)| {
                trace!(
                    "State of session to {} is {:?}",
                    session.target,
                    session.role()
                );
                session.role() == NodeRole::Leader
            })
            .map(|(_, session)| session.clone())
            .next()
    }

    /// Broadcast heartbeat requests to all nested sessions.
    pub(crate) async fn heartbeat(&self, data: &HeartbeatData) {
        self.try_reconnect().await;

        if self.need_refresh_placement_driver_cluster()
            && self.refresh_placement_driver_cluster().await.is_err()
        {
            error!("Failed to refresh placement driver cluster");
        }

        let sessions = self
            .sessions
            .borrow()
            .iter()
            .map(|(_, session)| session.clone())
            .collect::<Vec<_>>();
        for session in sessions {
            session.heartbeat(data).await
        }
    }

    pub(crate) async fn allocate_id(
        &self,
        host: &str,
        timeout: Option<Duration>,
    ) -> Result<i32, EsError> {
        let request = request::Request {
            timeout: timeout.unwrap_or(self.config.client_io_timeout()),
            headers: request::Headers::AllocateId {
                host: host.to_owned(),
            },
            body: None,
        };
        let response = self.request(request).await?;
        if response.ok() {
            if let Some(response::Headers::AllocateId { id }) = response.headers {
                Ok(id)
            } else {
                unreachable!()
            }
        } else {
            error!(
                "Failed to allocate ID from {}. Status-Message: `{}`",
                self.target, response.status.message
            );
            Err(EsError::from(&response))
        }
    }

    async fn refresh_leadership_on_demand(&self, status: &model::Status) -> bool {
        if let Some(ref details) = status.details {
            if !details.is_empty() {
                match flatbuffers::root::<PlacementDriverCluster>(&details[..]) {
                    Ok(cluster) => {
                        let nodes = cluster
                            .unpack()
                            .nodes
                            .iter()
                            .map(Into::<PlacementDriverNode>::into)
                            .collect::<Vec<_>>();
                        if !nodes.is_empty() {
                            self.refresh_leadership(&nodes);
                            return true;
                        }
                    }
                    Err(e) => {
                        error!("Failed to decode `PlacementDriverCluster` using flatbuffers. Cause: {e}");
                    }
                }
            }
        }
        self.refresh_placement_driver_cluster().await.is_ok()
    }

    pub(crate) async fn create_stream(&self, stream: StreamT) -> Result<StreamMetadata, EsError> {
        let request = request::Request {
            timeout: self.config.client_io_timeout(),
            headers: request::Headers::CreateStream { stream },
            body: None,
        };
        let response = self.request(request).await?;
        if !response.ok() {
            error!(
                "Failed to create stream from {}. Status-Message: `{}`",
                self.target, response.status.message
            );
            // TODO: refine error handling
            return Err(EsError::from(&response));
        }
        if let Some(response::Headers::CreateStream { stream }) = response.headers {
            Ok(stream)
        } else {
            unreachable!()
        }
    }

    pub(crate) async fn describe_stream(&self, stream_id: u64) -> Result<StreamMetadata, EsError> {
        let request = request::Request {
            timeout: self.config.client_io_timeout(),
            headers: request::Headers::DescribeStream { stream_id },
            body: None,
        };
        let response = self.request(request).await?;
        if response.ok() {
            if let Some(response::Headers::DescribeStream { stream }) = response.headers {
                Ok(stream)
            } else {
                unreachable!()
            }
        } else {
            error!(
                "Failed to describe stream[stream-id={stream_id}] from {}. Status: `{:?}`",
                self.target, response.status
            );
            Err(EsError::from(&response))
        }
    }

    /// Create the specified range to the target: placement driver or range server.
    ///
    /// If the target is placement driver, we need to select the session to the primary node;
    pub(crate) async fn create_range(
        &self,
        range: RangeMetadata,
    ) -> Result<RangeMetadata, EsError> {
        let stream_id = range.stream_id();
        let request = request::Request {
            timeout: self.config.client_io_timeout(),
            headers: request::Headers::CreateRange {
                range: range.clone(),
            },
            body: None,
        };
        let response = self.request(request).await?;
        if response.ok() {
            if let Some(response::Headers::CreateRange { range }) = response.headers {
                Ok(range)
            } else {
                unreachable!()
            }
        } else {
            error!(
                "Failed to create range on {} for Stream[id={}]: {response:?}",
                self.target, stream_id
            );
            Err(EsError::from(&response))
        }
    }

    pub(crate) async fn list_range(
        &self,
        criteria: ListRangeCriteria,
    ) -> Result<Vec<RangeMetadata>, EsError> {
        let request = request::Request {
            timeout: self.config.client_io_timeout(),
            headers: request::Headers::ListRange { criteria },
            body: None,
        };
        let response = self.request(request).await?;
        if response.ok() {
            if let Some(response::Headers::ListRange { ranges }) = response.headers {
                ranges.ok_or(EsError::new(
                    ErrorCode::UNEXPECTED,
                    "None ranges in list range response",
                ))
            } else {
                unreachable!()
            }
        } else {
            error!(
                "Failed to list-ranges from {}. Status-Message: `{}`",
                self.target, response.status.message
            );
            Err(EsError::from(&response))
        }
    }

    /// Describe current placement driver cluster membership.
    ///
    /// There are multiple rationales for this RPC.
    /// 1. Placement driver is built on top of RAFT consensus algorithm and election happens in case of leader outage. Some RPCs
    ///    should steer to the leader node and need to refresh the leadership on failure;
    /// 2. Heartbeat, metrics-reporting RPC requests should broadcast to all placement driver nodes, so that when leader changes,
    ///    range-server liveness and load evaluation are not impacted.
    ///
    /// # Implementation walkthrough
    /// Step 1: If placement driver access URL uses domain name, resolve it;
    ///  1.1 If the result `SocketAddress` has an existing `Session`, re-use it and go to step 2;
    ///  1.2 If the result `SocketAddress` is completely new, connect and build a new `Session`
    /// Step 2: Send DescribePlacementDriverRequest to the `Session` discovered in step 1;
    /// Step 3: Once response is received from placement driver server, update the aggregated `Session` table, including leadership
    async fn describe_placement_driver_cluster(
        &self,
    ) -> Result<Option<Vec<PlacementDriverNode>>, EsError> {
        self.try_reconnect().await;

        // Get latest `A` records for access point domain name
        let addrs = self
            .target
            .to_socket_addrs()
            .map_err(|e| {
                error!("Failed to parse {} into SocketAddr: {:?}", self.target, e);
                EsError::new(
                    ErrorCode::BAD_ADDRESS,
                    &format!("describe pd cluster fail, bad address, {}", e),
                )
            })?
            .collect::<Vec<_>>();

        let (mut tx, rx) = oneshot::channel();
        let range_server = self.config.server.range_server();
        let request = request::Request {
            timeout: self.config.client_io_timeout(),
            headers: request::Headers::DescribePlacementDriver { range_server },
            body: None,
        };

        // TODO: we shall broadcast describe cluster request to all known nodes.
        let mut request_sent = false;
        'outer: loop {
            for addr in &addrs {
                let session = match self.sessions.borrow().get(addr) {
                    Some(session) => session.clone(),
                    None => continue,
                };

                if let Err(mut ctx) = session.write(request.clone(), tx).await {
                    tx = ctx
                        .response_observer()
                        .expect("Response observer should NOT be consumed");
                    error!("Failed to send request to {}", addr);
                    continue;
                }
                trace!("Describe placement driver cluster via {}", addr);
                request_sent = true;
                break 'outer;
            }

            if !request_sent {
                warn!(
                    "Failed to describe placement driver cluster via existing sessions. Try to re-connect..."
                );
                if let Some(addr) = addrs.first() {
                    let session = Self::connect(
                        *addr,
                        self.config.client_connect_timeout(),
                        Arc::clone(&self.config),
                        Rc::clone(&self.sessions),
                        self.shutdown.clone(),
                    )
                    .await?;
                    self.sessions.borrow_mut().insert(*addr, session);
                } else {
                    break;
                }
            }
        }

        if !request_sent {
            return Err(EsError::new(
                ErrorCode::UNEXPECTED,
                "describe pd cluster fail, request not send",
            ));
        }

        match time::timeout(self.config.client_io_timeout(), rx).await {
            Ok(response) => match response {
                Ok(response) => {
                    debug_assert_eq!(
                        response.operation_code,
                        OperationCode::DESCRIBE_PLACEMENT_DRIVER,
                        "Operation code should be describe-placement-driver"
                    );
                    if !response.ok() {
                        warn!(
                            "Failed to describe placement driver cluster: {:?}",
                            response.status
                        );
                        Err(EsError::from(&response))
                    } else if let Some(response::Headers::DescribePlacementDriver { nodes }) =
                        response.headers
                    {
                        trace!("Received placement driver cluster {:?}", nodes);
                        Ok(nodes)
                    } else {
                        Err(EsError::new(
                            ErrorCode::UNEXPECTED,
                            "describe pd cluster fail, empty headers",
                        ))
                    }
                }
                Err(_e) => Err(EsError::new(
                    ErrorCode::UNEXPECTED,
                    "describe pd cluster fail, recv error",
                )),
            },
            Err(_e) => Err(EsError::new(
                ErrorCode::RPC_TIMEOUT,
                "describe pd cluster timeout",
            )),
        }
    }

    /// Seal range on range-server or placement driver.
    ///
    /// # Implementation Walkthrough
    /// 1. If the seal kind is placement driver, find the session to the leader node;
    /// 2. Send the request to the session and await response;
    ///
    /// # Returns
    /// If seal kind is seal-placement-driver and renew, returns the newly created mutable range;
    /// Otherwise, return the range that is being sealed with the end properly filled.
    ///
    /// If the seal kind is seal-range-server, resulting `end` of `StreamRange` is range-server specific only.
    /// Final end value of the range will be resolved after MinCopy of range servers responded.
    pub(crate) async fn seal(
        &self,
        kind: SealKind,
        range: RangeMetadata,
    ) -> Result<RangeMetadata, EsError> {
        let request = request::Request {
            timeout: self.config.client_io_timeout(),
            headers: request::Headers::SealRange {
                kind,
                range: range.clone(),
            },
            body: None,
        };
        let response = self.request(request).await?;
        if !response.ok() {
            if response.status.code == ErrorCode::RANGE_ALREADY_SEALED {
                // TODO: check whether end_offset is match if the range is already sealed
                warn!("Range{range:?} already sealed");
                return Ok(range);
            }
            if response.status.code == ErrorCode::RANGE_NOT_FOUND {
                warn!("Range{range:?} not found, use start_offset as end_offset");
                return Ok(RangeMetadata::new(
                    range.stream_id(),
                    range.index(),
                    range.epoch(),
                    range.start(),
                    Some(range.start()),
                ));
            }
            warn!("Failed to seal range: {:?}", response.status);
            return Err(EsError::from(&response));
        }

        if let Some(response::Headers::SealRange { range }) = response.headers {
            trace!("Sealed range {:?}", range);
            range.ok_or(EsError::new(
                ErrorCode::UNEXPECTED,
                "seal range fail, empty range",
            ))
        } else {
            Err(EsError::new(
                ErrorCode::UNEXPECTED,
                "seal range fail, empty response headers",
            ))
        }
    }

    /// Try to reconnect to the endpoints that is absent from sessions.
    ///
    ///
    /// # Implementation walkthrough
    /// 1. filter endpoints that is not found in sessions
    /// 2. connect to the target and then create a session
    /// 3. if there is no existing sessions, await till connect attempts completed or failed.
    async fn try_reconnect(&self) {
        if self.endpoints.borrow().is_empty() {
            return;
        }

        if self.endpoints.borrow().len() > self.sessions.borrow().len() {
            let futures = self
                .endpoints
                .borrow()
                .iter()
                .filter(|target| !self.sessions.borrow().contains_key(target))
                .map(|target| {
                    Self::connect(
                        *target,
                        self.config.client_connect_timeout(),
                        Arc::clone(&self.config),
                        Rc::clone(&self.sessions),
                        self.shutdown.clone(),
                    )
                })
                .collect::<Vec<_>>();

            if futures.is_empty() {
                return;
            }
            trace!("Reconnecting {} sessions", futures.len());
            let need_await = self.sessions.borrow().is_empty();
            let sessions = Rc::clone(&self.sessions);
            let handle = tokio_uring::spawn(async move {
                futures::future::join_all(futures)
                    .await
                    .into_iter()
                    .for_each(|res| match res {
                        Ok(session) => {
                            let target = session.target;
                            sessions.borrow_mut().insert(target, session);
                        }
                        Err(e) => {
                            error!("Failed to connect. Cause: {:?}", e);
                        }
                    });
            });

            if need_await {
                let _ = handle.await;
            }
        }
    }

    async fn connect(
        addr: SocketAddr,
        duration: Duration,
        config: Arc<Configuration>,
        sessions: Rc<RefCell<HashMap<SocketAddr, Session>>>,
        shutdown: broadcast::Sender<()>,
    ) -> Result<Session, EsError> {
        trace!("Establishing connection to {:?}", addr);
        let endpoint = addr.to_string();
        let connect = TcpStream::connect(addr);
        let stream = match timeout(duration, connect).await {
            Ok(res) => match res {
                Ok(connection) => {
                    trace!("Connection to {:?} established", addr);
                    connection.set_nodelay(true).map_err(|e| {
                        error!("Failed to disable Nagle's algorithm. Cause: {:?}", e);
                        EsError::new(ErrorCode::CONNECT_DISABLE_NAGLE_FAIL, "Disable nagle fail")
                    })?;
                    connection
                }
                Err(e) => match e.kind() {
                    ErrorKind::ConnectionRefused => {
                        error!("Connection to {} is refused", endpoint);
                        return Err(EsError::new(
                            ErrorCode::CONNECT_REFUSED,
                            &format!("{:?}", endpoint),
                        ));
                    }
                    _ => {
                        return Err(EsError::new(
                            ErrorCode::CONNECT_FAIL,
                            &format!("{:?}", endpoint),
                        ));
                    }
                },
            },
            Err(e) => {
                let description = format!("Timeout when connecting {}, elapsed: {}", endpoint, e);
                error!("{}", description);
                return Err(EsError::new(
                    ErrorCode::RPC_TIMEOUT,
                    &format!("{:?}", endpoint),
                ));
            }
        };
        Ok(Session::new(
            addr,
            stream,
            &endpoint,
            &config,
            Rc::downgrade(&sessions),
            shutdown.clone(),
        ))
    }

    pub(crate) async fn append(&self, buf: Vec<Bytes>) -> Result<Vec<AppendResultEntry>, EsError> {
        let start_timestamp = Instant::now();
        let request = request::Request {
            timeout: self.config.client_io_timeout(),
            headers: request::Headers::Append,
            body: Some(buf),
        };
        let response = self.request(request).await?;
        if !response.ok() {
            warn!("Failed to append: {:?}", response.status);
            return Err(EsError::from(&response));
        }

        if let Some(response::Headers::Append { entries }) = response.headers {
            trace!(
                "Append entries {:?} cost {}us",
                entries,
                start_timestamp.elapsed().as_micros()
            );
            Ok(entries)
        } else {
            Err(EsError::new(
                ErrorCode::UNEXPECTED,
                "append fail, empty response headers",
            ))
        }
    }

    pub(crate) async fn fetch(&self, request: FetchRequest) -> Result<FetchResultSet, EsError> {
        let request = request::Request {
            timeout: request.max_wait,
            headers: request::Headers::Fetch { request },
            body: None,
        };
        let response = self.request(request).await?;
        if !response.ok() {
            warn!("Failed to fetch: {:?}", response.status);
            return Err(EsError::from(&response));
        }

        if let Some(response::Headers::Fetch {
            throttle,
            object_metadata_list,
        }) = response.headers
        {
            Ok(FetchResultSet {
                throttle,
                payload: response.payload,
                object_metadata_list,
            })
        } else {
            Err(EsError::new(
                ErrorCode::UNEXPECTED,
                "fetch fail, empty response headers",
            ))
        }
    }

    pub(crate) async fn report_metrics(
        &self,
        state: RangeServerState,
        uring_statistics: &UringStatistics,
        range_server_statistics: &RangeServerStatistics,
        disk_statistics: &DiskStatistics,
        memory_statistics: &MemoryStatistics,
    ) -> Result<(), EsError> {
        self.try_reconnect().await;

        if self.need_refresh_placement_driver_cluster()
            && self.refresh_placement_driver_cluster().await.is_err()
        {
            error!("Failed to refresh placement driver cluster");
        }

        // TODO: add disk_unindexed_data_size, range_missing_replica_cnt, range_active_cnt
        let mut range_server = self.config.server.range_server();
        range_server.state = state;
        let extension = request::Headers::ReportMetrics {
            range_server,
            disk_in_rate: disk_statistics.get_disk_in_rate(),
            disk_out_rate: disk_statistics.get_disk_out_rate(),
            disk_free_space: disk_statistics.get_disk_free_space(),
            disk_unindexed_data_size: 0,
            memory_used: memory_statistics.get_memory_used(),
            uring_task_rate: uring_statistics.get_uring_task_rate(),
            uring_inflight_task_cnt: uring_statistics.get_uring_inflight_task_cnt(),
            uring_pending_task_cnt: uring_statistics.get_uring_pending_task_cnt(),
            uring_task_avg_latency: uring_statistics.get_uring_task_avg_latency(),
            network_append_rate: range_server_statistics.get_network_append_rate(),
            network_fetch_rate: range_server_statistics.get_network_fetch_rate(),
            network_failed_append_rate: range_server_statistics.get_network_failed_append_rate(),
            network_failed_fetch_rate: range_server_statistics.get_network_failed_fetch_rate(),
            network_append_avg_latency: range_server_statistics.get_network_append_avg_latency(),
            network_fetch_avg_latency: range_server_statistics.get_network_fetch_avg_latency(),
            range_missing_replica_cnt: 0,
            range_active_cnt: 0,
        };
        let request = request::Request {
            timeout: self.config.client_io_timeout(),
            headers: extension,
            body: None,
        };

        let res = self.broadcast_to_pd(&request).await;
        for item in res {
            match item {
                Ok(response) => {
                    if !response.ok() {
                        error!(
                            "Failed to report metrics to {}. Status-Message: `{}`",
                            self.target, response.status.message
                        );
                        return Err(EsError::from(&response));
                    }
                }
                Err(_e) => {}
            }
        }
        Ok(())
    }

    pub(crate) async fn report_range_progress(
        &self,
        range_progress: Vec<RangeProgress>,
    ) -> Result<(), EsError> {
        self.try_reconnect().await;

        if self.need_refresh_placement_driver_cluster()
            && self.refresh_placement_driver_cluster().await.is_err()
        {
            error!("Failed to refresh placement driver cluster");
        }

        let headers = request::Headers::ReportRangeProgress {
            range_server: self.config.server.range_server(),
            range_progress,
        };
        let request = request::Request {
            timeout: self.config.client_io_timeout(),
            headers,
            body: None,
        };

        let res = self.broadcast_to_pd(&request).await;
        for item in res {
            match item {
                Ok(response) => {
                    if !response.ok() {
                        error!(
                            "Failed to report range progress to {}. Status-Message: `{}`",
                            self.target, response.status.message
                        );
                        return Err(EsError::from(&response));
                    }
                }
                Err(_e) => {}
            }
        }
        Ok(())
    }

    pub async fn commit_object(&self, metadata: ObjectMetadata) -> Result<(), EsError> {
        let request = request::Request {
            timeout: self.config.client_io_timeout(),
            headers: request::Headers::CommitObject { metadata },
            body: None,
        };
        let response = self.request(request).await?;

        if response.ok() {
            Ok(())
        } else {
            error!(
                "Failed to commit object from {}. Status: `{:?}`",
                self.target, response.status
            );
            Err(EsError::from(&response))
        }
    }

    pub async fn list_resource(
        &self,
        types: &[ResourceType],
        limit: i32,
        continuation: &Option<Bytes>,
    ) -> Result<ListResourceResult, EsError> {
        let request = request::Request {
            timeout: self.config.client_io_timeout(),
            headers: request::Headers::ListResource {
                resource_type: types.to_owned(),
                limit,
                continuation: continuation.clone(),
            },
            body: None,
        };
        let response = self.request(request).await?;

        if response.ok() {
            match response.headers {
                Some(response::Headers::ListResource {
                    resources,
                    version,
                    continuation,
                }) => Ok(ListResourceResult {
                    resources,
                    version,
                    continuation,
                }),
                _ => unreachable!(),
            }
        } else {
            error!(
                "Failed to list resource from {}. Status: `{:?}`",
                self.target, response.status
            );
            Err(EsError::from(&response))
        }
    }

    pub async fn watch_resource(
        &self,
        types: &[ResourceType],
        version: i64,
        timeout: Duration,
    ) -> Result<WatchResourceResult, EsError> {
        let request = request::Request {
            timeout,
            headers: request::Headers::WatchResource {
                resource_type: types.to_owned(),
                version,
            },
            body: None,
        };
        let response = self.request(request).await?;

        if response.ok() {
            match response.headers {
                Some(response::Headers::WatchResource { events, version }) => {
                    Ok(WatchResourceResult { events, version })
                }
                _ => unreachable!(),
            }
        } else {
            error!(
                "Failed to watch resource from {}. Status: `{:?}`",
                self.target, response.status
            );
            Err(EsError::from(&response))
        }
    }

    pub async fn update_stream(
        &self,
        stream_id: u64,
        replica_count: Option<u8>,
        ack_count: Option<u8>,
        epoch: Option<u64>,
    ) -> Result<StreamMetadata, EsError> {
        let request = request::Request {
            timeout: self.config.client_io_timeout(),
            headers: request::Headers::UpdateStream {
                stream_id,
                replica_count,
                ack_count,
                epoch,
            },
            body: None,
        };
        let response = self.request(request).await?;
        if response.ok() {
            match response.headers {
                Some(response::Headers::UpdateStream { metadata }) => Ok(metadata),
                _ => unreachable!(),
            }
        } else {
            Err(EsError::from(&response))
        }
    }

    pub async fn trim_stream(
        &self,
        stream_id: u64,
        epoch: u64,
        min_offset: u64,
    ) -> Result<(), EsError> {
        let request = request::Request {
            timeout: self.config.client_io_timeout(),
            headers: request::Headers::TrimStream {
                stream_id,
                epoch,
                min_offset,
            },
            body: None,
        };
        let response = self.request(request).await?;
        if response.ok() {
            Ok(())
        } else {
            Err(EsError::from(&response))
        }
    }

    pub async fn delete_stream(&self, stream_id: u64, epoch: u64) -> Result<(), EsError> {
        let request = request::Request {
            timeout: self.config.client_io_timeout(),
            headers: request::Headers::DeleteStream { stream_id, epoch },
            body: None,
        };
        let response = self.request(request).await?;
        if response.ok() {
            Ok(())
        } else {
            Err(EsError::from(&response))
        }
    }

    async fn broadcast_to_pd(
        &self,
        request: &Request,
    ) -> Vec<Result<Response, oneshot::error::RecvError>> {
        let mut receivers = vec![];
        {
            let sessions = self
                .sessions
                .borrow()
                .iter()
                .map(|(_socket, session)| session.clone())
                .collect::<Vec<_>>();

            let futures = sessions
                .iter()
                .map(|session| {
                    let (tx, rx) = oneshot::channel();
                    receivers.push(rx);
                    session.write(request.clone(), tx)
                })
                .collect::<Vec<_>>();

            let _res: Vec<Result<(), InvocationContext>> = futures::future::join_all(futures).await;
        }
        futures::future::join_all(receivers).await
    }

    async fn request(&self, request: Request) -> Result<Response, EsError> {
        loop {
            self.try_reconnect().await;
            let session = self.pick_session(self.lb_policy).await.ok_or(EsError::new(
                ErrorCode::CONNECT_FAIL,
                &format!("{:?}", self.target),
            ))?;
            let (tx, rx) = oneshot::channel();
            if let Err(ctx) = session.write(request.clone(), tx).await {
                error!(
                    "Failed to send request to {}. Cause: {:?}",
                    self.target, ctx
                );
                return Err(EsError::new(ErrorCode::CONNECT_REFUSED, &self.target));
            }

            let response = rx.await.map_err(|e| {
                error!(
                    "Internal error while request from {}. Cause: {:?}",
                    self.target, e
                );
                EsError::new(ErrorCode::UNEXPECTED, "channel closed")
            })?;

            if !response.ok()
                && ErrorCode::PD_NOT_LEADER == response.status.code
                && self.refresh_leadership_on_demand(&response.status).await
            {
                // Retry after refresh leadership
                continue;
            }
            return Ok(response);
        }
    }

    pub fn go_away(&self) -> bool {
        for (_, session) in self.sessions.borrow().iter() {
            if session.state() == SessionState::GoAway {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::CompositeSession;
    use crate::lb_policy::LbPolicy;
    use mock_server::run_listener;
    use std::{error::Error, sync::Arc};
    use tokio::sync::broadcast;

    #[test]
    fn test_new() -> Result<(), Box<dyn Error>> {
        let config = Arc::new(config::Configuration::default());
        tokio_uring::start(async {
            let port = run_listener().await;
            let target = format!("{}:{}", "localhost", port);
            let (shutdown_tx, _shutdown_rx) = broadcast::channel(1);
            let _session =
                CompositeSession::new(&target, config, LbPolicy::PickFirst, shutdown_tx).await?;

            Ok(())
        })
    }

    #[test]
    fn test_describe_placement_driver_cluster() -> Result<(), Box<dyn Error>> {
        ulog::try_init_log();
        let mut config = config::Configuration::default();
        config.server.server_id = 1;
        let config = Arc::new(config);
        tokio_uring::start(async {
            let port = run_listener().await;
            let target = format!("{}:{}", "localhost", port);
            let (shutdown_tx, _shutdown_rx) = broadcast::channel(1);
            let composite_session =
                CompositeSession::new(&target, config, LbPolicy::PickFirst, shutdown_tx).await?;
            let nodes = composite_session
                .describe_placement_driver_cluster()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(1, nodes.len());
            Ok(())
        })
    }
}
