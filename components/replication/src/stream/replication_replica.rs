use std::{
    cell::RefCell,
    rc::{Rc, Weak},
    time::Duration,
};

use bytes::Bytes;
use client::client::Client;
use log::{error, info, warn};
use model::{
    error::EsError, range::RangeMetadata, request::fetch::FetchRequest,
    response::fetch::FetchResultSet, RangeServer,
};
use protocol::rpc::header::ErrorCode;
use protocol::rpc::header::SealKind;
use tokio::time::sleep;

#[cfg(test)]
use mockall::automock;

type AckCallback = Box<dyn Fn()>;

#[cfg_attr(test, automock)]
pub(crate) trait ReplicationReplica<C>
where
    C: Client + 'static,
{
    fn new(
        metadata: RangeMetadata,
        range_server: RangeServer,
        ack: AckCallback,
        client: Weak<C>,
    ) -> Self;

    fn confirm_offset(&self) -> u64;

    fn append(&self, flat_record_batch_bytes: Vec<Bytes>, base_offset: u64, last_offset: u64);

    async fn fetch(
        &self,
        start_offset: u64,
        end_offset: u64,
        batch_max_bytes: u32,
    ) -> Result<FetchResultSet, EsError>;

    async fn seal(&self, end_offset: Option<u64>) -> Result<u64, EsError>;

    fn corrupted(&self) -> bool;
}

/// Replicator is responsible for replicating data to a range-server of range replica.
///
/// It is created by ReplicationRange and is dropped when the range is sealed.
pub(crate) struct DefaultReplicationReplica<C>
where
    C: Client + 'static,
{
    log_ident: String,
    metadata: RangeMetadata,
    confirm_offset: Rc<RefCell<u64>>,
    range_server: RangeServer,
    corrupted: Rc<RefCell<bool>>,
    writable: Rc<RefCell<bool>>,
    ack_callback: Rc<Box<dyn Fn()>>,
    client: Weak<C>,
}

impl<C> DefaultReplicationReplica<C>
where
    C: Client + 'static,
{
    fn get_client(&self) -> Result<Rc<C>, EsError> {
        if let Some(client) = self.client.upgrade() {
            Ok(client)
        } else {
            Err(EsError::new(ErrorCode::UNEXPECTED, "client was dropped"))
        }
    }
}

impl<C> ReplicationReplica<C> for DefaultReplicationReplica<C>
where
    C: Client + 'static,
{
    /// Create a new replicator.
    ///
    /// # Arguments
    /// `range` - The replication range.
    /// `range_server` - The target range-server to replicate data to.
    fn new(
        metadata: RangeMetadata,
        range_server: RangeServer,
        ack: Box<dyn Fn()>,
        client: Weak<C>,
    ) -> Self {
        let confirm_offset = metadata.start();
        Self {
            log_ident: format!(
                "Replica[{}#{}-{}#{}] ",
                metadata.stream_id(),
                metadata.index(),
                range_server.server_id,
                range_server.advertise_address
            ),
            metadata: metadata.clone(),
            confirm_offset: Rc::new(RefCell::new(confirm_offset)),
            range_server,
            corrupted: Rc::new(RefCell::new(false)),
            writable: Rc::new(RefCell::new(true)),
            ack_callback: Rc::new(ack),
            client,
        }
    }

    fn confirm_offset(&self) -> u64 {
        // only sealed range replica has confirm offset.
        *self.confirm_offset.borrow()
    }

    fn append(&self, flat_record_batch_bytes: Vec<Bytes>, base_offset: u64, last_offset: u64) {
        let client = match self.get_client() {
            Ok(rst) => rst,
            Err(e) => {
                error!("{} replica append fail, {}", self.log_ident, e);
                self.corrupted.replace(true);
                return;
            }
        };
        let offset = Rc::clone(&self.confirm_offset);
        let target = self.range_server.advertise_address.clone();
        let corrupted = self.corrupted.clone();
        let writable = self.writable.clone();
        let ack = self.ack_callback.clone();

        // Spawn a task to replicate data to the target range-server.
        let log_ident = self.log_ident.clone();
        tokio_uring::spawn(async move {
            let mut attempts = 1;
            loop {
                if !*writable.borrow() {
                    info!("{}Range is sealed, aborting replication", log_ident);
                    break;
                }
                if *corrupted.borrow() {
                    ack();
                    break;
                }

                if attempts > 3 {
                    warn!("{log_ident}Failed to append entries(base_offset={base_offset}) after 3 attempts, aborting replication");
                    // TODO: Mark replication range as failing and incur seal immediately.
                    corrupted.replace(true);
                    break;
                }

                let result = client
                    .append(&target, flat_record_batch_bytes.clone())
                    .await;
                match result {
                    Ok(append_result_entries) => {
                        if append_result_entries.len() != 1 {
                            error!("{log_ident}Failed to append entries(base_offset={base_offset}): unexpected number of entries returned. Retry...");
                            attempts += 1;
                            sleep(Duration::from_millis(10)).await;
                            continue;
                        }
                        let status = &(append_result_entries[0].status);
                        if status.code != ErrorCode::OK {
                            warn!("{log_ident}Failed to append entries(base_offset={base_offset}): status code {status:?} is not OK. Retry...");
                            attempts += 1;
                            sleep(Duration::from_millis(10)).await;
                            continue;
                        }
                        let mut confirm_offset = offset.borrow_mut();
                        if *confirm_offset < last_offset {
                            *confirm_offset = last_offset;
                        }
                        break;
                    }
                    Err(e) => {
                        // TODO: inspect error and retry only if it's a network error.
                        // If the error is a protocol error, we should abort replication.
                        // If the range is sealed on range-server, we should abort replication and fire replication seal immediately.
                        warn!("{log_ident}Failed to append entries(base_offset={base_offset}): {e}. Retry...");
                        attempts += 1;
                        // TODO: Retry immediately?
                        sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                }
            }
            ack();
        });
    }

    async fn fetch(
        &self,
        start_offset: u64,
        end_offset: u64,
        batch_max_bytes: u32,
    ) -> Result<FetchResultSet, EsError> {
        let client = self.get_client()?;
        client
            .fetch(
                &self.range_server.advertise_address,
                FetchRequest {
                    max_wait: std::time::Duration::from_secs(3),
                    range: self.metadata.clone(),
                    offset: start_offset,
                    limit: end_offset,
                    min_bytes: None,
                    max_bytes: if batch_max_bytes > 0 {
                        Some(batch_max_bytes as usize)
                    } else {
                        None
                    },
                },
            )
            .await
    }

    /// Seal the range replica.
    /// - When range is open for write, then end_offset is Some(end_offset).
    /// - When range is created by old stream, then end_offset is None.
    async fn seal(&self, end_offset: Option<u64>) -> Result<u64, EsError> {
        self.writable.replace(false);
        let client = self.get_client()?;
        let mut metadata = self.metadata.clone();
        if let Some(end_offset) = end_offset {
            metadata.set_end(end_offset);
        }

        return match client
            .seal(
                Some(&self.range_server.advertise_address),
                SealKind::RANGE_SERVER,
                metadata,
            )
            .await
        {
            Ok(metadata) => {
                let end_offset = metadata.end().ok_or(EsError::new(
                    ErrorCode::UNEXPECTED,
                    "expect seal response contain end_offset",
                ))?;
                info!(
                    "{}Seal replica success with end_offset {end_offset}",
                    self.log_ident
                );
                *self.confirm_offset.borrow_mut() = end_offset;
                Ok(end_offset)
            }
            Err(e) => {
                error!("{}Seal replica fail, err: {e}", self.log_ident);
                Err(e)
            }
        };
    }

    fn corrupted(&self) -> bool {
        *self.corrupted.borrow()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use bytes::BytesMut;

    use client::client::MockClient;
    use model::{AppendResultEntry, Status};
    use protocol::rpc::header::RangeServerState;
    use tokio::sync::mpsc;

    use super::*;

    #[test]
    fn test_append() -> Result<(), Box<dyn Error>> {
        tokio_uring::start(async move {
            let metadata = RangeMetadata::new_range(0, 1, 2, 233, None, 1, 1);
            let client = MockClient::default();
            let mut client = Rc::new(client);
            let (tx, mut rx) = mpsc::unbounded_channel();
            let ack_callback = Box::new(move || {
                tx.send(()).unwrap();
            });
            let replica = DefaultReplicationReplica::new(
                metadata,
                RangeServer::new(233, "addr", RangeServerState::RANGE_SERVER_STATE_READ_WRITE),
                ack_callback,
                Rc::downgrade(&client),
            );

            unsafe {
                let client = Rc::get_mut_unchecked(&mut client);
                client.expect_append().times(1).returning(|_, _| {
                    Ok(vec![AppendResultEntry {
                        status: Status::ok(),
                        timestamp: chrono::offset::Utc::now(),
                    }])
                });
            }

            replica.append(vec![BytesMut::zeroed(1).freeze()], 233, 234);
            rx.recv().await.unwrap();
            assert_eq!(234, replica.confirm_offset());

            unsafe {
                let client = Rc::get_mut_unchecked(&mut client);
                client.expect_append().times(3).returning(|_, _| {
                    Ok(vec![AppendResultEntry {
                        status: Status::unspecified(),
                        timestamp: chrono::offset::Utc::now(),
                    }])
                });
            }
            assert!(!replica.corrupted());
            replica.append(vec![BytesMut::zeroed(1).freeze()], 234, 250);
            rx.recv().await.unwrap();
            assert!(replica.corrupted());
            assert_eq!(234, replica.confirm_offset());
        });
        Ok(())
    }

    #[test]
    fn test_fetch() -> Result<(), Box<dyn Error>> {
        tokio_uring::start(async move {
            let metadata = RangeMetadata::new_range(0, 1, 2, 233, None, 1, 1);
            let client = MockClient::default();
            let mut client = Rc::new(client);
            let ack_callback = Box::new(move || {});
            let replica = DefaultReplicationReplica::new(
                metadata,
                RangeServer::new(233, "addr", RangeServerState::RANGE_SERVER_STATE_READ_WRITE),
                ack_callback,
                Rc::downgrade(&client),
            );

            unsafe {
                let client = Rc::get_mut_unchecked(&mut client);
                client.expect_fetch().times(1).returning(|_, r| {
                    assert_eq!(240, r.offset);
                    assert_eq!(250, r.limit);
                    assert_eq!(Some(1000), r.max_bytes);
                    Err(EsError::unexpected("test mock error"))
                });
            }
            let rst = replica.fetch(240, 250, 1000).await;
            assert!(rst.is_err());
        });
        Ok(())
    }

    #[test]
    fn test_seal() -> Result<(), Box<dyn Error>> {
        tokio_uring::start(async move {
            let metadata = RangeMetadata::new_range(0, 1, 2, 233, None, 1, 1);
            let client = MockClient::default();
            let mut client = Rc::new(client);
            let ack_callback = Box::new(move || {});
            let replica = DefaultReplicationReplica::new(
                metadata,
                RangeServer::new(233, "addr", RangeServerState::RANGE_SERVER_STATE_READ_WRITE),
                ack_callback,
                Rc::downgrade(&client),
            );

            unsafe {
                let client = Rc::get_mut_unchecked(&mut client);
                client.expect_seal().times(1).returning(|_, _, m| {
                    assert_eq!(Some(250), m.end());
                    let mut m = m.clone();
                    m.set_end(240);
                    Ok(m)
                });
            }
            let end_offset = replica.seal(Some(250)).await.unwrap();
            assert_eq!(240, end_offset);
            assert_eq!(240, replica.confirm_offset());

            unsafe {
                let client = Rc::get_mut_unchecked(&mut client);
                client
                    .expect_seal()
                    .times(1)
                    .returning(|_, _, _m| Err(EsError::unexpected("test mock error")));
            }
            assert!(replica.seal(Some(250)).await.is_err());
        });
        Ok(())
    }
}
