use std::fmt::{self, Display, Formatter};

use derivative::Derivative;
use log::info;
use protocol::rpc::header::{OffloadOwnerT, RangeServerT, RangeT};

use crate::range_server::RangeServer;

/// The owner of the range which offloads the data to the object storage.
#[derive(Debug, PartialEq, Clone)]
pub struct OffloadOwner {
    /// The owner's [`server_id`].
    ///
    /// [`server_id`]: RangeServer::server_id
    pub server_id: i32,

    /// The epoch of the owner, which is used to check the validity of the owner.
    pub epoch: i16,
}

impl From<&OffloadOwner> for OffloadOwnerT {
    fn from(value: &OffloadOwner) -> Self {
        let mut owner = OffloadOwnerT::default();
        owner.server_id = value.server_id;
        owner.epoch = value.epoch;
        owner
    }
}

impl From<&OffloadOwnerT> for OffloadOwner {
    fn from(value: &OffloadOwnerT) -> Self {
        Self {
            server_id: value.server_id,
            epoch: value.epoch,
        }
    }
}

/// Representation of a stream range in form of `[start, end)` in which `start` is inclusive and `end` is exclusive.
/// If `start` == `end`, there will be no valid records in the range.
///
/// At the beginning, `end` will be `None` and it would grow as more slots are taken from the range.
/// Once the range is sealed, it becomes immutable and its right boundary becomes fixed.
/// TODO: add RangeReplicaMeta
#[derive(Derivative)]
#[derivative(Debug, PartialEq, Clone)]
pub struct RangeMetadata {
    stream_id: i64,

    epoch: u64,

    /// Range index
    index: i32,

    /// The start slot index, inclusive.
    start: u64,

    /// The end of the range, exclusive
    end: Option<u64>,

    /// List of range servers, that all have identical records within the range.
    #[derivative(PartialEq = "ignore")]
    replica: Vec<RangeServer>,

    /// The range replica expected count. When cluster servers count is less than replica count but
    /// but larger than ack_count, the range can still be successfully created.
    replica_count: u8,

    /// The range replica ack count, only success ack >= ack_count, then the write is success.
    /// For seal range, success seal range must seal replica count >= (replica_count - ack_count + 1)
    ack_count: u8,

    /// The owner of the range which offloads the data to the object storage.
    /// If [`replica`] is empty, [`offload_owner`] will be None.
    offload_owner: Option<OffloadOwner>,
}

impl RangeMetadata {
    // TODO: replace with new_range, after add RangeReplicaMeta
    pub fn new(stream_id: i64, index: i32, epoch: u64, start: u64, end: Option<u64>) -> Self {
        Self {
            stream_id,
            index,
            epoch,
            start,
            end,
            replica: vec![],
            replica_count: 0,
            ack_count: 0,
            offload_owner: Some(OffloadOwner {
                server_id: 0,
                epoch: 0,
            }),
        }
    }

    pub fn new_range(
        stream_id: i64,
        index: i32,
        epoch: u64,
        start: u64,
        end: Option<u64>,
        replica_count: u8,
        ack_count: u8,
    ) -> Self {
        Self {
            stream_id,
            index,
            epoch,
            start,
            end,
            replica: vec![],
            replica_count,
            ack_count,
            offload_owner: Some(OffloadOwner {
                server_id: 0,
                epoch: 0,
            }),
        }
    }

    pub fn replica(&self) -> &Vec<RangeServer> {
        &self.replica
    }

    pub fn replica_mut(&mut self) -> &mut Vec<RangeServer> {
        &mut self.replica
    }

    /// Whether the range is held by the given server or not.
    pub fn held_by(&self, server_id: i32) -> bool {
        self.replica.iter().any(|s| s.server_id == server_id)
    }

    /// Test if the given offset is within the range.
    pub fn contains(&self, offset: u64) -> bool {
        // FIXME: use the nextOffset/confirmOffset to check end contains
        if self.start > offset {
            return false;
        }

        match self.end {
            None => true,
            Some(end) => offset < end,
        }
    }

    pub fn stream_id(&self) -> i64 {
        self.stream_id
    }

    pub fn index(&self) -> i32 {
        self.index
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn end(&self) -> Option<u64> {
        self.end
    }

    pub fn replica_count(&self) -> u8 {
        self.replica_count
    }

    pub fn ack_count(&self) -> u8 {
        self.ack_count
    }

    pub fn offload_owner(&self) -> &Option<OffloadOwner> {
        &self.offload_owner
    }

    pub fn has_end(&self) -> bool {
        self.end.is_some()
    }

    pub fn set_end(&mut self, end: u64) {
        if let Some(ref mut e) = self.end {
            info!("Change range end from {} to {}", *e, end);
            *e = end;
        } else {
            self.end = Some(end);
        }
    }
}

impl Display for RangeMetadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{stream-id={}, index={}}}=[{}, {:?})",
            self.stream_id, self.index, self.start, self.end
        )
    }
}

impl From<&RangeMetadata> for RangeT {
    fn from(value: &RangeMetadata) -> Self {
        let mut range = RangeT::default();
        range.stream_id = value.stream_id;
        range.epoch = value.epoch as i64;
        range.index = value.index;
        range.start = value.start as i64;
        range.end = match value.end {
            None => -1,
            Some(offset) => offset as i64,
        };
        let mut replica: Vec<RangeServerT> = vec![];
        for server in &value.replica {
            replica.push(server.into());
        }
        if replica.is_empty() {
            range.servers = None;
        } else {
            range.servers = Some(replica);
        }
        range.replica_count = value.replica_count as i8;
        range.ack_count = value.ack_count as i8;
        range.offload_owner = value
            .offload_owner
            .as_ref()
            .map(|o| Box::new(OffloadOwnerT::from(o)));
        range
    }
}

impl From<&RangeT> for RangeMetadata {
    fn from(value: &RangeT) -> Self {
        let mut replica: Vec<RangeServer> = vec![];
        if let Some(servers) = &value.servers {
            for server in servers {
                replica.push(server.into());
            }
        }
        Self {
            stream_id: value.stream_id,
            epoch: value.epoch as u64,
            index: value.index,
            start: value.start as u64,
            end: match value.end {
                -1 => None,
                offset => Some(offset as u64),
            },
            replica,
            replica_count: value.replica_count as u8,
            ack_count: value.ack_count as u8,
            offload_owner: value
                .offload_owner
                .as_ref()
                .map(|o| OffloadOwner::from(o.as_ref())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_take_slot() {
        let mut range = RangeMetadata::new(0, 0, 0, 0, None);
        assert!(!range.has_end());

        // Double seal should return the same offset.
        range.set_end(100);
        assert!(range.has_end());
        assert_eq!(range.end(), Some(100));
    }

    #[test]
    fn test_contains() {
        // Test a sealed range that contains a given offset.
        let range = RangeMetadata::new(0, 0, 0, 0, Some(10));
        assert!(range.contains(0));
        assert!(range.contains(1));
        assert!(!range.contains(11));

        // Test a open range that contains a given offset.
        let range = RangeMetadata::new(0, 0, 0, 10, None);
        assert!(!range.contains(0));
        assert!(range.contains(11));
        assert!(range.contains(21));
    }
}
