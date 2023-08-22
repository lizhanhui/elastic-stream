use protocol::rpc::header::{
    self, ObjT, RangeServerT, RangeT, ResourceEventT, ResourceT, ResourceType, StreamT,
};

use crate::{object::ObjectMetadata, range::RangeMetadata, stream::StreamMetadata, RangeServer};

#[derive(Debug, Clone)]
pub enum Resource {
    None,
    RangeServer(RangeServer),
    Stream(StreamMetadata),
    Range(RangeMetadata),
    Object(ObjectMetadata),
}

impl From<&ResourceT> for Resource {
    fn from(t: &ResourceT) -> Self {
        match t.type_ {
            ResourceType::RESOURCE_RANGE_SERVER => {
                let range_server = if let Some(rs) = t.range_server.as_ref() {
                    RangeServer::from(rs.as_ref())
                } else {
                    RangeServer::from(&RangeServerT::default())
                };
                Resource::RangeServer(range_server)
            }
            ResourceType::RESOURCE_STREAM => {
                let stream = if let Some(s) = t.stream.as_ref() {
                    StreamMetadata::from(s.as_ref())
                } else {
                    StreamMetadata::from(&StreamT::default())
                };
                Resource::Stream(stream)
            }
            ResourceType::RESOURCE_RANGE => {
                let range = if let Some(r) = t.range.as_ref() {
                    RangeMetadata::from(r.as_ref())
                } else {
                    RangeMetadata::from(&RangeT::default())
                };
                Resource::Range(range)
            }
            ResourceType::RESOURCE_OBJECT => {
                let object = if let Some(o) = t.object.as_ref() {
                    ObjectMetadata::from(o.as_ref())
                } else {
                    ObjectMetadata::from(&ObjT::default())
                };
                Resource::Object(object)
            }
            ResourceType::RESOURCE_UNKNOWN => Resource::None,
            ResourceType(i8::MIN..=ResourceType::ENUM_MIN) => Resource::None,
            ResourceType(ResourceType::ENUM_MAX..=i8::MAX) => Resource::None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum EventType {
    None,

    /// When a resource is listed, it means that the resource is already there before the watch starts.
    Listed,

    /// It indicates that all resources have been listed.
    ListFinished,

    /// The following events are only valid for watch.
    /// The resource is newly added.
    Added,

    /// The resource is there before, and is modified.
    Modified,

    /// The resource is there before, and is deleted.
    Deleted,

    /// It indicates that an error occurs during list or watch.
    Reset,
}

/// The event of a resource.
///
/// # Scenarios
/// Here are all the scenarios that can trigger a resource event.
///
/// ## [`RangeServer`]
///
/// ### [`Added`]
/// A [`RangeServer`] with new [`RangeServer::server_id`] sends a heartbeat to PD.
///
/// ### [`Modified`]
/// A [`RangeServer`] with existing [`RangeServer::server_id`] sends a heartbeat to PD,
/// and [`RangeServer::advertise_address`] or [`RangeServer::state`] is changed.
///
/// ### [`Deleted`]
/// Will never happen.
///
///
/// ## [`Stream`]
///
/// ### [`Added`]
/// A [`Stream`] is created.
///
/// ### [`Modified`]
/// Here are several scenarios that can trigger a [`Modified`] event:
/// * [`StreamMetadata::replica`], [`StreamMetadata::ack_count`] or [`StreamMetadata::retention_period`] changed
/// The [`Stream`] is updated.
/// * [`StreamMetadata::start_offset`] increased
/// The [`Stream`] is trimmed to the new start offset.
/// * [`StreamMetadata::epoch`] increased
/// The [`Stream`] updates its epoch.
/// * [`StreamMetadata::deleted`] set to `true`
/// The [`Stream`] is deleted.
///
/// ### [`Deleted`]
/// After [`StreamMetadata::deleted`] is set to `true` for a period of time (default to 24h), the [`Deleted`] event is triggered.
///
///
/// ## [`Range`]
///
/// ### [`Added`]
/// A [`Range`] is created.
///
/// ### [`Modified`]
/// Here are several scenarios that can trigger a [`Modified`] event:
/// * [`RangeMetadata::start`] increased
/// The stream of [`RangeMetadata::stream_id`] is trimmed, and the [`Range`] covers the new start offset of the stream.
/// * [`RangeMetadata::end`] set from [`Option::None`] to [`Option::Some`]
/// The [`Range`] is sealed at the given end offset.
/// * [`RangeMetadata::offload_owner`] updated
/// The offload owner of the [`Range`] is changed.
///
/// ### [`Deleted`]
/// Here are two scenarios that can trigger a [`Deleted`] event:
/// * The stream of [`RangeMetadata::stream_id`] is deleted.
/// * The stream of [`RangeMetadata::stream_id`] is trimmed, and the [`Range`] falls out of the new start offset of the stream.
///
///
/// ## [`Object`]
///
/// ### [`Added`]
/// The [`Object`] is uploaded and committed.
/// TODO: prepare the object
///
/// ### [`Modified`]
/// Will never happen.
/// TODO: commit the object
///
/// ### [`Deleted`]
/// Here are two scenarios that can trigger a [`Deleted`] event:
/// * The stream of [`ObjectMetadata::stream_id`] is deleted.
/// * The stream of [`ObjectMetadata::stream_id`] is trimmed, and the [`Object`] falls out of the new start offset of the stream.
/// NOTE: [`Object`]s are deleted asynchronously, so the [`Deleted`] event may be delayed.
///
///
/// [`RangeServer`]: Resource::RangeServer
/// [`Stream`]: Resource::Stream
/// [`Range`]: Resource::Range
/// [`Object`]: Resource::Object
/// [`Added`]: EventType::Added
/// [`Modified`]: EventType::Modified
/// [`Deleted`]: EventType::Deleted
#[derive(Debug, Clone)]
pub struct ResourceEvent {
    /// The type of the event, indicating what happened to the resource.
    pub event_type: EventType,

    /// The resource that the event is about.
    /// If the event type is [`EventType::Listed`], [`EventType::Added`] or [`EventType::Modified`], the resource is the one after the change.
    /// If the event type is [`EventType::Deleted`], the resource is the one before deletion.
    /// If the event type is [`EventType::ListFinished`] or [`EventType::Reset`], the resource will be [`Resource::None`].
    pub resource: Resource,
}

impl From<&ResourceEventT> for ResourceEvent {
    fn from(t: &ResourceEventT) -> Self {
        Self {
            event_type: match t.type_ {
                header::EventType::EVENT_ADDED => EventType::Added,
                header::EventType::EVENT_MODIFIED => EventType::Modified,
                header::EventType::EVENT_DELETED => EventType::Deleted,
                _ => EventType::None,
            },
            resource: Resource::from(t.resource.as_ref()),
        }
    }
}

impl ResourceEvent {
    pub fn list_finished() -> Self {
        Self {
            event_type: EventType::ListFinished,
            resource: Resource::None,
        }
    }

    pub fn reset() -> Self {
        Self {
            event_type: EventType::Reset,
            resource: Resource::None,
        }
    }
}

/// Metadata changes are defined as `ResourceEvent`; This trait defines contracts for components to implement if they have interest in metadata changes.
///
/// Components that are interested in metadata and its changes need to implement this trait and register itself
/// into `MetadataManager`.
///
pub trait ResourceEventObserver {
    /// This method feeds metadata and changes to components that are interested
    fn on_resource_event(&self, event: &ResourceEvent);
}
