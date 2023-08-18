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
