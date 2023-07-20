use protocol::rpc::header::{
    self, ObjT, RangeServerT, RangeT, ResourceEventT, ResourceT, ResourceType, StreamT,
};

use crate::{object::ObjectMetadata, range::RangeMetadata, stream::StreamMetadata, RangeServer};

#[derive(Debug, Clone)]
pub enum Resource {
    NONE,
    RangeServer(RangeServer),
    Stream(StreamMetadata),
    Range(RangeMetadata),
    Object(ObjectMetadata),
}

impl From<&ResourceT> for Resource {
    fn from(t: &ResourceT) -> Self {
        match t.type_ {
            ResourceType::RANGE_SERVER => {
                let range_server = if let Some(rs) = t.range_server.as_ref() {
                    RangeServer::from(rs.as_ref())
                } else {
                    RangeServer::from(&RangeServerT::default())
                };
                Resource::RangeServer(range_server)
            }
            ResourceType::STREAM => {
                let stream = if let Some(s) = t.stream.as_ref() {
                    StreamMetadata::from(s.as_ref())
                } else {
                    StreamMetadata::from(&StreamT::default())
                };
                Resource::Stream(stream)
            }
            ResourceType::RANGE => {
                let range = if let Some(r) = t.range.as_ref() {
                    RangeMetadata::from(r.as_ref())
                } else {
                    RangeMetadata::from(&RangeT::default())
                };
                Resource::Range(range)
            }
            ResourceType::OBJECT => {
                let object = if let Some(o) = t.object.as_ref() {
                    ObjectMetadata::from(o.as_ref())
                } else {
                    ObjectMetadata::from(&ObjT::default())
                };
                Resource::Object(object)
            }
            ResourceType::UNKNOWN => Resource::NONE,
            ResourceType(i8::MIN..=ResourceType::ENUM_MIN) => Resource::NONE,
            ResourceType(ResourceType::ENUM_MAX..=i8::MAX) => Resource::NONE,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum EventType {
    NONE,
    ADDED,
    MODIFIED,
    DELETED,
}

#[derive(Debug, Clone)]
pub struct ResourceEvent {
    pub event_type: EventType,
    pub resource: Resource,
}

impl From<&ResourceEventT> for ResourceEvent {
    fn from(t: &ResourceEventT) -> Self {
        Self {
            event_type: match t.type_ {
                header::EventType::ADDED => EventType::ADDED,
                header::EventType::MODIFIED => EventType::MODIFIED,
                header::EventType::DELETED => EventType::DELETED,
                _ => EventType::NONE,
            },
            resource: Resource::from(t.resource.as_ref()),
        }
    }
}
