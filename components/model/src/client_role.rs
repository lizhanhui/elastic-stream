#[derive(Debug)]
pub enum ClientRole {
    Unknown,
    PlacementManager,
    DataNode,
}

impl From<&ClientRole> for protocol::rpc::header::ClientRole {
    fn from(role: &ClientRole) -> Self {
        match role {
            ClientRole::Unknown => protocol::rpc::header::ClientRole::CLIENT_ROLE_UNKNOWN,
            ClientRole::PlacementManager => protocol::rpc::header::ClientRole::CLIENT_ROLE_PM,
            ClientRole::DataNode => protocol::rpc::header::ClientRole::CLIENT_ROLE_DATA_NODE,
        }
    }
}
