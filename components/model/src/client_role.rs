#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum ClientRole {
    Unspecified,
    PlacementDriver,
    RangeServer,
    Frontend,
}

impl From<ClientRole> for protocol::rpc::header::ClientRole {
    fn from(role: ClientRole) -> Self {
        match role {
            ClientRole::Unspecified => protocol::rpc::header::ClientRole::CLIENT_ROLE_UNSPECIFIED,
            ClientRole::PlacementDriver => protocol::rpc::header::ClientRole::CLIENT_ROLE_PD,
            ClientRole::RangeServer => protocol::rpc::header::ClientRole::CLIENT_ROLE_RANGE_SERVER,
            ClientRole::Frontend => protocol::rpc::header::ClientRole::CLIENT_ROLE_FRONTEND,
        }
    }
}

#[cfg(test)]
mod tests {
    use protocol::rpc::header;

    #[test]
    fn test_conversion() {
        assert_eq!(
            Into::<header::ClientRole>::into(super::ClientRole::Frontend),
            header::ClientRole::CLIENT_ROLE_FRONTEND
        );
        assert_eq!(
            Into::<header::ClientRole>::into(super::ClientRole::RangeServer),
            header::ClientRole::CLIENT_ROLE_RANGE_SERVER
        );
        assert_eq!(
            Into::<header::ClientRole>::into(super::ClientRole::PlacementDriver),
            header::ClientRole::CLIENT_ROLE_PD
        );
        assert_eq!(
            Into::<header::ClientRole>::into(super::ClientRole::Unspecified),
            header::ClientRole::CLIENT_ROLE_UNSPECIFIED
        );
    }
}
