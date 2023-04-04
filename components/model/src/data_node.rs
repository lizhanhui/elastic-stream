use protocol::rpc::header::DataNodeT;

#[derive(Debug, Clone)]
pub struct DataNode {
    pub node_id: i32,
    pub advertise_address: String,
}

impl DataNode {
    pub fn new<Addr>(id: i32, address: Addr) -> Self
    where
        Addr: AsRef<str>,
    {
        Self {
            node_id: id,
            advertise_address: address.as_ref().to_owned(),
        }
    }
}

impl From<&DataNode> for DataNodeT {
    fn from(value: &DataNode) -> Self {
        let mut ret = DataNodeT::default();
        ret.node_id = value.node_id;
        ret.advertise_addr = Some(value.advertise_address.to_owned());
        ret
    }
}
