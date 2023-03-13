use super::data_node::DataNode;

#[derive(Debug)]
pub enum RangeCriteria {
    DataNode(DataNode),
    StreamId(i64),
}
