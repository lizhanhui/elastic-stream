#[derive(Debug)]
pub enum Role {
    Leader,
    Follower,
}

#[derive(Debug)]
pub struct Node {
    pub(crate) name: String,
    pub(crate) advertise_address: String,
    pub(crate) role: Role,
}
