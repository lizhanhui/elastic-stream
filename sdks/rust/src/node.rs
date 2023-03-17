#[derive(Debug)]
pub enum Role {
    Leader,
    Follower,
}

#[derive(Debug)]
pub struct Node {
    id: usize,
    advertise_address: String,
    role: Role,
}
