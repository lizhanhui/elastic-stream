pub mod error;

pub trait Record<'a> {
    fn partition(&self) -> Option<u32>;

    fn offset(&self) -> Option<u64>;

    fn data(&self) -> &'a [u8];
}

pub mod range;

pub mod partition;
