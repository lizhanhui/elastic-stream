use crate::error::ServiceError;
use log::{error, warn};
use model::Batch;

/// Append Request Window ensures append requests of a stream range are dispatched to store in order.
///
/// Note that `Window` is intended to be used by a thread-per-core scenario and is not thread-safe.
///
/// # Design
/// Each `Window` has two offset, `next` and `committed`.
/// * `next` is the next exact offset of the request to be dispatched.
/// * `committed` is the committed offset of the stream range.
///
/// The below rules show the relationship between `next` and `committed`:
/// * `next` is always greater than or equal to `committed`.
/// * The request with offset equal to `next` is the expected next request to be dispatched.
/// * The request with offset less than `committed` should be responded with a `ServiceError::OffsetCommitted`.
/// * The request with offset greater than `next` should be responded with a `ServiceError::OffsetOutOfOrder`.
/// * The other requests should be responded with a `ServiceError::OffsetInWindow`.
#[derive(Debug)]
pub(crate) struct Window {
    /// Identifier of the window. Currently is in form of {stream-id}#{range-index}.
    log_ident: String,

    /// The barrier offset, the requests beyond this offset should be blocked.
    next: u64,

    /// The committed offset means all records prior to this offset are already persisted to store.
    committed: u64,
}

impl Window {
    pub(crate) fn new(log_ident: String, next: u64) -> Self {
        Self {
            log_ident,
            next,
            // The initial commit offset is the same as the next offset.
            committed: next,
        }
    }

    pub fn next(&self) -> u64 {
        self.next
    }

    pub(crate) fn committed(&self) -> u64 {
        self.committed
    }

    /// Checks the request with the given offset is ready to be dispatched.
    ///
    /// # Arguments
    /// * `request` - the request to be dispatched.
    ///
    /// # Return
    /// `Ok` if the request is ready to be dispatched, `Err` any error occurs.
    pub(crate) fn check_barrier<R>(&mut self, request: &R) -> Result<(), ServiceError>
    where
        R: Batch + Ord,
    {
        if request.offset() < self.committed {
            // A retry request on a committed offset.
            // The client could regard the request as success.
            warn!(
                "{}Try to append request on committed offset {}, current committed={:?}",
                self.log_ident,
                request.offset(),
                self.committed
            );
            return Err(ServiceError::OffsetCommitted);
        }

        if request.offset() < self.next {
            // A retry request on a offset that is already in the write window.
            // To avoid data loss, the client should await the request to be completed and retry if necessary.
            warn!(
                "{}Try to append request on offset {}, which is in the write window, current next={:?}",
                self.log_ident,
                request.offset(),
                self.next
            );
            return Err(ServiceError::OffsetInFlight);
        }

        if request.offset() > self.next {
            // A request on a offset that is beyond the write window.
            // The client should promise the order of the requests.
            error!(
                "{}Try to append request on offset {}, which is beyond the write window, current next={:?}",
                self.log_ident,
                request.offset(),
                self.next
            );
            return Err(ServiceError::OffsetOutOfOrder);
        }
        // Expected request to be dispatched, just advance the next offset and go.
        self.next += request.len() as u64;
        Ok(())
    }

    /// Move the committed offset.
    ///
    /// # Arguments
    /// * `value` - the records whose offsets prior to `value` are all persisted.
    ///
    /// # Return
    /// * the committed offset.
    pub(crate) fn commit(&mut self, value: u64) -> u64 {
        if value > self.committed {
            self.committed = value;
        }
        self.committed
    }
}

#[cfg(test)]
mod tests {
    use model::Batch;
    use std::cmp::Ordering;

    #[derive(Debug)]
    struct Foo {
        offset: u64,
        len: u32,
    }

    impl Batch for Foo {
        fn offset(&self) -> u64 {
            self.offset
        }

        fn len(&self) -> u32 {
            self.len
        }
    }

    impl PartialEq for Foo {
        fn eq(&self, other: &Self) -> bool {
            self.offset == other.offset
        }
    }

    impl PartialOrd for Foo {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Eq for Foo {}

    impl Ord for Foo {
        fn cmp(&self, other: &Self) -> Ordering {
            other.offset.cmp(&self.offset)
        }
    }

    impl Foo {
        fn new(offset: u64) -> Self {
            Self { offset, len: 2 }
        }
    }

    #[test]
    fn test_new() {
        let mut window = super::Window::new(String::from(""), 0);
        let foo1 = Foo::new(0);
        assert!(window.check_barrier(&foo1).is_ok());
        assert_eq!(2, window.next());
    }

    #[tokio::test]
    #[ignore]
    async fn test_check_barrier() {
        let mut window = super::Window::new(String::from(""), 0);
        let foo1 = Foo::new(0);
        let foo2 = Foo::new(2);

        // foo2 will be rejected because the offset is beyond the write window, the error is OffsetOutOfOrder.
        assert!(matches!(
            window.check_barrier(&foo2).unwrap_err(),
            super::ServiceError::OffsetOutOfOrder
        ));

        // foo1 will be accepted.
        assert!(window.check_barrier(&foo1).is_ok());
        assert_eq!(2, window.next());
        assert_eq!(0, window.committed());
        // Now, foo2 will be accepted.
        assert!(window.check_barrier(&foo2).is_ok());
        assert_eq!(0, window.committed());

        // Commit foo2, and foo1 will be committed implicitly.
        assert_eq!(4, window.commit(4));
        assert_eq!(4, window.committed());

        // Now, foo1 will be rejected because the offset is already committed, the error is OffsetCommitted.
        assert!(matches!(
            window.check_barrier(&foo1).unwrap_err(),
            super::ServiceError::OffsetCommitted
        ));

        let foo3 = Foo::new(4);
        assert!(window.check_barrier(&foo3).is_ok());

        // The in-flight request will be rejected, the error is OffsetInFlight.
        assert!(matches!(
            window.check_barrier(&foo3).unwrap_err(),
            super::ServiceError::OffsetInFlight
        ));

        assert!(window.next == 6);
        assert!(window.committed == 4);
    }
}
