use std::fmt;

use log::info;
use model::range::RangeMetadata;

use crate::error::ServiceError;

use super::window::Window;

#[derive(Debug)]
pub(crate) struct Range {
    log_ident: String,

    pub(crate) metadata: RangeMetadata,

    committed: Option<u64>,

    window: Option<Window>,
}

impl Range {
    pub(crate) fn new(metadata: RangeMetadata) -> Self {
        let log_ident = format!("Range[{}#{}] ", metadata.stream_id(), metadata.index());
        Self {
            log_ident: log_ident.clone(),
            window: if metadata.has_end() {
                None
            } else {
                Some(Window::new(log_ident, metadata.start()))
            },
            metadata,
            committed: None,
        }
    }

    pub(crate) fn committed(&self) -> Option<u64> {
        self.committed
    }

    pub(crate) fn reset(&mut self, offset: u64) {
        if let Some(window) = self.window_mut() {
            window.reset(offset);
        }
    }

    /// Move the committed offset
    /// Arguments:
    /// - new_committed_offset: the records before the new_committed are persisted.
    ///     Cause of append handle use async await, so the new_committed_offset may not
    ///     pass in order, only the larger new_committed_offset will be accepted.
    pub(crate) fn commit(&mut self, new_committed_offset: u64) -> Result<(), ServiceError> {
        let new_committed_offset = match self.window_mut() {
            Some(win) => win.commit(new_committed_offset),
            None => {
                return Err(ServiceError::AlreadySealed);
            }
        };

        if let Some(ref mut committed) = self.committed {
            if new_committed_offset > *committed {
                *committed = new_committed_offset;
            }
            return Ok(());
        }

        if new_committed_offset >= self.metadata.start() {
            self.committed = Some(new_committed_offset);
        }
        Ok(())
    }

    pub(crate) fn seal(&mut self, metadata: &mut RangeMetadata) {
        let end = self.committed.unwrap_or(self.metadata.start());
        if let Some(end_offset) = metadata.end() {
            self.metadata.set_end(end_offset)
        };

        metadata.set_end(end);

        // Drop window once the range is sealed.
        self.window.take();

        info!(
            "{}Range sealed, request metadata={}, the range end={}",
            self.log_ident, metadata, end
        );
        if !self.data_complete() {
            // TODO: spawn a task to replica data from peers.
        }
    }

    pub(crate) fn data_complete(&self) -> bool {
        match self.committed {
            Some(committed) => committed >= self.metadata.end().unwrap_or(self.metadata.start()),
            None => false,
        }
    }

    pub(crate) fn window_mut(&mut self) -> Option<&mut Window> {
        self.window.as_mut()
    }
}

impl fmt::Display for Range {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Range[stream-id={}, range-index={}], committed={:?}, next={}",
            self.metadata.stream_id(),
            self.metadata.index(),
            self.window
                .as_ref()
                .map_or(self.committed.unwrap_or(0), |win| { win.committed() }),
            self.window.as_ref().map_or(0, |window| window.next())
        )
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use model::{range::RangeMetadata, Batch};

    #[test]
    fn test_new() -> Result<(), Box<dyn Error>> {
        let metadata = RangeMetadata::new(0, 0, 0, 0, None);
        let range = super::Range::new(metadata);
        assert_eq!(range.committed(), None);
        assert!(!range.data_complete());
        Ok(())
    }

    #[test]
    fn test_display() -> Result<(), Box<dyn Error>> {
        let metadata = RangeMetadata::new(0, 0, 0, 0, None);
        let mut range = super::Range::new(metadata);

        let expected = "Range[stream-id=0, range-index=0], committed=0, next=0";
        assert_eq!(expected, format!("{}", range));

        match range.window_mut() {
            Some(win) => {
                win.check_barrier(&Foo { offset: 0, len: 42 })?;
            }
            None => {
                panic!("Window should not be None");
            }
        }
        range.commit(42)?;

        let expected = "Range[stream-id=0, range-index=0], committed=42, next=42";
        assert_eq!(expected, format!("{}", range));

        Ok(())
    }

    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
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

    #[tokio::test]
    async fn test_commit() -> Result<(), Box<dyn Error>> {
        let metadata = RangeMetadata::new(0, 0, 0, 0, None);
        let mut range = super::Range::new(metadata);
        range
            .window_mut()
            .and_then(|window| window.check_barrier(&Foo { offset: 0, len: 1 }).ok());
        range.commit(1)?;
        assert_eq!(range.committed(), Some(1));

        Ok(())
    }

    #[tokio::test]
    async fn test_seal() -> Result<(), Box<dyn Error>> {
        let metadata = RangeMetadata::new(0, 0, 0, 0, None);
        let mut range = super::Range::new(metadata.clone());
        range
            .window_mut()
            .and_then(|window| window.check_barrier(&Foo { offset: 0, len: 1 }).ok());
        range.commit(1)?;

        let mut metadata = RangeMetadata::new(0, 0, 0, 0, Some(1));
        range.seal(&mut metadata);

        assert_eq!(range.committed(), Some(1));
        assert!(range.data_complete(), "Data should be complete");

        let mut metadata = RangeMetadata::new(0, 0, 0, 0, Some(2));
        range.seal(&mut metadata);

        assert_eq!(range.committed(), Some(1));
        assert!(!range.data_complete(), "Data should not be complete");

        Ok(())
    }
}
