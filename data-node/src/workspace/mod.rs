//! A workspace is a place where each node manages its mutable stream ranges.
//!
//! Workspaces are responsible for ranges retrieval and seal. For each range,
//! there are likely a window of in-progress record-append operation pushed down to
//! IO layer. So each stream range have a in-progress window.

pub(crate) mod append_window;

pub(crate) mod stream_manager;
