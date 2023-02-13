//! Each partition is composed of multiple segments.
//!
//! A segment is addressable through querying placement-managers.
//! Once a segement is sealed, it is immutable to write.
//!
//! A segement normally contains 3 replica among either the same available zone or multiple AZs.
//! Placement managers will periodically check if replica requirement is met or not for each segment.
//! Once a violation is found, placement manager would instruct data nodes of choice to add or remove replica.

use std::fs::File;

pub trait Segment {}

pub struct JournalSegment {
    pub(crate) file: File,
    pub(crate) file_name: String,
}

impl JournalSegment {
    pub fn new(file: File, file_name: &str) -> Self {
        Self {
            file,
            file_name: file_name.to_owned(),
        }
    }
}

impl Segment for JournalSegment {}
