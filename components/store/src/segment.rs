use monoio::fs::File;

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
