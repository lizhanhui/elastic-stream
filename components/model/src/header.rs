use std::{collections::HashMap, fmt};
use strum_macros::EnumString;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
#[derive(EnumString)]
pub enum Common {
    Keys,
    Tag,
    RecordId,
    CreatedAt,
}

impl fmt::Display for Common {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Headers {
    common: HashMap<Common, String>,
}

impl Headers {
    pub fn new() -> Self {
        Self {
            common: HashMap::new(),
        }
    }

    pub(crate) fn add_header(&mut self, key: Common, value: String) -> Option<String> {
        self.common.insert(key, value)
    }

    pub(crate) fn get_header(&self, key: Common) -> Option<&String> {
        self.common.get(&key)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&Common, &String)> {
        self.common.iter()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_headers() {
        let mut headers = Headers::new();
        headers.add_header(Common::RecordId, "record_id_123".to_string());
        headers.add_header(Common::Keys, "key1 key2".to_string());
        headers.add_header(Common::Tag, "tag".to_string());
        headers.add_header(Common::CreatedAt, "2020-01-01T00:00:00Z".to_string());

        assert_eq!(headers.get_header(Common::RecordId), Some(&"record_id_123".to_string()));
        assert_eq!(headers.get_header(Common::Keys), Some(&"key1 key2".to_string()));
        assert_eq!(headers.get_header(Common::Tag), Some(&"tag".to_string()));
        assert_eq!(headers.get_header(Common::CreatedAt), Some(&"2020-01-01T00:00:00Z".to_string()));
    }

    #[test]
    fn test_common_key_display() {
        assert_eq!(Common::RecordId.to_string(), "RecordId");
        assert_eq!(Common::Keys.to_string(), "Keys");
        assert_eq!(Common::Tag.to_string(), "Tag");
        assert_eq!(Common::CreatedAt.to_string(), "CreatedAt");

        assert_eq!(Common::RecordId, Common::from_str("RecordId").unwrap());
        assert_eq!(Common::Keys, Common::from_str("Keys").unwrap());
        assert_eq!(Common::Tag, Common::from_str("Tag").unwrap());
        assert_eq!(Common::CreatedAt, Common::from_str("CreatedAt").unwrap());
    }
}
