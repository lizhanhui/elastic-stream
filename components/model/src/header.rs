use std::collections::HashMap;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Common {
    Keys,
    Tag,
    RecordId,
    CreatedAt,
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
}

#[cfg(test)]
mod tests {
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
}
