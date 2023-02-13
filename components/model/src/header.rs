use std::collections::HashMap;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Common {
    Stream,
    Key,
    Tag,
    RecordId,
}

#[derive(Debug, Clone)]
pub struct Headers {
    pub common: HashMap<Common, String>,
    pub ext: HashMap<String, String>,
}

impl Headers {
    pub fn new(stream: i32) -> Self {
        let mut common = HashMap::new();
        common
            .entry(Common::Stream)
            .or_insert(stream.to_string());

        Self {
            common,
            ext: HashMap::new(),
        }
    }

    pub(crate) fn stream(&self) -> Option<i32> {
        if let Some(s) = self.common.get(&Common::Stream) {
            return s.parse::<i32>().ok();
        }
        None
    }

    pub(crate) fn key(&self) -> Option<&str> {
        self.common.get(&Common::Key).map(|s| &s[..])
    }

    pub(crate) fn add_property(&mut self, key: String, value: String) -> Option<String> {
        self.ext.insert(key, value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_headers() {
        let mut headers = Headers::new(123);
        assert_eq!(headers.stream(), Some(123));
        assert_eq!(headers.key(), None);

        headers
            .common
            .entry(Common::Key)
            .or_insert("order_123".to_string());
        assert_eq!(headers.key(), Some("order_123"));
    }
}
