use std::collections::HashMap;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Common {
    Topic,
    Partition,
    Key,
    Offset,
}

#[derive(Debug, Clone)]
pub struct Headers {
    pub common: HashMap<Common, String>,
    pub ext: HashMap<String, String>,
}

impl Headers {
    pub fn new(topic: String, partition: i32) -> Self {
        let mut common = HashMap::new();
        common.entry(Common::Topic).or_insert(topic);
        common
            .entry(Common::Partition)
            .or_insert(partition.to_string());

        Self {
            common,
            ext: HashMap::new(),
        }
    }

    pub(crate) fn topic(&self) -> Option<&str> {
        self.common.get(&Common::Topic).map(|s| &s[..])
    }

    pub(crate) fn partition(&self) -> Option<i32> {
        if let Some(s) = self.common.get(&Common::Partition) {
            return s.parse::<i32>().ok();
        }
        None
    }

    pub(crate) fn key(&self) -> Option<&str> {
        self.common.get(&Common::Key).map(|s| &s[..])
    }

    pub(crate) fn offset(&self) -> Option<i64> {
        if let Some(s) = self.common.get(&Common::Offset) {
            return s.parse::<i64>().ok();
        }
        None
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
        let mut headers = Headers::new("test_topic".to_string(), 0);
        assert_eq!(headers.topic(), Some("test_topic"));
        assert_eq!(headers.partition(), Some(0));
        assert_eq!(headers.key(), None);
        assert_eq!(headers.offset(), None);

        headers
            .common
            .entry(Common::Offset)
            .or_insert("123".to_string());
        assert_eq!(headers.offset(), Some(123));

        headers
            .common
            .entry(Common::Key)
            .or_insert("order_123".to_string());
        assert_eq!(headers.key(), Some("order_123"));
    }
}
