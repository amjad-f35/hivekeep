//! Messages of the locust master/worker protocol: a msgpack array of
//! `(type, data, node_id)`.

use rmpv::Value;

/// One protocol message.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    /// The message type (`spawn`, `stats`, `heartbeat`, ...).
    pub kind: String,
    /// The payload; usually a map, sometimes nil or a scalar.
    pub data: Value,
    /// The worker the message is from or for.
    pub node_id: String,
}

/// Why bytes could not be read as a message.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    /// Not msgpack.
    #[error("decoding msgpack: {0}")]
    Msgpack(String),
    /// Msgpack, but not a `(type, data, node_id)` triple.
    #[error("malformed message: {0}")]
    Shape(&'static str),
}

impl Message {
    /// A message of `kind` carrying `data`.
    pub fn new(kind: &str, data: Value, node_id: &str) -> Self {
        Message {
            kind: kind.to_string(),
            data,
            node_id: node_id.to_string(),
        }
    }

    /// The wire form.
    pub fn encode(&self) -> Vec<u8> {
        let v = Value::Array(vec![
            Value::from(self.kind.as_str()),
            self.data.clone(),
            Value::from(self.node_id.as_str()),
        ]);
        let mut buf = Vec::new();
        // Writing to a Vec cannot fail.
        let _ = rmpv::encode::write_value(&mut buf, &v);
        buf
    }

    /// Reads the wire form.
    pub fn decode(mut bytes: &[u8]) -> Result<Self, DecodeError> {
        let v = rmpv::decode::read_value(&mut bytes)
            .map_err(|e| DecodeError::Msgpack(e.to_string()))?;
        let Value::Array(items) = v else {
            return Err(DecodeError::Shape("not an array"));
        };
        let mut it = items.into_iter();
        let kind = it
            .next()
            .and_then(|v| text(&v))
            .ok_or(DecodeError::Shape("no type"))?;
        let data = it.next().unwrap_or(Value::Nil);
        let node_id = it.next().and_then(|v| text(&v)).unwrap_or_default();
        Ok(Message {
            kind,
            data,
            node_id,
        })
    }

    /// The entry `key` of a map payload.
    pub fn get(&self, key: &str) -> Option<&Value> {
        map_get(&self.data, key)
    }
}

/// A string or binary msgpack value as text.
pub fn text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => s.as_str().map(str::to_string),
        Value::Binary(b) => String::from_utf8(b.clone()).ok(),
        _ => None,
    }
}

/// The entry `key` of a msgpack map.
pub fn map_get<'a>(v: &'a Value, key: &str) -> Option<&'a Value> {
    match v {
        Value::Map(entries) => entries
            .iter()
            .find(|(k, _)| text(k).as_deref() == Some(key))
            .map(|(_, v)| v),
        _ => None,
    }
}

/// A msgpack number as f64.
pub fn number(v: &Value) -> Option<f64> {
    match v {
        Value::Integer(i) => i.as_f64(),
        Value::F32(f) => Some(*f as f64),
        Value::F64(f) => Some(*f),
        _ => None,
    }
}

/// Builds a msgpack map from string keys.
pub fn map<I, K>(entries: I) -> Value
where
    I: IntoIterator<Item = (K, Value)>,
    K: Into<String>,
{
    Value::Map(
        entries
            .into_iter()
            .map(|(k, v)| (Value::from(k.into()), v))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let m = Message::new(
            "spawn",
            map([("user_classes_count", map([("GluttonUser", Value::from(3))]))]),
            "w1",
        );
        let back = Message::decode(&m.encode()).unwrap();
        assert_eq!(back, m);
        let count = back
            .get("user_classes_count")
            .and_then(|v| map_get(v, "GluttonUser"))
            .and_then(number);
        assert_eq!(count, Some(3.0));
    }

    #[test]
    fn rejects_malformed_input() {
        assert!(Message::decode(b"\xc1").is_err());
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &Value::from(5)).unwrap();
        assert!(matches!(Message::decode(&buf), Err(DecodeError::Shape(_))));
        // A bare (type) still decodes, with nil data.
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &Value::Array(vec![Value::from("quit")])).unwrap();
        let m = Message::decode(&buf).unwrap();
        assert_eq!(m.kind, "quit");
        assert_eq!(m.data, Value::Nil);
    }
}
