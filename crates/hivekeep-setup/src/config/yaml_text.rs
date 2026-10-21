//! Parsing a YAML document into a tree whose scalars keep the text that was
//! written.
//!
//! Resolving a scalar to a typed value first would hand a string setting
//! `0.1` for `0.10` and `123` for `0123`, and an image tag or an account id
//! is exactly the kind of value typed unquoted into a hand-edited file. The
//! document is read twice: once to learn its shape, and once more asking the
//! parser for every scalar as a string, which yields its source text.

use serde::de::{self, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use std::fmt;

/// A parsed node. Mappings keep their keys in document order, duplicates
/// included, so the caller can reject them with a useful message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Node {
    /// A scalar, as written.
    Scalar(String),
    /// An explicit or implicit null.
    Null,
    Map(Vec<(String, Node)>),
    Seq,
    /// Something no setting can hold (a tagged value).
    Other,
}

/// The shape of a node, learned without keeping any scalar text.
#[derive(Clone, Debug)]
enum Shape {
    Scalar,
    Null,
    Map(Vec<Shape>),
    Seq,
    Other,
}

struct ShapeVisitor;

impl<'de> Visitor<'de> for ShapeVisitor {
    type Value = Shape;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("any YAML value")
    }
    fn visit_bool<E>(self, _: bool) -> Result<Shape, E> {
        Ok(Shape::Scalar)
    }
    fn visit_i64<E>(self, _: i64) -> Result<Shape, E> {
        Ok(Shape::Scalar)
    }
    fn visit_u64<E>(self, _: u64) -> Result<Shape, E> {
        Ok(Shape::Scalar)
    }
    fn visit_i128<E>(self, _: i128) -> Result<Shape, E> {
        Ok(Shape::Scalar)
    }
    fn visit_u128<E>(self, _: u128) -> Result<Shape, E> {
        Ok(Shape::Scalar)
    }
    fn visit_f64<E>(self, _: f64) -> Result<Shape, E> {
        Ok(Shape::Scalar)
    }
    fn visit_str<E>(self, _: &str) -> Result<Shape, E> {
        Ok(Shape::Scalar)
    }
    fn visit_unit<E>(self) -> Result<Shape, E> {
        Ok(Shape::Null)
    }
    fn visit_none<E>(self) -> Result<Shape, E> {
        Ok(Shape::Null)
    }
    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Shape, D::Error> {
        d.deserialize_any(ShapeVisitor)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Shape, A::Error> {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(Shape::Seq)
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Shape, A::Error> {
        let mut children = Vec::new();
        while map.next_key::<IgnoredAny>()?.is_some() {
            children.push(map.next_value_seed(ShapeSeed)?);
        }
        Ok(Shape::Map(children))
    }
    fn visit_enum<A: de::EnumAccess<'de>>(self, data: A) -> Result<Shape, A::Error> {
        let (_, v): (IgnoredAny, _) = data.variant()?;
        de::VariantAccess::newtype_variant::<IgnoredAny>(v)?;
        Ok(Shape::Other)
    }
}

struct ShapeSeed;

impl<'de> DeserializeSeed<'de> for ShapeSeed {
    type Value = Shape;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Shape, D::Error> {
        d.deserialize_any(ShapeVisitor)
    }
}

/// Reads a node again, now knowing its shape.
struct NodeSeed<'a>(&'a Shape);

impl<'de> DeserializeSeed<'de> for NodeSeed<'_> {
    type Value = Node;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Node, D::Error> {
        match self.0 {
            Shape::Scalar => String::deserialize_text(d).map(Node::Scalar),
            Shape::Null => {
                IgnoredAny::deserialize_any_into(d)?;
                Ok(Node::Null)
            }
            Shape::Seq => {
                IgnoredAny::deserialize_any_into(d)?;
                Ok(Node::Seq)
            }
            Shape::Other => {
                IgnoredAny::deserialize_any_into(d)?;
                Ok(Node::Other)
            }
            Shape::Map(children) => d.deserialize_map(MapVisitor(children)),
        }
    }
}

trait TextExt: Sized {
    fn deserialize_text<'de, D: Deserializer<'de>>(d: D) -> Result<Self, D::Error>;
}

impl TextExt for String {
    fn deserialize_text<'de, D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct S;
        impl Visitor<'_> for S {
            type Value = String;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a scalar")
            }
            fn visit_str<E>(self, v: &str) -> Result<String, E> {
                Ok(v.to_string())
            }
        }
        d.deserialize_str(S)
    }
}

trait IgnoreExt {
    fn deserialize_any_into<'de, D: Deserializer<'de>>(d: D) -> Result<(), D::Error>;
}

impl IgnoreExt for IgnoredAny {
    fn deserialize_any_into<'de, D: Deserializer<'de>>(d: D) -> Result<(), D::Error> {
        d.deserialize_ignored_any(IgnoredAny).map(|_| ())
    }
}

struct MapVisitor<'a>(&'a [Shape]);

impl<'de> Visitor<'de> for MapVisitor<'_> {
    type Value = Node;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a mapping")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Node, A::Error> {
        let mut out = Vec::with_capacity(self.0.len());
        let mut shapes = self.0.iter();
        while let Some(key) = map.next_key_seed(KeySeed)? {
            let shape = shapes
                .next()
                .ok_or_else(|| de::Error::custom("document changed between reads"))?;
            out.push((key, map.next_value_seed(NodeSeed(shape))?));
        }
        Ok(Node::Map(out))
    }
}

struct KeySeed;

impl<'de> DeserializeSeed<'de> for KeySeed {
    type Value = String;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<String, D::Error> {
        String::deserialize_text(d)
    }
}

/// Parses a single YAML document. An empty document is [`Node::Null`].
pub fn parse(text: &str) -> Result<Node, String> {
    if text.trim().is_empty() {
        return Ok(Node::Null);
    }
    let shape = ShapeSeed
        .deserialize(serde_yaml::Deserializer::from_str(text))
        .map_err(|e| e.to_string())?;
    NodeSeed(&shape)
        .deserialize(serde_yaml::Deserializer::from_str(text))
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalars_keep_their_text() {
        let n = parse("a: 0.10\nb: 0123\nc: \"q\"\nd:\ne: ~\nf: [1]\ng:\n  h: yes\n").unwrap();
        let Node::Map(entries) = n else {
            panic!("not a map")
        };
        let get = |k: &str| {
            entries
                .iter()
                .find(|(x, _)| x == k)
                .map(|(_, v)| v.clone())
                .unwrap()
        };
        assert_eq!(get("a"), Node::Scalar("0.10".into()));
        assert_eq!(get("b"), Node::Scalar("0123".into()));
        assert_eq!(get("c"), Node::Scalar("q".into()));
        assert_eq!(get("d"), Node::Null);
        assert_eq!(get("e"), Node::Null);
        assert_eq!(get("f"), Node::Seq);
        assert_eq!(
            get("g"),
            Node::Map(vec![("h".into(), Node::Scalar("yes".into()))])
        );
    }

    #[test]
    fn duplicate_keys_survive_parsing() {
        let n = parse("a: 1\na: 2\n").unwrap();
        assert_eq!(
            n,
            Node::Map(vec![
                ("a".into(), Node::Scalar("1".into())),
                ("a".into(), Node::Scalar("2".into()))
            ])
        );
    }
}
