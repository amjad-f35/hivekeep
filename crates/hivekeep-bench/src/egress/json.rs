//! JSON encoding with the number formatting the benchmark's drivers expect:
//! floats in their shortest round-trip decimal form (`0.000001`, `63.1`,
//! `1`), switching to exponent form only below 1e-6 or from 1e21 up
//! (`1e-7`, `1e+21`).

use std::io;

use serde::Serialize;
use serde_json::ser::{Formatter, PrettyFormatter};

/// Formats `f` the way the drivers read numbers.
pub fn format_f64(f: f64) -> String {
    let abs = f.abs();
    if abs != 0.0 && (abs < 1e-6 || abs >= 1e21) {
        let s = format!("{f:e}");
        // Rust writes `1e21` and `1e-7`; the positive exponent gets its sign.
        match s.split_once('e') {
            Some((m, e)) if !e.starts_with('-') => format!("{m}e+{e}"),
            _ => s,
        }
    } else {
        format!("{f}")
    }
}

/// A compact formatter writing floats with [`format_f64`].
struct Compact;

impl Formatter for Compact {
    fn write_f64<W: ?Sized + io::Write>(&mut self, w: &mut W, value: f64) -> io::Result<()> {
        w.write_all(format_f64(value).as_bytes())
    }
    fn write_f32<W: ?Sized + io::Write>(&mut self, w: &mut W, value: f32) -> io::Result<()> {
        w.write_all(format_f64(value as f64).as_bytes())
    }
}

/// An indenting formatter writing floats with [`format_f64`].
struct Pretty<'a>(PrettyFormatter<'a>);

impl Formatter for Pretty<'_> {
    fn write_f64<W: ?Sized + io::Write>(&mut self, w: &mut W, value: f64) -> io::Result<()> {
        w.write_all(format_f64(value).as_bytes())
    }
    fn write_f32<W: ?Sized + io::Write>(&mut self, w: &mut W, value: f32) -> io::Result<()> {
        w.write_all(format_f64(value as f64).as_bytes())
    }
    fn begin_array<W: ?Sized + io::Write>(&mut self, w: &mut W) -> io::Result<()> {
        self.0.begin_array(w)
    }
    fn end_array<W: ?Sized + io::Write>(&mut self, w: &mut W) -> io::Result<()> {
        self.0.end_array(w)
    }
    fn begin_array_value<W: ?Sized + io::Write>(
        &mut self,
        w: &mut W,
        first: bool,
    ) -> io::Result<()> {
        self.0.begin_array_value(w, first)
    }
    fn end_array_value<W: ?Sized + io::Write>(&mut self, w: &mut W) -> io::Result<()> {
        self.0.end_array_value(w)
    }
    fn begin_object<W: ?Sized + io::Write>(&mut self, w: &mut W) -> io::Result<()> {
        self.0.begin_object(w)
    }
    fn end_object<W: ?Sized + io::Write>(&mut self, w: &mut W) -> io::Result<()> {
        self.0.end_object(w)
    }
    fn begin_object_key<W: ?Sized + io::Write>(
        &mut self,
        w: &mut W,
        first: bool,
    ) -> io::Result<()> {
        self.0.begin_object_key(w, first)
    }
    fn begin_object_value<W: ?Sized + io::Write>(&mut self, w: &mut W) -> io::Result<()> {
        self.0.begin_object_value(w)
    }
    fn end_object_value<W: ?Sized + io::Write>(&mut self, w: &mut W) -> io::Result<()> {
        self.0.end_object_value(w)
    }
}

/// Encodes `v` compactly.
pub fn to_vec<T: Serialize + ?Sized>(v: &T) -> serde_json::Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut ser = serde_json::Serializer::with_formatter(&mut out, Compact);
    v.serialize(&mut ser)?;
    Ok(out)
}

/// Encodes `v` indented by two spaces.
pub fn to_vec_pretty<T: Serialize + ?Sized>(v: &T) -> serde_json::Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut ser = serde_json::Serializer::with_formatter(
        &mut out,
        Pretty(PrettyFormatter::with_indent(b"  ")),
    );
    v.serialize(&mut ser)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floats_read_as_decimals() {
        for (f, want) in [
            (1e-6, "0.000001"),
            (63.1, "63.1"),
            (1.0, "1"),
            (0.0, "0"),
            (1.6190000000000004, "1.6190000000000004"),
            (1e-7, "1e-7"),
            (1e21, "1e+21"),
            (-2.5e-8, "-2.5e-8"),
        ] {
            assert_eq!(format_f64(f), want, "{f}");
        }
        assert_eq!(to_vec(&[1.0f64, 0.5]).unwrap(), b"[1,0.5]");
    }
}
