//! Colored, indented terminal output for the harness.
//!
//! Text carries `<green>...</green>`-style directives that [`colorf`] turns
//! into ANSI escapes, or strips when color is off. Color is off when
//! `HK_E2E_NO_COLOR` is set or when `NO_COLOR` is present and non-empty
//! (the no-color.org convention).

use std::io::{self, Write};
use std::sync::LazyLock;

use regex::Regex;

/// ANSI reset.
pub const ANSI_RESET: &str = "\x1b[0m";
/// ANSI dim.
pub const ANSI_DIM: &str = "\x1b[2m";
/// ANSI red.
pub const ANSI_RED: &str = "\x1b[31m";
/// ANSI cyan.
pub const ANSI_CYAN: &str = "\x1b[36m";
/// ANSI green.
pub const ANSI_GREEN: &str = "\x1b[32m";
/// ANSI yellow.
pub const ANSI_YELLOW: &str = "\x1b[33m";
/// ANSI bold.
pub const ANSI_BOLD: &str = "\x1b[1m";

struct ColorEntry {
    ansi: &'static str,
    re: Regex,
}

static COLORS: LazyLock<Vec<ColorEntry>> = LazyLock::new(|| {
    [
        ("cyan", ANSI_CYAN),
        ("green", ANSI_GREEN),
        ("red", ANSI_RED),
        ("yellow", ANSI_YELLOW),
        ("bold", ANSI_BOLD),
        ("dim", ANSI_DIM),
        ("reset", ANSI_RESET),
    ]
    .into_iter()
    .map(|(name, ansi)| ColorEntry {
        ansi,
        re: Regex::new(&format!("(?s)<{name}>(.*?)</{name}>")).expect("static regex"),
    })
    .collect()
});

/// Whether the environment asks for no color.
pub fn no_color() -> bool {
    no_color_from(
        std::env::var_os(crate::settings::NO_COLOR_ENV).is_some(),
        std::env::var("NO_COLOR")
            .map(|v| !v.is_empty())
            .unwrap_or(false),
    )
}

fn no_color_from(flag: bool, env_no_color: bool) -> bool {
    flag || env_no_color
}

/// Renders the color directives in `text` for the current environment.
/// Unterminated directives are left as literals.
pub fn colorf(text: &str) -> String {
    colorf_with(text, no_color())
}

/// [`colorf`] with color explicitly on (`no_color == false`) or off.
pub fn colorf_with(text: &str, no_color: bool) -> String {
    let mut s = text.to_string();
    for c in COLORS.iter() {
        let replacement = if no_color {
            "$1".to_string()
        } else {
            format!("{}$1{}", c.ansi, ANSI_RESET)
        };
        s = c.re.replace_all(&s, replacement.as_str()).into_owned();
    }
    s
}

/// Writes `text` with its directives rendered.
pub fn fcolorf(w: &mut dyn Write, text: &str) -> io::Result<()> {
    fcolorf_with(w, text, no_color())
}

/// Writes `text` with its directives rendered, then a newline.
pub fn fcolorfln(w: &mut dyn Write, text: &str) -> io::Result<()> {
    fcolorf_with(w, &format!("{text}\n"), no_color())
}

fn fcolorf_with(w: &mut dyn Write, text: &str, no_color: bool) -> io::Result<()> {
    w.write_all(colorf_with(text, no_color).as_bytes())
}

/// Wraps every write in one ANSI color, unless color is off.
pub struct ColorWriter<W> {
    w: W,
    ansi: String,
    no_color: bool,
}

impl<W: Write> ColorWriter<W> {
    /// Colors writes to `w` with `ansi`, following the environment.
    pub fn new(w: W, ansi: &str) -> Self {
        Self::with_no_color(w, ansi, no_color())
    }

    /// Colors writes to `w` with `ansi`, or passes them through when
    /// `no_color` is set.
    pub fn with_no_color(w: W, ansi: &str, no_color: bool) -> Self {
        ColorWriter {
            w,
            ansi: ansi.to_string(),
            no_color,
        }
    }

    /// The wrapped writer.
    pub fn into_inner(self) -> W {
        self.w
    }
}

impl<W: Write> Write for ColorWriter<W> {
    fn write(&mut self, p: &[u8]) -> io::Result<usize> {
        if p.is_empty() {
            return Ok(0);
        }
        if self.no_color {
            return self.w.write(p);
        }
        self.w.write_all(self.ansi.as_bytes())?;
        self.w.write_all(p)?;
        self.w.write_all(ANSI_RESET.as_bytes())?;
        Ok(p.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.w.flush()
    }
}

/// Inserts an indent at the start of every line written through it.
pub struct IndentWriter<W> {
    w: W,
    indent: String,
    at_line_start: bool,
}

impl<W: Write> IndentWriter<W> {
    /// Indents every line written to `w` with `indent`.
    pub fn new(w: W, indent: &str) -> Self {
        IndentWriter {
            w,
            indent: indent.to_string(),
            at_line_start: true,
        }
    }

    /// The wrapped writer.
    pub fn into_inner(self) -> W {
        self.w
    }
}

impl<W: Write> Write for IndentWriter<W> {
    fn write(&mut self, p: &[u8]) -> io::Result<usize> {
        if p.is_empty() {
            return Ok(0);
        }
        let mut buf = Vec::with_capacity(p.len() + self.indent.len());
        for &b in p {
            if self.at_line_start {
                buf.extend_from_slice(self.indent.as_bytes());
                self.at_line_start = false;
            }
            buf.push(b);
            if b == b'\n' {
                self.at_line_start = true;
            }
        }
        // One write keeps a wrapping ColorWriter to one escape pair.
        self.w.write_all(&buf)?;
        Ok(p.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.w.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn colorf_cases() {
        let cases: &[(&str, bool, &str, &str)] = &[
            (
                "single green tag",
                false,
                "This is <green>green</green> text",
                "This is \x1b[32mgreen\x1b[0m text",
            ),
            (
                "multiple green tags",
                false,
                "<green>one</green> and <green>two</green>",
                "\x1b[32mone\x1b[0m and \x1b[32mtwo\x1b[0m",
            ),
            (
                "different tags",
                false,
                "<green>green</green> and <red>red</red> and <yellow>yellow</yellow>",
                "\x1b[32mgreen\x1b[0m and \x1b[31mred\x1b[0m and \x1b[33myellow\x1b[0m",
            ),
            (
                "unterminated tag",
                false,
                "This is <green>unterminated",
                "This is <green>unterminated",
            ),
            (
                "formatting with tags",
                false,
                "User <green>bob</green> logged in",
                "User \x1b[32mbob\x1b[0m logged in",
            ),
            (
                "multiline tag",
                false,
                "This is <green>green\ntext</green>",
                "This is \x1b[32mgreen\ntext\x1b[0m",
            ),
            (
                "color disabled - multiline tags stripped",
                true,
                "This is <green>green\ntext</green>",
                "This is green\ntext",
            ),
            (
                "color disabled - tags stripped",
                true,
                "This is <green>green</green> and <red>red</red>",
                "This is green and red",
            ),
            (
                "color disabled - unterminated remains",
                true,
                "This is <green>unterminated",
                "This is <green>unterminated",
            ),
            (
                "dim tag",
                false,
                "This is <dim>dim</dim> text",
                "This is \x1b[2mdim\x1b[0m text",
            ),
            (
                "color disabled - dim tag stripped",
                true,
                "This is <dim>dim</dim> text",
                "This is dim text",
            ),
        ];
        for (name, no_color, input, want) in cases {
            assert_eq!(colorf_with(input, *no_color), *want, "{name}");
        }
        // The formatting case: arguments are substituted before rendering.
        assert_eq!(
            colorf_with(&format!("User <green>{}</green> logged in", "bob"), false),
            "User \x1b[32mbob\x1b[0m logged in"
        );
    }

    #[test]
    fn fcolorf_and_fcolorfln() {
        let mut buf = Vec::new();
        fcolorf_with(
            &mut buf,
            &format!("Hello <green>{}</green>", "world"),
            false,
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "Hello \x1b[32mworld\x1b[0m"
        );

        let mut buf = Vec::new();
        fcolorf_with(
            &mut buf,
            &format!("Hello <green>{}</green>\n", "world"),
            false,
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "Hello \x1b[32mworld\x1b[0m\n"
        );

        let mut buf = Vec::new();
        fcolorf_with(&mut buf, "Hello <green>world</green>", true).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "Hello world");
    }

    #[test]
    fn color_writer() {
        let cases = [
            (
                "color enabled - red",
                false,
                ANSI_RED,
                "hello",
                "\x1b[31mhello\x1b[0m",
            ),
            (
                "color enabled - dim",
                false,
                ANSI_DIM,
                "world",
                "\x1b[2mworld\x1b[0m",
            ),
            ("color disabled", true, ANSI_RED, "hello", "hello"),
            ("empty input", false, ANSI_RED, "", ""),
        ];
        for (name, no_color, ansi, input, want) in cases {
            let mut cw = ColorWriter::with_no_color(Vec::new(), ansi, no_color);
            let n = cw.write(input.as_bytes()).unwrap();
            assert_eq!(n, input.len(), "{name}");
            assert_eq!(String::from_utf8(cw.into_inner()).unwrap(), want, "{name}");
        }
    }

    #[test]
    fn indent_writer() {
        let cases: &[(&str, &[&str], &str)] = &[
            ("single line", &["hello"], "  hello"),
            (
                "multiple lines in one write",
                &["hello\nworld"],
                "  hello\n  world",
            ),
            ("multiple writes", &["hello\n", "world"], "  hello\n  world"),
            ("newline at end", &["hello\n"], "  hello\n"),
            ("empty writes", &["", "hello"], "  hello"),
            (
                "write without newline then write with newline",
                &["hello ", "world\n"],
                "  hello world\n",
            ),
        ];
        for (name, inputs, want) in cases {
            let mut iw = IndentWriter::new(Vec::new(), "  ");
            for input in *inputs {
                assert_eq!(iw.write(input.as_bytes()).unwrap(), input.len(), "{name}");
            }
            assert_eq!(String::from_utf8(iw.into_inner()).unwrap(), *want, "{name}");
        }
    }

    #[test]
    fn indent_and_color_chaining() {
        let cyan = ColorWriter::with_no_color(Vec::new(), ANSI_CYAN, false);
        let mut iw = IndentWriter::new(cyan, "        ");
        iw.write_all(b"hello\nworld\n").unwrap();
        let out = String::from_utf8(iw.into_inner().into_inner()).unwrap();
        assert_eq!(out, "\x1b[36m        hello\n        world\n\x1b[0m");
    }

    #[test]
    fn env_no_color() {
        // NO_COLOR set alone disables color.
        assert!(no_color_from(false, true));
        assert_eq!(
            colorf_with("<green>hello</green>", no_color_from(false, true)),
            "hello"
        );
        // Neither set leaves color on.
        assert!(!no_color_from(false, false));
        assert_eq!(
            colorf_with("<green>hello</green>", no_color_from(false, false)),
            "\x1b[32mhello\x1b[0m"
        );
    }
}
