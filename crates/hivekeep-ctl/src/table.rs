//! Aligned text tables.
//!
//! Every column but the last is padded to its widest cell plus three spaces;
//! the last column is written as is.

use std::io::{self, Write};

const PADDING: usize = 3;

/// A table being built row by row.
#[derive(Debug, Default)]
pub struct Table {
    rows: Vec<Vec<String>>,
}

impl Table {
    /// A table whose first row is `header`.
    pub fn new<S: Into<String>>(header: impl IntoIterator<Item = S>) -> Self {
        let mut t = Table::default();
        t.row(header);
        t
    }

    /// Appends a row.
    pub fn row<S: Into<String>>(&mut self, cells: impl IntoIterator<Item = S>) {
        self.rows.push(cells.into_iter().map(Into::into).collect());
    }

    /// Renders the table to a string.
    pub fn render(&self) -> String {
        let columns = self.rows.iter().map(Vec::len).max().unwrap_or(0);
        let mut widths = vec![0usize; columns];
        for row in &self.rows {
            // The last cell of a row is not aligned, so it does not widen its
            // column.
            for (i, cell) in row.iter().enumerate().take(row.len().saturating_sub(1)) {
                widths[i] = widths[i].max(cell.chars().count());
            }
        }
        let mut out = String::new();
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                out.push_str(cell);
                if i + 1 < row.len() {
                    let pad = widths[i] + PADDING - cell.chars().count();
                    out.extend(std::iter::repeat_n(' ', pad));
                }
            }
            out.push('\n');
        }
        out
    }

    /// Writes the rendered table to `w`.
    pub fn write_to(&self, w: &mut dyn Write) -> io::Result<()> {
        w.write_all(self.render().as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligns_all_but_the_last_column() {
        let mut t = Table::new(["NAME", "AGE"]);
        t.row(["team-a", "5m"]);
        t.row(["b", "a-long-value"]);
        assert_eq!(
            t.render(),
            "NAME     AGE\nteam-a   5m\nb        a-long-value\n"
        );
    }

    #[test]
    fn empty_cells_are_padded() {
        let mut t = Table::new(["A", "B", "C"]);
        t.row(["x", "", "z"]);
        assert_eq!(t.render(), "A   B   C\nx       z\n");
    }
}
