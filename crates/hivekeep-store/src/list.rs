//! Pagination parameters and results shared by every list operation.

use crate::error::{Result, StoreError};

/// Page size used when a caller leaves it unset.
pub const DEFAULT_PAGE_SIZE: i32 = 1000;

/// Pagination parameters common to every list method.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListOptions {
    /// Caps how many items one call returns. Zero means
    /// [`DEFAULT_PAGE_SIZE`]; negative is rejected.
    pub page_size: i32,
    /// Resumes a listing after the page it was issued for. Empty starts from
    /// the first page.
    pub page_token: String,
}

impl ListOptions {
    /// Options for one page of at most `page_size` items after `page_token`.
    pub fn new(page_size: i32, page_token: impl Into<String>) -> Self {
        ListOptions {
            page_size,
            page_token: page_token.into(),
        }
    }

    /// Applies the default page size and rejects negative sizes.
    pub fn normalize(&self) -> Result<ListOptions> {
        if self.page_size < 0 {
            return Err(StoreError::InvalidPageSize);
        }
        let mut out = self.clone();
        if out.page_size == 0 {
            out.page_size = DEFAULT_PAGE_SIZE;
        }
        Ok(out)
    }
}

/// One page of a listing plus the token for the next one.
#[derive(Debug, Clone, PartialEq)]
pub struct ListResponse<T> {
    /// The items on this page, in key order.
    pub items: Vec<T>,
    /// Token for the next page; empty once the listing is exhausted.
    pub next_page_token: String,
}

impl<T> Default for ListResponse<T> {
    fn default() -> Self {
        ListResponse {
            items: Vec::new(),
            next_page_token: String::new(),
        }
    }
}

impl<T> ListResponse<T> {
    /// Whether another page follows this one.
    pub fn has_next_page(&self) -> bool {
        !self.next_page_token.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize() {
        let got = ListOptions::default().normalize().unwrap();
        assert_eq!(got.page_size, DEFAULT_PAGE_SIZE);
        assert!(matches!(
            ListOptions::new(-1, "").normalize(),
            Err(StoreError::InvalidPageSize)
        ));
        assert_eq!(ListOptions::new(5, "t").normalize().unwrap().page_size, 5);
    }
}
