//! `index_range.hpp` — a `[begin, end)` range over a strong index type.
//!
//! Upstream's `index_range<Index>` iterates `index_iter` with `++Index`;
//! we model it as an `IntoIterator` yielding the index type. `IndexStep`
//! plays the role of `operator++` (std's `iter::Step` is unstable).

use crate::types::{FileIndex, PieceIndex};

/// `Index` concept: a strong index that can step to its successor
/// (`operator++` upstream).
pub trait IndexStep: Copy + PartialOrd + Eq {
    /// `++i`.
    fn next_index(self) -> Self;
}

impl IndexStep for PieceIndex {
    fn next_index(self) -> Self {
        self.next()
    }
}

impl IndexStep for FileIndex {
    fn next_index(self) -> Self {
        Self(self.0 + 1)
    }
}

impl IndexStep for i32 {
    fn next_index(self) -> Self {
        self + 1
    }
}

/// `index_range<Index>` — iterate `begin..end` inclusive-exclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexRange<I> {
    /// `_begin` upstream.
    pub begin: I,
    /// `_end` upstream.
    pub end: I,
}

/// `index_range{b, e}` construction.
///
/// Upstream asserts `begin <= end` in the `index_range` ctor — an end
/// before begin would iterate `cur != end` until index wraparound
/// rather than describe an empty range. `const` was dropped for the
/// `debug_assert` (trait calls aren't const); no caller needs it.
#[must_use]
pub fn index_range<I: PartialOrd>(begin: I, end: I) -> IndexRange<I> {
    debug_assert!(begin <= end, "index_range: begin must not exceed end");
    IndexRange { begin, end }
}

/// `index_iter<Index>` — the forward iterator.
#[derive(Debug, Clone)]
pub struct IndexIter<I> {
    cur: I,
    end: I,
}

impl<I: IndexStep> Iterator for IndexIter<I> {
    type Item = I;

    fn next(&mut self) -> Option<I> {
        if self.cur == self.end {
            None
        } else {
            let ret = self.cur;
            self.cur = self.cur.next_index();
            Some(ret)
        }
    }
}

impl<I: IndexStep> IntoIterator for IndexRange<I> {
    type Item = I;
    type IntoIter = IndexIter<I>;

    fn into_iter(self) -> Self::IntoIter {
        IndexIter {
            cur: self.begin,
            end: self.end,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iterates_indices() {
        let r = index_range(PieceIndex(0), PieceIndex(5));
        let v: Vec<PieceIndex> = r.into_iter().collect();
        assert_eq!(
            v,
            [
                PieceIndex(0),
                PieceIndex(1),
                PieceIndex(2),
                PieceIndex(3),
                PieceIndex(4)
            ]
        );

        let empty: Vec<FileIndex> = index_range(FileIndex(3), FileIndex(3))
            .into_iter()
            .collect();
        assert!(empty.is_empty());
    }
}
