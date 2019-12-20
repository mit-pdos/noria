use fnv::FnvBuildHasher;
use indexmap::IndexMap;
use std::rc::Rc;

use super::mk_key::MakeKey;
use crate::prelude::*;
use common::{SizeOf, Timestamp};

type FnvHashMap<K, V> = IndexMap<K, V, FnvBuildHasher>;

// MVTO header
#[derive(Clone)]
pub(super) struct VersionedRowsHeader {
    pub(super) beg_ts: Timestamp,
    pub(super) end_ts: Option<Timestamp>, // None for +INF
}

pub(crate) const VERSIONED_ROW_HEADER_SIZE: u64 = std::mem::size_of::<VersionedRowsHeader>() as u64;

impl Default for VersionedRowsHeader {
    fn default() -> Self {
        Self {
            beg_ts: 0,
            end_ts: None,
        }
    }
}

impl VersionedRowsHeader {
    pub(crate) fn with_begin_ts(beg_ts: Timestamp) -> VersionedRowsHeader {
        Self {
            beg_ts,
            end_ts: None,
        }
    }
}

// An AoS array of (txn_id, beg_ts, end_ts) and rows.
pub(super) struct VersionedRows {
    pub(super) headers: Vec<VersionedRowsHeader>,
    pub(super) rows: Vec<Row>,
}

#[derive(Clone, Copy)]
pub(crate) struct VersionedRowsIter<'rows> {
    vrs: &'rows VersionedRows,
    cur: usize,
    ts: Option<Timestamp>,
}

impl VersionedRows {
    fn num_rows(&self) -> usize {
        debug_assert_eq!(self.rows.len(), self.headers.len());
        self.rows.len()
    }

    fn row_iter_at<'a>(&'a self, ts: Option<Timestamp>) -> VersionedRowsIter<'a> {
        VersionedRowsIter {
            vrs: &self,
            cur: 0,
            ts,
        }
    }
}

impl<'a> Iterator for VersionedRowsIter<'a> {
    type Item = &'a Row;
    fn next(&mut self) -> Option<Self::Item> {
        let curr_ts = self.ts.clone();
        let visible = move |header: &VersionedRowsHeader| match curr_ts {
            Some(ts) => {
                if header.beg_ts > ts {
                    return false;
                }
                if let Some(end_ts) = header.end_ts {
                    if end_ts <= ts {
                        return false;
                    }
                }
                true
            }
            None => header.end_ts.is_none(),
        };
        if self.cur == self.vrs.num_rows() {
            return None;
        }
        while !visible(&self.vrs.headers[self.cur]) {
            self.cur += 1;
        }
        Some(&self.vrs.rows[self.cur])
    }
}

impl Default for VersionedRows {
    fn default() -> Self {
        Self {
            headers: vec![],
            rows: vec![],
        }
    }
}

#[allow(clippy::type_complexity)]
pub(super) enum KeyedState {
    Single(FnvHashMap<DataType, VersionedRows>),
    Double(FnvHashMap<(DataType, DataType), VersionedRows>),
    Tri(FnvHashMap<(DataType, DataType, DataType), VersionedRows>),
    Quad(FnvHashMap<(DataType, DataType, DataType, DataType), VersionedRows>),
    Quin(FnvHashMap<(DataType, DataType, DataType, DataType, DataType), VersionedRows>),
    Sex(FnvHashMap<(DataType, DataType, DataType, DataType, DataType, DataType), VersionedRows>),
}

impl KeyedState {
    pub(super) fn lookup<'a>(
        &'a self,
        key: &KeyType,
        tid: Option<Timestamp>,
    ) -> Option<VersionedRowsIter<'a>> {
        match (self, key) {
            // TODO: filter out only visible rows.
            (&KeyedState::Single(ref m), &KeyType::Single(k)) => {
                m.get(k).map(|vrs| vrs.row_iter_at(tid))
            }
            (&KeyedState::Double(ref m), &KeyType::Double(ref k)) => {
                m.get(k).map(|vrs| vrs.row_iter_at(tid))
            }
            (&KeyedState::Tri(ref m), &KeyType::Tri(ref k)) => {
                m.get(k).map(|vrs| vrs.row_iter_at(tid))
            }
            (&KeyedState::Quad(ref m), &KeyType::Quad(ref k)) => {
                m.get(k).map(|vrs| vrs.row_iter_at(tid))
            }
            (&KeyedState::Quin(ref m), &KeyType::Quin(ref k)) => {
                m.get(k).map(|vrs| vrs.row_iter_at(tid))
            }
            (&KeyedState::Sex(ref m), &KeyType::Sex(ref k)) => {
                m.get(k).map(|vrs| vrs.row_iter_at(tid))
            }
            _ => unreachable!(),
        }
    }

    /// Remove all rows for a randomly chosen key seeded by `seed`, returning that key along with
    /// the number of bytes freed. Returns `None` if map is empty.
    pub(super) fn evict_with_seed(&mut self, seed: usize) -> Option<(u64, Vec<DataType>)> {
        let (rs, key) = match *self {
            KeyedState::Single(ref mut m) => {
                let index = seed % m.len();
                m.swap_remove_index(index).map(|(k, rs)| (rs, vec![k]))
            }
            KeyedState::Double(ref mut m) => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|(k, rs)| (rs, vec![k.0, k.1]))
            }
            KeyedState::Tri(ref mut m) => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|(k, rs)| (rs, vec![k.0, k.1, k.2]))
            }
            KeyedState::Quad(ref mut m) => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|(k, rs)| (rs, vec![k.0, k.1, k.2, k.3]))
            }
            KeyedState::Quin(ref mut m) => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|(k, rs)| (rs, vec![k.0, k.1, k.2, k.3, k.4]))
            }
            KeyedState::Sex(ref mut m) => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|(k, rs)| (rs, vec![k.0, k.1, k.2, k.3, k.4, k.5]))
            }
        }?;
        Some((
            rs.rows
                .iter()
                .filter(|r| Rc::strong_count(&r.0) == 1)
                .map(|r| {
                    SizeOf::deep_size_of(r) + std::mem::size_of::<VersionedRowsHeader>() as u64
                })
                .sum(),
            key,
        ))
    }

    /// Remove all rows for the given key, returning the number of bytes freed.
    pub(super) fn evict(&mut self, key: &[DataType]) -> u64 {
        match *self {
            KeyedState::Single(ref mut m) => m.swap_remove(&(key[0])),
            KeyedState::Double(ref mut m) => {
                m.swap_remove::<(DataType, _)>(&MakeKey::from_key(key))
            }
            KeyedState::Tri(ref mut m) => {
                m.swap_remove::<(DataType, _, _)>(&MakeKey::from_key(key))
            }
            KeyedState::Quad(ref mut m) => {
                m.swap_remove::<(DataType, _, _, _)>(&MakeKey::from_key(key))
            }
            KeyedState::Quin(ref mut m) => {
                m.swap_remove::<(DataType, _, _, _, _)>(&MakeKey::from_key(key))
            }
            KeyedState::Sex(ref mut m) => {
                m.swap_remove::<(DataType, _, _, _, _, _)>(&MakeKey::from_key(key))
            }
        }
        .map(|rows| {
            rows.rows
                .iter()
                .filter(|r| Rc::strong_count(&r.0) == 1)
                .map(SizeOf::deep_size_of)
                .sum()
        })
        .unwrap_or(0)
    }
}

impl<'a> Into<KeyedState> for &'a [usize] {
    fn into(self) -> KeyedState {
        match self.len() {
            0 => unreachable!(),
            1 => KeyedState::Single(FnvHashMap::default()),
            2 => KeyedState::Double(FnvHashMap::default()),
            3 => KeyedState::Tri(FnvHashMap::default()),
            4 => KeyedState::Quad(FnvHashMap::default()),
            5 => KeyedState::Quin(FnvHashMap::default()),
            6 => KeyedState::Sex(FnvHashMap::default()),
            x => panic!("invalid compound key of length: {}", x),
        }
    }
}
