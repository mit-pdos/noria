use super::mk_key::MakeKey;
use crate::prelude::*;
use crate::state::keyed_state::{KeyedState, VersionedRows, VersionedRowsHeader};
use common::SizeOf;
use rand::prelude::*;
use std::rc::Rc;

pub(super) struct SingleState {
    key: Vec<usize>,
    state: KeyedState,
    partial: bool,
    rows: usize,
}

macro_rules! insert_row_match_impl {
    ($self:ident, $r:ident, $map:ident) => {{
        let key = MakeKey::from_row(&$self.key, &*$r);
        match $map.entry(key) {
            Entry::Occupied(mut vrs) => {
                // TODO: Fix the begin_ts and end_ts.
                vrs.get_mut().rows.push($r);
                vrs.get_mut().headers.push(VersionedRowsHeader::default());
            }
            Entry::Vacant(..) if $self.partial => return false,
            rs @ Entry::Vacant(..) => {
                let vrs = rs.or_default();
                // TODO: Fix the begin_ts and end_ts.
                vrs.rows.push($r);
                vrs.headers.push(VersionedRowsHeader::default());
            }
        }
    }};
}

macro_rules! remove_row_match_impl {
    ($self:ident, $r:ident, $do_remove:ident, $map:ident, $($hint:tt)*) => {{
        // TODO: can we avoid the Clone here?
        let key = MakeKey::from_row(&$self.key, $r);
        // TODO: Deal with headers.
        if let Some(VersionedRows { headers: _, ref mut rows }) = $map.get_mut::<$($hint)*>(&key) {
            return $do_remove(&mut $self.rows, rows);
        }
    }};
}

impl SingleState {
    pub(super) fn new(columns: &[usize], partial: bool) -> Self {
        Self {
            key: Vec::from(columns),
            state: columns.into(),
            partial,
            rows: 0,
        }
    }
    /// Inserts the given record, or returns false if a hole was encountered (and the record hence
    /// not inserted).
    pub(super) fn insert_row(&mut self, r: Row) -> bool {
        use indexmap::map::Entry;
        match self.state {
            KeyedState::Single(ref mut map) => {
                // treat this specially to avoid the extra Vec
                debug_assert_eq!(self.key.len(), 1);
                // i *wish* we could use the entry API here, but it would mean an extra clone
                // in the common case of an entry already existing for the given key...
                if let Some(VersionedRows {
                    ref mut headers,
                    ref mut rows,
                }) = map.get_mut(&r[self.key[0]])
                {
                    self.rows += 1;
                    // TODO: Fix the begin_ts and end_ts.
                    rows.push(r);
                    headers.push(VersionedRowsHeader::default());
                    return true;
                } else if self.partial {
                    // trying to insert a record into partial materialization hole!
                    return false;
                }
                // TODO: Fix the begin_ts and end_ts.
                map.insert(
                    r[self.key[0]].clone(),
                    VersionedRows {
                        headers: vec![VersionedRowsHeader::default()],
                        rows: vec![r],
                    },
                );
            }
            KeyedState::Double(ref mut map) => insert_row_match_impl!(self, r, map),
            KeyedState::Tri(ref mut map) => insert_row_match_impl!(self, r, map),
            KeyedState::Quad(ref mut map) => insert_row_match_impl!(self, r, map),
            KeyedState::Quin(ref mut map) => insert_row_match_impl!(self, r, map),
            KeyedState::Sex(ref mut map) => insert_row_match_impl!(self, r, map),
        }

        self.rows += 1;
        true
    }

    /// Attempt to remove row `r`.
    pub(super) fn remove_row(&mut self, r: &[DataType], hit: &mut bool) -> Option<Row> {
        let mut do_remove = |self_rows: &mut usize, rs: &mut Vec<Row>| -> Option<Row> {
            *hit = true;
            let rm = if rs.len() == 1 {
                // it *should* be impossible to get a negative for a record that we don't have
                debug_assert_eq!(r, &rs[0][..]);
                Some(rs.swap_remove(0))
            } else if let Some(i) = rs.iter().position(|rsr| &rsr[..] == r) {
                Some(rs.swap_remove(i))
            } else {
                None
            };

            if rm.is_some() {
                *self_rows = self_rows.checked_sub(1).unwrap();
            }
            rm
        };

        match self.state {
            KeyedState::Single(ref mut map) => {
                if let Some(VersionedRows {
                    headers: _,
                    ref mut rows,
                }) = map.get_mut(&r[self.key[0]])
                {
                    // TODO: deal with deleted headers.
                    return do_remove(&mut self.rows, rows);
                }
            }
            KeyedState::Double(ref mut map) => {
                remove_row_match_impl!(self, r, do_remove, map, (DataType, _))
            }
            KeyedState::Tri(ref mut map) => {
                remove_row_match_impl!(self, r, do_remove, map, (DataType, _, _))
            }
            KeyedState::Quad(ref mut map) => {
                remove_row_match_impl!(self, r, do_remove, map, (DataType, _, _, _))
            }
            KeyedState::Quin(ref mut map) => {
                remove_row_match_impl!(self, r, do_remove, map, (DataType, _, _, _, _))
            }
            KeyedState::Sex(ref mut map) => {
                remove_row_match_impl!(self, r, do_remove, map, (DataType, _, _, _, _, _))
            }
        }
        None
    }

    pub(super) fn mark_filled(&mut self, key: Vec<DataType>) {
        let mut key = key.into_iter();
        let replaced = match self.state {
            KeyedState::Single(ref mut map) => {
                map.insert(key.next().unwrap(), VersionedRows::default())
            }
            KeyedState::Double(ref mut map) => map.insert(
                (key.next().unwrap(), key.next().unwrap()),
                VersionedRows::default(),
            ),
            KeyedState::Tri(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                VersionedRows::default(),
            ),
            KeyedState::Quad(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                VersionedRows::default(),
            ),
            KeyedState::Quin(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                VersionedRows::default(),
            ),
            KeyedState::Sex(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                VersionedRows::default(),
            ),
        };
        assert!(replaced.is_none());
    }

    pub(super) fn mark_hole(&mut self, key: &[DataType]) -> u64 {
        let removed = match self.state {
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
        };
        // mark_hole should only be called on keys we called mark_filled on
        removed
            .unwrap()
            .rows
            .iter()
            .filter(|r| Rc::strong_count(&r.0) == 1)
            .map(SizeOf::deep_size_of)
            .sum()
    }

    pub(super) fn clear(&mut self) {
        self.rows = 0;
        match self.state {
            KeyedState::Single(ref mut map) => map.clear(),
            KeyedState::Double(ref mut map) => map.clear(),
            KeyedState::Tri(ref mut map) => map.clear(),
            KeyedState::Quad(ref mut map) => map.clear(),
            KeyedState::Quin(ref mut map) => map.clear(),
            KeyedState::Sex(ref mut map) => map.clear(),
        };
    }

    /// Evict `count` randomly selected keys from state and return them along with the number of
    /// bytes freed.
    pub(super) fn evict_random_keys(
        &mut self,
        count: usize,
        rng: &mut ThreadRng,
    ) -> (u64, Vec<Vec<DataType>>) {
        let mut bytes_freed = 0;
        let mut keys = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some((n, key)) = self.state.evict_with_seed(rng.gen()) {
                bytes_freed += n;
                keys.push(key);
            } else {
                break;
            }
        }
        (bytes_freed, keys)
    }

    /// Evicts a specified key from this state, returning the number of bytes freed.
    pub(super) fn evict_keys(&mut self, keys: &[Vec<DataType>]) -> u64 {
        keys.iter().map(|k| self.state.evict(k)).sum()
    }

    pub(super) fn values<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Vec<Row>> + 'a> {
        match self.state {
            KeyedState::Single(ref map) => Box::new(map.values().map(|vrs| &vrs.rows)),
            KeyedState::Double(ref map) => Box::new(map.values().map(|vrs| &vrs.rows)),
            KeyedState::Tri(ref map) => Box::new(map.values().map(|vrs| &vrs.rows)),
            KeyedState::Quad(ref map) => Box::new(map.values().map(|vrs| &vrs.rows)),
            KeyedState::Quin(ref map) => Box::new(map.values().map(|vrs| &vrs.rows)),
            KeyedState::Sex(ref map) => Box::new(map.values().map(|vrs| &vrs.rows)),
        }
    }
    pub(super) fn key(&self) -> &[usize] {
        &self.key
    }
    pub(super) fn partial(&self) -> bool {
        self.partial
    }
    pub(super) fn rows(&self) -> usize {
        self.rows
    }
    pub(super) fn lookup<'a>(&'a self, key: &KeyType, ts: Option<Timestamp>) -> LookupResult<'a> {
        if let Some(rs) = self.state.lookup(key, ts) {
            LookupResult::Some(RecordResult::Iterated(rs))
        } else if self.partial() {
            // partially materialized, so this is a hole (empty results would be vec![])
            LookupResult::Missing
        } else {
            LookupResult::Some(RecordResult::Owned(vec![]))
        }
    }
}
