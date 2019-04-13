use common::SizeOf;
use fnv::FnvBuildHasher;
use prelude::*;
use rand::{Rng, ThreadRng};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

/// Allocate a new end-user facing result table.
crate fn new(
    srmap: bool,
    cols: usize,
    key: &[usize],
    uid: usize,
) -> (SingleReadHandle, WriteHandle) {
    new_inner(srmap, cols, key, None, uid)
}

/// Allocate a new partially materialized end-user facing result table.
///
/// Misses in this table will call `trigger` to populate the entry, and retry until successful.
crate fn new_partial<F>(
    srmap: bool,
    cols: usize,
    key: &[usize],
    trigger: F,
    uid: usize,
) -> (SingleReadHandle, WriteHandle)
where
    F: Fn(&[DataType], Option<usize>) -> bool + 'static + Send + Sync,
{
    new_inner(srmap, cols, key, Some(Arc::new(trigger)), uid)
}

fn new_inner(
    srmap: bool,
    cols: usize,
    key: &[usize],
    trigger: Option<Arc<Fn(&[DataType], Option<usize>) -> bool + Send + Sync>>,
    uid: usize,
) -> (SingleReadHandle, WriteHandle) {
    let contiguous = {
        let mut contiguous = true;
        let mut last = None;
        for &k in key {
            if let Some(last) = last {
                if k != last + 1 {
                    contiguous = false;
                    break;
                }
            }
            last = Some(k);
        }
        contiguous
    };

    macro_rules! make_srmap {
        ($variant:tt) => {{
            use srmap;
            let (r, w) = srmap::construct(-1);
            (
                multir_sr::Handle::$variant(r),
                multiw_sr::Handle::$variant(w),
            )
        }};
    }

    macro_rules! make {
        ($variant:tt) => {{
            use evmap;
            let (r, w) = evmap::Options::default()
                .with_meta(-1)
                .with_hasher(FnvBuildHasher::default())
                .construct();
            (multir::Handle::$variant(r), multiw::Handle::$variant(w))
        }};
    }

    if srmap {
        println!("creating new srmap");
        let (r, w) = match (key.len(), srmap) {
            (0, _) => unreachable!(),
            (1, true) => make_srmap!(SingleSR),
            (2, true) => make_srmap!(DoubleSR),
            (_, true) => make_srmap!(ManySR),
            (_, false) => unreachable!(),
        };

        let w = WriteHandle {
            partial: trigger.is_some(),
            handle: WHandleVariant::Srmap(w),
            key: Vec::from(key),
            cols,
            contiguous,
            mem_size: 0,
            uid: uid,
        };

        let r = SingleReadHandle {
            handle: RHandleVariant::Srmap(r),
            trigger,
            key: Vec::from(key),
            uid: uid,
        };

        (r, w)
    } else {
        let (r, w) = match (key.len(), srmap) {
            (0, _) => unreachable!(),
            (1, false) => make!(Single),
            (2, false) => make!(Double),
            (_, false) => unreachable!(),
            (_, true) => unreachable!(),
        };

        let w = WriteHandle {
            partial: trigger.is_some(),
            handle: WHandleVariant::Evmap(w),
            key: Vec::from(key),
            cols,
            contiguous,
            mem_size: 0,
            uid: uid,
        };

        let r = SingleReadHandle {
            handle: RHandleVariant::Evmap(r),
            trigger,
            key: Vec::from(key),
            uid: uid,
        };

        (r, w)
    }
}

mod multir;
mod multir_sr;
mod multiw;
mod multiw_sr;

fn key_to_single(k: Key) -> Cow<DataType> {
    assert_eq!(k.len(), 1);
    match k {
        Cow::Owned(mut k) => Cow::Owned(k.swap_remove(0)),
        Cow::Borrowed(k) => Cow::Borrowed(&k[0]),
    }
}

fn key_to_double(k: Key) -> Cow<(DataType, DataType)> {
    assert_eq!(k.len(), 2);
    match k {
        Cow::Owned(k) => {
            let mut k = k.into_iter();
            let k1 = k.next().unwrap();
            let k2 = k.next().unwrap();
            Cow::Owned((k1, k2))
        }
        Cow::Borrowed(k) => Cow::Owned((k[0].clone(), k[1].clone())),
    }
}

enum WHandleVariant {
    Evmap(multiw::Handle),
    Srmap(multiw_sr::Handle),
}

crate struct WriteHandle {
    handle: WHandleVariant,
    partial: bool,
    cols: usize,
    key: Vec<usize>,
    contiguous: bool,
    mem_size: usize,
    pub uid: usize,
}

type Key<'a> = Cow<'a, [DataType]>;
crate struct MutWriteHandleEntry<'a> {
    handle: &'a mut WriteHandle,
    key: Key<'a>,
}
crate struct WriteHandleEntry<'a> {
    handle: &'a mut WriteHandle,
    key: Key<'a>,
}

impl<'a> MutWriteHandleEntry<'a> {
    crate fn mark_filled(&mut self) {
        if let WHandleVariant::Srmap(ref mut hand) = self.handle.handle {
            if let Some((None, _)) =
                hand.meta_get_and(Cow::Borrowed(&*self.key), |rs| rs.is_empty())
            {
                hand.clear(Cow::Borrowed(&*self.key));
            } else {
                unreachable!("attempted to fill already-filled key");
            }
            hand.refresh();
        } else {
            unimplemented!();
        }
    }

    crate fn mark_hole(&mut self) {
        if let WHandleVariant::Srmap(ref mut hand) = self.handle.handle {
            println!("mark hole");
            let size = hand
                .meta_get_and(Cow::Borrowed(&*self.key), |rs| {
                    rs.iter().map(|r| r.deep_size_of()).sum()
                })
                .map(|r| r.0.unwrap_or(0))
                .unwrap_or(0);
            self.handle.mem_size = self.handle.mem_size.checked_sub(size as usize).unwrap();
            hand.empty(Cow::Borrowed(&*self.key))
        } else {
            unimplemented!();
        }
    }
}

impl<'a> WriteHandleEntry<'a> {
    crate fn try_find_and<F, T>(&self, mut then: F) -> Result<(Option<T>, i64), ()>
    where
        F: FnMut(&[Vec<DataType>]) -> T,
    {
        if let WHandleVariant::Srmap(ref hand) = self.handle.handle {
            hand.meta_get_and(self.key.clone(), &mut then).ok_or(())
        } else {
            unimplemented!()
        }
        // match &self.handle.handle {
        //     Some(handle) => {
        //         handle.meta_get_and(self.key.clone(), &mut then).ok_or(())
        //     },
        //     None => {
        //         match &self.handle.handleSR {
        //             Some(handleSR) => {
        //                 handleSR.meta_get_and(self.key.clone(), &mut then).ok_or(())
        //             },
        //             None => {Err(())}
        //         }
        //
        //     }
        // }
    }
}

fn key_from_record<'a, R>(key: &[usize], contiguous: bool, record: R) -> Key<'a>
where
    R: Into<Cow<'a, [DataType]>>,
{
    match record.into() {
        Cow::Owned(mut record) => {
            let mut i = 0;
            let mut keep = key.iter().peekable();
            record.retain(|_| {
                i += 1;
                if let Some(&&next) = keep.peek() {
                    if next != i - 1 {
                        return false;
                    }
                } else {
                    return false;
                }

                assert_eq!(*keep.next().unwrap(), i - 1);
                true
            });
            Cow::Owned(record)
        }
        Cow::Borrowed(record) if contiguous => Cow::Borrowed(&record[key[0]..(key[0] + key.len())]),
        Cow::Borrowed(record) => Cow::Owned(key.iter().map(|&i| &record[i]).cloned().collect()),
    }
}

impl WriteHandle {
    crate fn clone_new_user(
        &self,
        r: &SingleReadHandle,
    ) -> Option<(SingleReadHandle, WriteHandle)> {
        if let WHandleVariant::Srmap(ref hand) = self.handle {
            let (uid, r_handle, w_handle) = hand.clone_new_user();
            // println!("CLONING NEW USER. uid: {}", uid);
            let r = r.clone_new_user(r_handle, uid.clone());
            let w = WriteHandle {
                handle: WHandleVariant::Srmap(w_handle),
                partial: self.partial.clone(),
                cols: self.cols.clone(),
                key: self.key.clone(),
                contiguous: self.contiguous.clone(),
                mem_size: self.mem_size.clone(),
                uid: uid.clone(),
            };
            return Some((r, w));
        }
        None
    }

    crate fn clone_new_user_partial(
        &self,
        r: &SingleReadHandle,
        trigger: Option<Arc<Fn(&[DataType], Option<usize>) -> bool + Send + Sync>>,
    ) -> Option<(SingleReadHandle, WriteHandle)> {
        if let WHandleVariant::Srmap(ref hand) = self.handle {
            let (uid, r_handle, w_handle) = hand.clone_new_user();
            // println!("CLONING NEW USER. uid: {}", uid);
            let r = r.clone_new_user_partial(r_handle, uid.clone(), trigger);
            let w = WriteHandle {
                handle: WHandleVariant::Srmap(w_handle),
                partial: self.partial.clone(),
                cols: self.cols.clone(),
                key: self.key.clone(),
                contiguous: self.contiguous.clone(),
                mem_size: self.mem_size.clone(),
                uid: uid.clone(),
            };
            return Some((r, w));
        }
        None
    }

    crate fn clone(&self, r: &SingleReadHandle) -> Option<(SingleReadHandle, WriteHandle)> {
        if let WHandleVariant::Srmap(ref hand) = self.handle {
            let w_handle = hand.clone();
            if let RHandleVariant::Srmap(ref rhand) = r.handle {
                let w = WriteHandle {
                    handle: WHandleVariant::Srmap(w_handle),
                    partial: self.partial.clone(),
                    cols: self.cols.clone(),
                    key: self.key.clone(),
                    contiguous: self.contiguous.clone(),
                    mem_size: self.mem_size.clone(),
                    uid: self.uid.clone(),
                };
                return Some((r.clone(rhand.clone(), self.uid.clone()), w));
            }
        }
        None
    }

    crate fn mut_with_key<'a, K>(&'a mut self, key: K) -> MutWriteHandleEntry<'a>
    where
        K: Into<Key<'a>>,
    {
        MutWriteHandleEntry {
            handle: self,
            key: key.into(),
        }
    }

    crate fn with_key<'a, K>(&'a mut self, key: K) -> WriteHandleEntry<'a>
    where
        K: Into<Key<'a>>,
    {
        WriteHandleEntry {
            handle: self,
            key: key.into(),
        }
    }

    #[allow(dead_code)]
    fn mut_entry_from_record<'a, R>(&'a mut self, record: R) -> MutWriteHandleEntry<'a>
    where
        R: Into<Cow<'a, [DataType]>>,
    {
        let key = key_from_record(&self.key[..], self.contiguous, record);
        self.mut_with_key(key)
    }

    crate fn entry_from_record<'a, R>(&'a mut self, record: R) -> WriteHandleEntry<'a>
    where
        R: Into<Cow<'a, [DataType]>>,
    {
        let key = key_from_record(&self.key[..], self.contiguous, record);
        self.with_key(key)
    }

    crate fn swap(&mut self) {
        match self.handle {
            WHandleVariant::Srmap(ref mut hand) => hand.refresh(),
            WHandleVariant::Evmap(ref mut hand) => hand.refresh(),
        }
    }

    /// Add a new set of records to the backlog.
    ///
    /// These will be made visible to readers after the next call to `swap()`.
    crate fn add<I>(&mut self, rs: I, _id: Option<usize>)
    where
        I: IntoIterator<Item = Record>,
    {
        match self.handle {
            WHandleVariant::Srmap(ref mut hand) => {
                let mem_delta = hand.add(&self.key[..], self.cols, rs, Some(self.uid));
                if mem_delta > 0 {
                    self.mem_size += mem_delta as usize;
                } else if mem_delta < 0 {
                    self.mem_size = self
                        .mem_size
                        .checked_sub(mem_delta.checked_abs().unwrap() as usize)
                        .unwrap();
                }
                // hand.refresh();
            }
            WHandleVariant::Evmap(ref mut hand) => {
                let mem_delta = hand.add(&self.key[..], self.cols, rs);
                if mem_delta > 0 {
                    self.mem_size += mem_delta as usize;
                } else if mem_delta < 0 {
                    self.mem_size = self
                        .mem_size
                        .checked_sub(mem_delta.checked_abs().unwrap() as usize)
                        .unwrap();
                }
            }
        }
    }

    crate fn is_partial(&self) -> bool {
        self.partial
    }

    /// Evict `count` randomly selected keys from state and return them along with the number of
    /// bytes that will be freed once the underlying `evmap` applies the operation.
    crate fn evict_random_key(&mut self, rng: &mut ThreadRng) -> u64 {
        match self.handle {
            WHandleVariant::Srmap(ref mut hand) => {
                let mut bytes_to_be_freed = 0;
                if self.mem_size > 0 {
                    if hand.is_empty() {
                        unreachable!("mem size is {}, but map is empty", self.mem_size);
                    }

                    match hand.empty_at_index(rng.gen()) {
                        None => (),
                        Some(vs) => {
                            let size: u64 = vs.iter().map(|r| r.deep_size_of() as u64).sum();
                            bytes_to_be_freed += size;
                        }
                    }
                    self.mem_size = self
                        .mem_size
                        .checked_sub(bytes_to_be_freed as usize)
                        .unwrap();
                }
                bytes_to_be_freed
            }
            WHandleVariant::Evmap(ref mut hand) => {
                let mut bytes_to_be_freed = 0;
                if self.mem_size > 0 {
                    if hand.is_empty() {
                        unreachable!("mem size is {}, but map is empty", self.mem_size);
                    }

                    match hand.empty_at_index(rng.gen()) {
                        None => (),
                        Some(vs) => {
                            let size: u64 = vs.iter().map(|r| r.deep_size_of() as u64).sum();
                            bytes_to_be_freed += size;
                        }
                    }
                    self.mem_size = self
                        .mem_size
                        .checked_sub(bytes_to_be_freed as usize)
                        .unwrap();
                }
                bytes_to_be_freed
            }
        }
    }

    crate fn clear(&mut self) {
        if let WHandleVariant::Evmap(ref mut h) = self.handle {
            self.mem_size = 0;
            h.purge();
        }
    }
}

impl SizeOf for WriteHandle {
    fn size_of(&self) -> u64 {
        use std::mem::size_of;

        size_of::<Self>() as u64
    }

    fn deep_size_of(&self) -> u64 {
        self.mem_size as u64
    }
}

#[derive(Clone)]
enum RHandleVariant {
    Evmap(multir::Handle),
    Srmap(multir_sr::Handle),
}

/// Handle to get the state of a single shard of a reader.
#[derive(Clone)]
pub struct SingleReadHandle {
    handle: RHandleVariant,
    trigger: Option<Arc<Fn(&[DataType], Option<usize>) -> bool + Send + Sync>>,
    key: Vec<usize>,
    pub uid: usize,
}

impl SingleReadHandle {
    pub fn clone_new_user(&self, r: multir_sr::Handle, uid: usize) -> SingleReadHandle {
        SingleReadHandle {
            handle: RHandleVariant::Srmap(r),
            trigger: self.trigger.clone(),
            key: self.key.clone(),
            uid: uid.clone(),
        }
    }

    pub fn clone_new_user_partial(
        &self,
        r: multir_sr::Handle,
        uid: usize,
        trigger: Option<Arc<Fn(&[DataType], Option<usize>) -> bool + Send + Sync>>,
    ) -> SingleReadHandle {
        SingleReadHandle {
            handle: RHandleVariant::Srmap(r),
            trigger,
            key: self.key.clone(),
            uid: uid.clone(),
        }
    }

    pub fn clone(&self, r: multir_sr::Handle, uid: usize) -> SingleReadHandle {
        SingleReadHandle {
            handle: RHandleVariant::Srmap(r),
            trigger: self.trigger.clone(),
            key: self.key.clone(),
            uid: uid.clone(),
        }
    }

    pub fn universe(&self) -> usize {
        self.uid.clone()
    }

    /// Trigger a replay of a missing key from a partially materialized view.
    pub fn trigger(&self, key: &[DataType], id: Option<usize>) -> bool {
        assert!(
            self.trigger.is_some(),
            "tried to trigger a replay for a fully materialized view"
        );

        // trigger a replay to populate
        (*self.trigger.as_ref().unwrap())(key, id)
    }

    /// Find all entries that matched the given conditions.
    ///
    /// Returned records are passed to `then` before being returned.
    ///
    /// Note that not all writes will be included with this read -- only those that have been
    /// swapped in by the writer.
    ///
    /// Holes in partially materialized state are returned as `Ok((None, _))`.
    pub fn try_find_and<F, T>(&self, key: &[DataType], mut then: F) -> Result<(Option<T>, i64), ()>
    where
        F: FnMut(&[Vec<DataType>]) -> T,
    {
        match self.handle {
            RHandleVariant::Srmap(ref hand) => {
                // println!("try find and. uid: {:?}", self.uid);
                hand.meta_get_and(key, &mut then)
                    .ok_or(())
                    .map(|(mut records, meta)| {
                        if records.is_none() && self.trigger.is_none() {
                            records = Some(then(&[]));
                        }
                        (records, meta)
                    })
            }
            RHandleVariant::Evmap(ref hand) => {
                hand.meta_get_and(key, &mut then)
                    .ok_or(())
                    .map(|(mut records, meta)| {
                        if records.is_none() && self.trigger.is_none() {
                            records = Some(then(&[]));
                        }
                        (records, meta)
                    })
            }
        }
    }

    pub fn len(&self) -> usize {
        match self.handle {
            RHandleVariant::Srmap(ref hand) => hand.len(),
            RHandleVariant::Evmap(ref hand) => hand.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_works() {
        let a = vec![1.into(), "a".into()];

        let (r, mut w) = new(2, &[0]);

        // initially, store is uninitialized
        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()), Err(()));

        w.swap();

        // after first swap, it is empty, but ready
        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()), Ok((Some(0), -1)));

        w.add(vec![Record::Positive(a.clone())]);

        // it is empty even after an add (we haven't swapped yet)
        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()), Ok((Some(0), -1)));

        w.swap();

        // but after the swap, the record is there!
        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, Some(1));
        assert!(r
            .try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == a[0] && r[1] == a[1]))
            .unwrap()
            .0
            .unwrap());
    }

    #[test]
    fn busybusybusy() {
        use std::thread;

        let n = 10000;
        let (r, mut w) = new(1, &[0]);
        thread::spawn(move || {
            for i in 0..n {
                w.add(vec![Record::Positive(vec![i.into()])]);
                w.swap();
            }
        });

        for i in 0..n {
            let i = &[i.into()];
            loop {
                match r.try_find_and(i, |rs| rs.len()) {
                    Ok((None, _)) => continue,
                    Ok((Some(1), _)) => break,
                    Ok((Some(i), _)) => assert_ne!(i, 1),
                    Err(()) => continue,
                }
            }
        }
    }

    #[test]
    fn minimal_query() {
        let a = vec![1.into(), "a".into()];
        let b = vec![1.into(), "b".into()];

        let (r, mut w) = new(2, &[0]);
        w.add(vec![Record::Positive(a.clone())]);
        w.swap();
        w.add(vec![Record::Positive(b.clone())]);

        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, Some(1));
        assert!(r
            .try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == a[0] && r[1] == a[1]))
            .unwrap()
            .0
            .unwrap());
    }

    #[test]
    fn non_minimal_query() {
        let a = vec![1.into(), "a".into()];
        let b = vec![1.into(), "b".into()];
        let c = vec![1.into(), "c".into()];

        let (r, mut w) = new(2, &[0]);
        w.add(vec![Record::Positive(a.clone())]);
        w.add(vec![Record::Positive(b.clone())]);
        w.swap();
        w.add(vec![Record::Positive(c.clone())]);

        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, Some(2));
        assert!(r
            .try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == a[0] && r[1] == a[1]))
            .unwrap()
            .0
            .unwrap());
        assert!(r
            .try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == b[0] && r[1] == b[1]))
            .unwrap()
            .0
            .unwrap());
    }

    #[test]
    fn absorb_negative_immediate() {
        let a = vec![1.into(), "a".into()];
        let b = vec![1.into(), "b".into()];

        let (r, mut w) = new(2, &[0]);
        w.add(vec![Record::Positive(a.clone())]);
        w.add(vec![Record::Positive(b.clone())]);
        w.add(vec![Record::Negative(a.clone())]);
        w.swap();

        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, Some(1));
        assert!(r
            .try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == b[0] && r[1] == b[1]))
            .unwrap()
            .0
            .unwrap());
    }

    #[test]
    fn srmap_works() {
        let a = vec![1.into(), "a".into()];
        let b = vec![1.into(), "b".into()];
        let a_rec = vec![Record::Positive(a.clone())];
        let b_rec = vec![Record::Positive(b.clone())];

        let (mut r1, mut w1) = new(true, 2, &[0], 0);
        let (mut r2, mut w2) = w1.clone_new_user(r1.clone());
        let (mut r3, mut w3) = w1.clone_new_user(r1.clone());

        w1.add(a_rec.clone());
        w2.add(a_rec.clone());
        w2.add(b_rec.clone());
        w3.add(a_rec.clone());

        r1.try_find_and(&a[0..1], |rs| println!("Rs: {:?}", rs.clone()));
        r2.try_find_and(&a[0..1], |rs| println!("Rs: {:?}", rs.clone()));
        r3.try_find_and(&a[0..1], |rs| println!("Rs: {:?}", rs.clone()));
        r3.try_find_and(&b[0..1], |rs| println!("Rs: {:?}", rs.clone()));
        r2.try_find_and(&b[0..1], |rs| println!("Rs: {:?}", rs.clone()));

        // assert_eq!(r3.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, Some(1));
        // assert_eq!(r2.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, Some(1));
        // assert_eq!(r2.try_find_and(&b[0..1], |rs| rs.len()).unwrap().0, Some(1));
        // assert_eq!(r3.try_find_and(&b[0..1], |rs| rs.len()).unwrap().0, Some(0));
    }

    #[test]
    fn absorb_negative_later() {
        let a = vec![1.into(), "a".into()];
        let b = vec![1.into(), "b".into()];

        let (r, mut w) = new(2, &[0]);
        w.add(vec![Record::Positive(a.clone())]);
        w.add(vec![Record::Positive(b.clone())]);
        w.swap();
        w.add(vec![Record::Negative(a.clone())]);
        w.swap();

        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, Some(1));
        assert!(r
            .try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == b[0] && r[1] == b[1]))
            .unwrap()
            .0
            .unwrap());
    }

    #[test]
    fn absorb_multi() {
        let a = vec![1.into(), "a".into()];
        let b = vec![1.into(), "b".into()];
        let c = vec![1.into(), "c".into()];

        let (r, mut w) = new(2, &[0]);
        w.add(vec![
            Record::Positive(a.clone()),
            Record::Positive(b.clone()),
        ]);
        w.swap();

        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, Some(2));
        assert!(r
            .try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == a[0] && r[1] == a[1]))
            .unwrap()
            .0
            .unwrap());
        assert!(r
            .try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == b[0] && r[1] == b[1]))
            .unwrap()
            .0
            .unwrap());

        w.add(vec![
            Record::Negative(a.clone()),
            Record::Positive(c.clone()),
            Record::Negative(c.clone()),
        ]);
        w.swap();

        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, Some(1));
        assert!(r
            .try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == b[0] && r[1] == b[1]))
            .unwrap()
            .0
            .unwrap());
    }
}
