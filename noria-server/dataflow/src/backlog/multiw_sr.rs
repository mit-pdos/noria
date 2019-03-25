use super::{key_to_double, key_to_single, Key};
use evmap;
use fnv::FnvBuildHasher;
use prelude::*;

#[derive(Clone)]
pub enum Handle {
    SingleSR(srmap::handle::handle::Handle<DataType, Vec<DataType>, i64>),
    DoubleSR(srmap::handle::handle::Handle<(DataType, DataType), Vec<DataType>, i64>),
    ManySR(srmap::handle::handle::Handle<Vec<DataType>, Vec<DataType>, i64>),
}


impl Handle {

    pub fn is_empty(&self) -> bool {
        match *self {
            Handle::SingleSR(ref h) => h.is_empty(),
            Handle::DoubleSR(ref h) => h.is_empty(),
            Handle::ManySR(ref h) => h.is_empty(),
        }
    }

    pub fn clone_new_user(&mut self) -> (usize, super::multir_sr::Handle, Handle) {
         match *self {
             Handle::SingleSR(ref mut h) => {
                                            let (uid, mut inr, mut inw) = h.clone_new_user();
                                            (uid, super::multir_sr::Handle::SingleSR(inr), Handle::SingleSR(inw))
                                             },
             Handle::DoubleSR(ref mut h) => {
                                             let (uid, mut inr, mut inw) = h.clone_new_user();
                                             (uid, super::multir_sr::Handle::DoubleSR(inr), Handle::DoubleSR(inw))
                                             },
             Handle::ManySR(ref mut h) => {
                                             let (uid, mut inr, mut inw) = h.clone_new_user();
                                             (uid, super::multir_sr::Handle::ManySR(inr), Handle::ManySR(inw))
                                           },
         }
    }

    pub fn clear(&mut self, k: Key) {
        match *self {
            Handle::SingleSR(ref mut h) => {h.clear(key_to_single(k).into_owned())},
            Handle::DoubleSR(ref mut h) => {h.clear(key_to_double(k).into_owned())},
            Handle::ManySR(ref mut h) => {h.clear(k.into_owned())},
        }
    }

    pub fn empty(&mut self, k: Key) {
        match *self {
            Handle::SingleSR(ref mut h) => {h.empty(key_to_single(k).into_owned())},
            Handle::DoubleSR(ref mut h) => {h.empty(key_to_double(k).into_owned())},
            Handle::ManySR(ref mut h) => {h.empty(k.into_owned())},
        }
    }

    /// Evict `count` randomly selected keys from state and return them along with the number of
    /// bytes freed.
    pub fn empty_at_index(&mut self, index: usize) -> Option<&Vec<Vec<DataType>>> {
        match *self {
            Handle::SingleSR(ref mut h) => unimplemented!(),
            Handle::DoubleSR(ref mut h) => unimplemented!(),
            Handle::ManySR(ref mut h) => unimplemented!(),
        }
    }

    pub fn refresh(&mut self) {
        match *self {
            Handle::SingleSR(ref mut h) => {h.refresh()},
            Handle::DoubleSR(ref mut h) => {h.refresh()},
            Handle::ManySR(ref mut h) => {h.refresh()},
        }
    }

    pub fn meta_get_and<F, T>(&self, key: Key, then: F) -> Option<(Option<T>, i64)>
    where
        F: FnOnce(&[Vec<DataType>]) -> T,
    {
        match *self {
            Handle::SingleSR(ref h) => {
                assert_eq!(key.len(), 1);
                h.meta_get_and(&key[0], then)
            },
            Handle::DoubleSR(ref h) => {
                assert_eq!(key.len(), 2);
                // we want to transmute &[T; 2] to &(T, T), but that's not actually safe
                // we're not guaranteed that they have the same memory layout
                // we *could* just clone DataType, but that would mean dealing with string refcounts
                // so instead, we play a trick where we memcopy onto the stack and then forget!
                //
                // h/t https://gist.github.com/mitsuhiko/f6478a0dd1ef174b33c63d905babc89a
                use std::mem;
                use std::ptr;
                unsafe {
                    let mut stack_key: (DataType, DataType) = mem::uninitialized();
                    ptr::copy_nonoverlapping(
                        &key[0] as *const DataType,
                        &mut stack_key.0 as *mut DataType,
                        1,
                    );
                    ptr::copy_nonoverlapping(
                        &key[1] as *const DataType,
                        &mut stack_key.1 as *mut DataType,
                        1,
                    );
                    let v = h.meta_get_and(&stack_key, then);
                    mem::forget(stack_key);
                    v
                }
            },
            Handle::ManySR(ref h) => {
                h.meta_get_and(&key.to_vec(), then)
            },
        }
    }


    pub fn add<I>(&mut self, key: &[usize], cols: usize, rs: I, id: Option<usize>) -> isize
    where
        I: IntoIterator<Item = Record>,
    {
        println!("adding");
        // println!("working yay: key: {:?} ", key.clone());
        let mut memory_delta = 0isize;
        match *self {
            Handle::SingleSR(ref mut h) => {
                assert_eq!(key.len(), 1);
                for r in rs {
                    debug_assert!(r.len() >= cols);
                    match r {
                        Record::Positive(r) => {
                            memory_delta += r.deep_size_of() as isize;
                            h.insert(r[key[0]].clone(), r, id);
                        }
                        Record::Negative(r) => {
                            // TODO: evmap will remove the empty vec for a key if we remove the
                            // last record. this means that future lookups will fail, and cause a
                            // replay, which will produce an empty result. this will work, but is
                            // somewhat inefficient.
                            memory_delta -= r.deep_size_of() as isize;
                            h.remove(r[key[0]].clone(), id);
                        }
                    }
                }
            }
            Handle::DoubleSR(ref mut h) => {
                assert_eq!(key.len(), 2);
                for r in rs {
                    debug_assert!(r.len() >= cols);
                    match r {
                        Record::Positive(r) => {
                            memory_delta += r.deep_size_of() as isize;
                            h.insert((r[key[0]].clone(), r[key[1]].clone()), r, id);
                        }
                        Record::Negative(r) => {
                            memory_delta -= r.deep_size_of() as isize;
                            h.remove((r[key[0]].clone(), r[key[1]].clone()), id);
                        }
                    }
                }
            }
            Handle::ManySR(ref mut h) => for r in rs {
                debug_assert!(r.len() >= cols);
                let key = key.iter().map(|&k| &r[k]).cloned().collect();
                match r {
                    Record::Positive(r) => {
                        memory_delta += r.deep_size_of() as isize;
                        h.insert(key, r, id);
                    }
                    Record::Negative(r) => {
                        memory_delta -= r.deep_size_of() as isize;
                        h.remove(key, id);
                    }
                }
            }
        }
        memory_delta
    }
}
