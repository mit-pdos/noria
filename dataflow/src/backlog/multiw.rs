use super::{key_to_double, key_to_single, Key};
use basics::data::SizeOf;
use basics::{DataType, Record};
use srmap;
use std::collections::HashMap;
use fnv::FnvBuildHasher;
use evmap;

#[derive(Clone)]
pub(super) enum Handle {
    SingleSR(srmap::srmap::WriteHandle<DataType, Vec<DataType>, i64>),
    DoubleSR(srmap::srmap::WriteHandle<(DataType, DataType), Vec<DataType>, i64>),
    ManySR(srmap::srmap::WriteHandle<Vec<DataType>, Vec<DataType>, i64>),
    // Single(evmap::WriteHandle<DataType, Vec<DataType>, i64, FnvBuildHasher>),
    // Double(evmap::WriteHandle<(DataType, DataType), Vec<DataType>, i64, FnvBuildHasher>),
    // Many(evmap::WriteHandle<Vec<DataType>, Vec<DataType>, i64, FnvBuildHasher>),
}

impl Handle {

    pub fn is_empty(&self) -> bool {
        match *self {
            // Handle::Single(ref h) => h.is_empty(),
            // Handle::Double(ref h) => h.is_empty(),
            // Handle::Many(ref h) => h.is_empty(),
            Handle::SingleSR(ref h) => h.is_empty(),
            Handle::DoubleSR(ref h) => h.is_empty(),
            Handle::ManySR(ref h) => h.is_empty(),
        }
    }

    pub fn add_user(&mut self, uid: usize) {
        match *self {
            Handle::SingleSR(ref mut h) => h.add_user(uid),
            Handle::DoubleSR(ref mut h) => h.add_user(uid),
            Handle::ManySR(ref mut h) => h.add_user(uid),
            _ => {}
        }
    }

    pub fn clear(&mut self, k: Key, uid: usize) {
        match *self {
            // Handle::Single(ref mut h) => {
            //                                         h.clear(key_to_single(k).into_owned())},
            Handle::SingleSR(ref mut h) => {h.clear(key_to_single(k).into_owned(), uid.clone())},
            // Handle::Double(ref mut h) => {
            //                                         h.clear(key_to_double(k).into_owned())},
            Handle::DoubleSR(ref mut h) => {h.clear(key_to_double(k).into_owned(), uid.clone())},
            // Handle::Many(ref mut h) => {
            //                                         h.clear(k.into_owned())},
            Handle::ManySR(ref mut h) => {h.clear(k.into_owned(), uid.clone())},
        }
    }

    pub fn empty(&mut self, k: Key, uid: usize) {
        match *self {
            // Handle::Single(ref mut h) => {
            //                                         h.empty(key_to_single(k).into_owned())},
            Handle::SingleSR(ref mut h) => {h.empty(key_to_single(k).into_owned(), uid.clone())},
            // Handle::Double(ref mut h) => {
            //                                         h.empty(key_to_double(k).into_owned())},
            Handle::DoubleSR(ref mut h) => {h.empty(key_to_double(k).into_owned(), uid.clone())},
            // Handle::Many(ref mut h) => {
            //                                         h.empty(k.into_owned())},
            Handle::ManySR(ref mut h) => {h.empty(k.into_owned(), uid.clone())},
        }
    }

    /// Evict `count` randomly selected keys from state and return them along with the number of
    /// bytes freed.
    pub fn empty_at_index(&mut self, index: usize) -> Option<&Vec<Vec<DataType>>> {
        match *self {
            // Handle::Single(ref mut h) => h.empty_at_index(index).map(|r| r.1),
            Handle::SingleSR(ref mut h) => unimplemented!(),
            // Handle::Double(ref mut h) => h.empty_at_index(index).map(|r| r.1),
            Handle::DoubleSR(ref mut h) => unimplemented!(),
            // Handle::Many(ref mut h) => h.empty_at_index(index).map(|r| r.1),
            Handle::ManySR(ref mut h) => unimplemented!(),
        }
    }

    pub fn refresh(&mut self) {
        match *self {
            // Handle::Single(ref mut h) => h.refresh(),
            // Handle::Double(ref mut h) => h.refresh(),
            // Handle::Many(ref mut h) => h.refresh(),
            _ => (),
        }
    }

    pub fn meta_get_and<F, T>(&self, key: Key, then: F, uid: usize) -> Option<(Option<T>, i64)>
    where
        F: FnOnce(&[Vec<DataType>]) -> T,
    {
        match *self {
            // Handle::Single(ref h) => {
            //     assert_eq!(key.len(), 1);
            //     h.meta_get_and(&key[0], then)
            // },
            Handle::SingleSR(ref h) => {
                assert_eq!(key.len(), 1);
                h.meta_get_and(&key[0], then, uid.clone())
            },
            // Handle::Double(ref h) => {
            //     assert_eq!(key.len(), 2);
            //     // we want to transmute &[T; 2] to &(T, T), but that's not actually safe
            //     // we're not guaranteed that they have the same memory layout
            //     // we *could* just clone DataType, but that would mean dealing with string refcounts
            //     // so instead, we play a trick where we memcopy onto the stack and then forget!
            //     //
            //     // h/t https://gist.github.com/mitsuhiko/f6478a0dd1ef174b33c63d905babc89a
            //     use std::mem;
            //     use std::ptr;
            //     unsafe {
            //         let mut stack_key: (DataType, DataType) = mem::uninitialized();
            //         ptr::copy_nonoverlapping(
            //             &key[0] as *const DataType,
            //             &mut stack_key.0 as *mut DataType,
            //             1,
            //         );
            //         ptr::copy_nonoverlapping(
            //             &key[1] as *const DataType,
            //             &mut stack_key.1 as *mut DataType,
            //             1,
            //         );
            //         let v = h.meta_get_and(&stack_key, then);
            //         mem::forget(stack_key);
            //         v
            //     }
            // },
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
                    let v = h.meta_get_and(&stack_key, then, uid.clone());
                    mem::forget(stack_key);
                    v
                }
            },
            // Handle::Many(ref h) => {
            //     h.meta_get_and(&key.to_vec(), then)
            // },
            Handle::ManySR(ref h) => {
                h.meta_get_and(&key.to_vec(), then, uid.clone())
            },
        }
    }


    pub fn add<I>(&mut self, key: &[usize], cols: usize, rs: I, uid: usize) -> isize
    where
        I: IntoIterator<Item = Record>,
    {
        let mut memory_delta = 0isize;
        match *self {
            // Handle::Single(ref mut h) => {
            //     assert_eq!(key.len(), 1);
            //     for r in rs {
            //         debug_assert!(r.len() >= cols);
            //         match r {
            //             Record::Positive(r) => {
            //                 memory_delta += r.deep_size_of() as isize;
            //                 h.insert(r[key[0]].clone(), r);
            //             }
            //             Record::Negative(r) => {
            //                 // TODO: evmap will remove the empty vec for a key if we remove the
            //                 // last record. this means that future lookups will fail, and cause a
            //                 // replay, which will produce an empty result. this will work, but is
            //                 // somewhat inefficient.
            //                 memory_delta -= r.deep_size_of() as isize;
            //                 h.remove(r[key[0]].clone(), r);
            //             }
            //         }
            //     }
            // }
            Handle::SingleSR(ref mut h) => {
                assert_eq!(key.len(), 1);
                for r in rs {
                    debug_assert!(r.len() >= cols);
                    match r {
                        Record::Positive(r) => {
                            memory_delta += r.deep_size_of() as isize;
                            println!("*** MULTIW: insert: KEY: {:?}, R: {:?}, UID: {:?}", r[key[0]].clone(), r.clone(), uid.clone());
                            h.insert(r[key[0]].clone(), r, uid.clone());
                        }
                        Record::Negative(r) => {
                            // TODO: evmap will remove the empty vec for a key if we remove the
                            // last record. this means that future lookups will fail, and cause a
                            // replay, which will produce an empty result. this will work, but is
                            // somewhat inefficient.
                            memory_delta -= r.deep_size_of() as isize;
                            h.remove(r[key[0]].clone(), uid.clone());
                        }
                    }
                }
            }
            // Handle::Double(ref mut h) => {
            //     assert_eq!(key.len(), 2);
            //     for r in rs {
            //         debug_assert!(r.len() >= cols);
            //         match r {
            //             Record::Positive(r) => {
            //                 memory_delta += r.deep_size_of() as isize;
            //                 h.insert((r[key[0]].clone(), r[key[1]].clone()), r);
            //             }
            //             Record::Negative(r) => {
            //                 memory_delta -= r.deep_size_of() as isize;
            //                 h.remove((r[key[0]].clone(), r[key[1]].clone()), r);
            //             }
            //         }
            //     }
            // }
            Handle::DoubleSR(ref mut h) => {
                assert_eq!(key.len(), 2);
                for r in rs {
                    debug_assert!(r.len() >= cols);
                    match r {
                        Record::Positive(r) => {
                            memory_delta += r.deep_size_of() as isize;
                            h.insert((r[key[0]].clone(), r[key[1]].clone()), r, uid.clone());
                        }
                        Record::Negative(r) => {
                            memory_delta -= r.deep_size_of() as isize;
                            h.remove((r[key[0]].clone(), r[key[1]].clone()), uid.clone());
                        }
                    }
                }
            }
            // Handle::Many(ref mut h) => for r in rs {
            //     debug_assert!(r.len() >= cols);
            //     let key = key.iter().map(|&k| &r[k]).cloned().collect();
            //     match r {
            //         Record::Positive(r) => {
            //             memory_delta += r.deep_size_of() as isize;
            //             h.insert(key, r);
            //         }
            //         Record::Negative(r) => {
            //             memory_delta -= r.deep_size_of() as isize;
            //             h.remove(key, r);
            //         }
            //     }
            // },
            Handle::ManySR(ref mut h) => for r in rs {
                debug_assert!(r.len() >= cols);
                let key = key.iter().map(|&k| &r[k]).cloned().collect();
                match r {
                    Record::Positive(r) => {
                        memory_delta += r.deep_size_of() as isize;
                        h.insert(key, r, uid.clone());
                    }
                    Record::Negative(r) => {
                        memory_delta -= r.deep_size_of() as isize;
                        h.remove(key, uid.clone());
                    }
                }
            }
        }
        memory_delta
    }
}
