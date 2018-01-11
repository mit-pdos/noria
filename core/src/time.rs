use std::borrow::Cow;
use std::cmp::{Ordering, PartialOrd};
use std::ops::{Index, IndexMut};

static ZERO_TIME: Time = Time(0);

/// Uniquely identifies an entity that can generate `VectorTime`s (like a base node shard). It maps
/// to a single dimension in a vector time.
pub type TimeSource = usize;

#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Time(u64);
impl Time {
    pub fn zero() -> Time {
        Time(0)
    }
    pub fn next(&self) -> Time {
        Time(self.0 + 1)
    }
    pub fn difference(&self, other: Time) -> u64 {
        assert!(other <= *self);
        self.0 - other.0
    }
    pub fn increment(&mut self) {
        self.0 += 1;
    }
}

/// A `VectorTime` represents the time at some number of base nodes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorTime(Vec<Time>);
impl VectorTime {
    pub fn zero() -> VectorTime {
        VectorTime(Vec::new())
    }
    pub fn new(t: Time, source: TimeSource) -> VectorTime {
        let mut v = vec![Time::zero(); source + 1];
        v[source] = t;
        VectorTime(v)
    }
    pub fn supremum(&self, other: Cow<VectorTime>) -> VectorTime {
        let mut other = other.into_owned();
        for i in 0..other.0.len() {
            other.0[i] = other[i].max(self[i]);
        }
        for i in other.0.len()..(self.0.len()) {
            other.0.push(self[i]);
        }
        other
    }
    /// Set this VectorTime to be the supremum of itself and other.
    pub fn advance_to(&mut self, other: &VectorTime) {
        for i in 0..self.0.len() {
            self[i] = self[i].max(other[i]);
        }
        for i in self.0.len()..(other.0.len()) {
            self[i] = other[i];
        }
    }
}
impl Index<usize> for VectorTime {
    type Output = Time;
    fn index(&self, index: usize) -> &Self::Output {
        if index < self.0.len() {
            &self.0[index]
        } else {
            &ZERO_TIME
        }
    }
}
impl IndexMut<usize> for VectorTime {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        if self.0.len() <= index {
            self.0.resize(index, Time::zero());
        }
        &mut self.0[index]
    }
}
impl<T: AsRef<VectorTime>> PartialEq<T> for VectorTime {
    fn eq(&self, other: &T) -> bool {
        let other = other.as_ref();
        for i in 0..(self.0.len().max(other.0.len())) {
            if self[i] != other[i] {
                return false;
            }
        }
        true
    }
}
impl Eq for VectorTime {}
impl<T: AsRef<VectorTime>> PartialOrd<T> for VectorTime {
    fn partial_cmp(&self, other: &T) -> Option<Ordering> {
        let other = other.as_ref();
        let mut less = false;
        let mut greater = false;
        for i in 0..(self.0.len().max(other.0.len())) {
            match self[i].cmp(&other[i]) {
                Ordering::Less => less = true,
                Ordering::Greater => greater = true,
                Ordering::Equal => {}
            }

            if less && greater {
                return None;
            }
        }

        match (less, greater) {
            (false, false) => Some(Ordering::Equal),
            (true, false) => Some(Ordering::Less),
            (false, true) => Some(Ordering::Greater),
            (true, true) => unreachable!(),
        }
    }
}
impl AsRef<VectorTime> for VectorTime {
    fn as_ref(&self) -> &Self {
        self
    }
}
