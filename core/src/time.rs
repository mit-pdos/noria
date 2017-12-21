use std::cmp::{Ordering, PartialOrd};
use std::ops::{Index, IndexMut};

static ZERO_TIME: Time = Time(0);

pub type VectorTimeComponent = usize;

#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Time(u64);
impl Time {
    pub fn zero() -> Time {
        Time(0)
    }
}

/// A `VectorTime` represents the time at some number of base nodes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorTime(Vec<Time>);
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
impl PartialOrd for VectorTime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
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
