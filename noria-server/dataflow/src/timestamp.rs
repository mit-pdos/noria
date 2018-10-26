
use prelude::*;

#[derive(Copy, Clone, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Time(u64);

#[derive(Copy, Clone, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct TimestampAssigner(u64);
impl TimestampAssigner {
    pub fn assign(&mut self) -> Time {
        self.0 += 1;
        Time(self.0 - 1)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Path(u64);

#[derive(Copy, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PathAssigner(u64);
impl PathAssigner {
    pub fn assign(&mut self) -> Path {
        self.0 += 1;
        Path(self.0 - 1)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TimeComponent {
    pub path: Path,
    pub time: Time,
}

/// Maps incoming paths at a node to outgoing paths at a node.
#[derive(Clone, Serialize, Deserialize)]
pub struct PathMap(Option<Vec<Vec<Path>>>);
impl PathMap {
    pub fn lookup(&self, ancestor: usize, incoming: Path) -> Path {
        match self.0 {
            Some(ref v) => v[ancestor][incoming.0 as usize],
            None => incoming,
        }
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct VectorTime {
    components: Vec<Time>,
}
impl VectorTime {
    /// Create a new empty `VectorTime`
    pub fn new() -> Self {
        Self { components: Vec::new() }
    }

    /// Add an additional component to the `VectorTime`.
    pub fn extend(&mut self, component: TimeComponent) {
        let index = component.path.0 as usize;
        assert_eq!(self.components.len() + 1, index);
        self.components.push(component.time);
    }

    /// Advance a single time component.
    pub fn advance(&mut self, component: TimeComponent) {
        let index = component.path.0 as usize;
        assert_eq!(self.components[index].0 + 1, component.time.0);
        self.components[index] = component.time;
    }
}

/// Encapsulates all time related state for a view.
pub struct ViewTimeState {
    time: VectorTime,
    paths: PathMap,
    base_paths: Vec<((NodeIndex, usize), Vec<Path>)>
}
impl ViewTimeState {
    /// Checks whether this view may contain split updates.
    pub fn is_consistent(&self) -> bool {
        for (_, ref paths) in &self.base_paths {
            let time = self.time.components[paths[0].0 as usize];
            for i in 1..paths.len() {
                if self.time.components[paths[i].0 as usize] != time {
                    return false;
                }
            }
        }
        return true;
    }

    /// Updates the internal state, and returns a new time component that should be emitted by this
    /// view.
    pub fn process_update(&mut self, ancestor: usize, time_component: TimeComponent) -> TimeComponent {
        self.time.advance(time_component);
        TimeComponent {
            path: self.paths.lookup(ancestor, time_component.path),
            ..time_component
        }
    }
}
