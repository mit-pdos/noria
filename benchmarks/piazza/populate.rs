use std::time;
use std::collections::HashMap;
use distributary::DataType;
use rand;
use rand::Rng;
use super::Backend;

pub const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_fsec {
    ($d:expr) => {{
        let d = $d;
        (d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64) as f64 / NANOS_PER_SEC as f64
    }}
}

const STUDENTS_PER_CLASS: usize = 50;
const TAS_PER_CLASS: usize = STUDENTS_PER_CLASS / 10;

pub struct Populate {
    nposts: i32,
    nusers: i32,
    nclasses: i32,
    private: f32,
    rng: rand::ThreadRng,
    students: HashMap<DataType, Vec<DataType>>,
    tas: HashMap<DataType, Vec<DataType>>,
}


impl Populate {
    pub fn new(nposts: i32, nusers: i32, nclasses: i32, private: f32) -> Populate {
        Populate {
            nposts: nposts,
            nusers: nusers,
            nclasses: nclasses,
            private: private,
            rng: rand::thread_rng(),
            students: HashMap::new(),
            tas: HashMap::new(),
        }
    }

    pub fn populate_tables(&mut self, mut backend: &mut Backend) {
        self.enroll_students();
        self.populate_roles(&mut backend);
        self.populate_users(&mut backend);
        self.populate_posts(&mut backend);
        self.populate_classes(&mut backend);
    }

    fn enroll_students(&mut self) {
        for i in 0..self.nusers {
            let mut classes: Vec<DataType> = Vec::new();
            while classes.len() < STUDENTS_PER_CLASS {
                let cid = self.cid();
                if !classes.contains(&cid) {
                    classes.push(cid.clone());
                }
            }

            self.students.insert(i.into(), classes);
        }

        for i in 0..self.nclasses {
            let mut tas: Vec<DataType> = Vec::new();

            while tas.len() < TAS_PER_CLASS {
                let uid = self.uid();
                if !tas.contains(&uid) {
                    tas.push(uid.clone());
                }
            }

            self.tas.insert(i.into(), tas);
        }
    }

    fn populate(backend: &mut Backend, name: &'static str, mut records: Vec<Vec<DataType>>) -> usize {
        let ins = backend.g.inputs();
        let mut mutator = backend
            .g
            .get_mutator(ins[name])
            .unwrap();

        let start = time::Instant::now();

        let i = records.len();
        for r in records.drain(..) {
            mutator.put(r).unwrap();
        }

        let dur = dur_to_fsec!(start.elapsed());
        println!(
            "Inserted {} {} in {:.2}s ({:.2} PUTs/sec)!",
            i,
            name,
            dur,
            i as f64 / dur
        );

        i
    }

    fn populate_roles(&mut self, mut backend: &mut Backend) {
        println!("Populating roles...");
        let mut records = Vec::new();
        // add tas
        for (cid, tas) in self.tas.iter() {
            for ta in tas {
                let uid = ta.clone();
                let cid = cid.clone();
                let role = 1.into(); // ta
                records.push(vec![uid, cid, role]);
            }
        }

        // add students
        for (uid, classes) in self.students.iter() {
            for cid in classes {
                let uid = uid.clone();
                let cid = cid.clone();
                let role = 0.into(); // student
                records.push(vec![uid, cid, role]);
            }
        }

        Self::populate(&mut backend, "Role", records);
    }

    fn populate_users(&mut self, mut backend: &mut Backend) {
        println!("Populating users...");
        let mut records = Vec::new();
        for i in 0..self.nusers {
            let uid = i.into();
            records.push(vec![uid]);
        }

        Self::populate(&mut backend, "User", records);
    }

    fn populate_posts(&mut self, mut backend: &mut Backend) {
        println!("Populating posts...");
        let mut records = Vec::new();
        for i in 0..self.nposts {
            let pid = i.into();
            let author = self.uid();
            let cid = self.cid_for(&author);
            let content = "".into();
            let private = self.private();
            records.push(vec![pid, cid, author, content, private]);
        }

        Self::populate(&mut backend, "Post", records);
    }

    fn populate_classes(&mut self, mut backend: &mut Backend) {
        println!("Populating classes...");
        let mut records = Vec::new();
        for i in 0..self.nclasses {
            let cid = i.into();
            records.push(vec![cid]);
        }

        Self::populate(&mut backend, "Class", records);
    }

    /// Generate random uid within bounds
    fn uid(&mut self) -> DataType {
        self.rng.gen_range(0, self.nusers).into()
    }

    /// Generate random cid within bounds
    fn cid(&mut self) -> DataType {
        self.rng.gen_range(0, self.nclasses).into()
    }

    fn cid_for(&mut self, uid: &DataType) -> DataType {
        let classes = self.students[uid].as_slice();
        self.rng.choose(&classes).unwrap().clone()
    }

    #[allow(dead_code)]
    /// Generate random pid within bounds
    fn pid(&mut self) -> DataType {
        self.rng.gen_range(0, self.nposts).into()
    }

    fn private(&mut self) -> DataType {
        let m: f32 = self.rng.gen_range(0.0, 1.0);
        ((m < self.private) as i32).into()
    }

}