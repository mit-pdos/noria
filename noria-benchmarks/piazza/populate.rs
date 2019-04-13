use noria::DataType;
use rand;
use rand::Rng;
use std::collections::HashMap;

const CLASSES_PER_STUDENT: usize = 10;
pub const TAS_PER_CLASS: usize = 1;

pub struct Populate {
    nposts: i32,
    nusers: i32,
    nclasses: i32,
    private: f32,
    rng: rand::ThreadRng,
    students: HashMap<DataType, Vec<DataType>>,
    tas: HashMap<DataType, Vec<DataType>>,
    pub classes: HashMap<DataType, i32>, // class ID to num posts in class mapping
    pub authors: HashMap<DataType, i32>,
    pub largest: i32,
}

impl Populate {
    pub fn new(nposts: i32, nusers: i32, nclasses: i32, private: f32) -> Populate {
        Populate {
            nposts,
            nusers,
            nclasses,
            private,
            rng: rand::thread_rng(),
            students: HashMap::new(),
            tas: HashMap::new(),
            classes: HashMap::new(),
            authors: HashMap::new(),
            largest: 0,
        }
    }

    pub fn get_enrollment(&mut self) -> HashMap<DataType, Vec<DataType>> {
        self.students.clone()
    }

    pub fn classes(&mut self) -> HashMap<DataType, i32> {
        self.classes.clone()
    }

    pub fn authors(&mut self) -> HashMap<DataType, i32> {
        self.authors.clone()
    }

    pub fn enroll_students(&mut self, classes_per_student: i32) {
        // println!("Enrolling students...");
        for i in 0..self.nusers {
            let mut classes: Vec<DataType> = Vec::new();
            while classes.len() < (classes_per_student as usize)
                && (classes.len() as i32 != self.nclasses)
            {
                let cid = self.cid();
                if !classes.contains(&cid) {
                    classes.push(cid.clone());
                }
            }

            self.students.insert(i.into(), classes);
        }

        // println!("Enrolling TAs...");
        for i in 0..self.nclasses {
            let mut tas: Vec<DataType> = Vec::new();

            while tas.len() < TAS_PER_CLASS && (tas.len() as i32 != self.nusers) {
                let uid = self.uid();
                if !tas.contains(&uid) {
                    tas.push(uid.clone());
                }
            }
            self.tas.insert(i.into(), tas);
        }
    }

    pub fn get_roles(&mut self) -> Vec<Vec<DataType>> {
        // println!("Populating roles...");
        let mut records = Vec::new();
        // add tas
        // we populate in this order so that each batch of writes
        // as many cids as possible. this is important for the
        // trigger node, since it creates one thread per batch
        // if the batch has a new id. having the same cid in one
        // batch would mean we create a new thread for each cid,
        // instead of one thread per batch of cid.
        for i in 0..TAS_PER_CLASS {
            for (cid, tas) in self.tas.iter() {
                let uid = tas[i].clone();
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

        records
    }

    pub fn get_users(&mut self) -> Vec<Vec<DataType>> {
        let mut records = Vec::new();
        for i in 1..self.nusers + 1 {
            let uid = i.into();
            records.push(vec![uid]);
        }

        records
    }

    pub fn get_posts(&mut self) -> Vec<Vec<DataType>> {
        println!("Populating posts... with {:?}", self.nposts);
        let mut records = Vec::new();
        for i in 0..self.nposts {
            let pid = self.largest.into();
            let author = self.uid();
            match self.authors.get_mut(&author.clone()) {
                Some(count) => *count += 1,
                None => {
                    self.authors.insert(author.clone(), 1);
                }
            }
            let cid = self.cid_for(&author);
            match self.classes.get_mut(&cid.clone()) {
                Some(count) => *count += 1,
                None => println!("cid {:?} nonexistent?!", cid),
            }
            let content = format!("post #{}", i).into();
            let private = self.private();
            let anon = 1.into();
            records.push(vec![pid, cid, author, content, private, anon]);
        }

        records
    }

    pub fn get_user_posts(
        &mut self,
        uid: i32,
        cid: DataType,
        nposts: i32,
    ) -> (Vec<Vec<DataType>>, i32, i32) {
        println!("Populating posts... with {:?}", nposts);
        let mut num_priv = 0;
        let mut num_pub = 0;
        let mut records = Vec::new();
        for i in 0..nposts {
            let pid = self.largest.into();
            let author: DataType = uid.into();
            match self.authors.get_mut(&author.clone()) {
                Some(count) => *count += 1,
                None => {
                    self.authors.insert(author.clone(), 1);
                }
            }
            match self.classes.get_mut(&cid.clone()) {
                Some(count) => *count += 1,
                None => println!("cid {:?} nonexistent?!", cid),
            }
            let content = format!("post #{}", i).into();
            let private = self.private();
            if private == 0.into() {
                num_pub += 1;
            } else {
                num_priv += 1;
            }
            let anon = 1.into();
            records.push(vec![
                pid,
                cid.clone(),
                author.clone(),
                content,
                private,
                anon,
            ]);
            self.largest += 1;
        }
        println!(
            "populated for user {:?}. priv: {:?}, pub: {:?}",
            uid, num_priv, num_pub
        );
        (records, num_priv, num_pub)
    }

    pub fn classes_for_student(&mut self, uid: usize) -> Vec<DataType> {
        let res: DataType = uid.clone().into();
        self.students[&res].clone()
    }

    pub fn get_classes(&mut self) -> Vec<Vec<DataType>> {
        println!("Populating classes...");
        let mut records = Vec::new();
        for i in 0..self.nclasses {
            let cid: DataType = i.into();
            self.classes.insert(cid.clone(), 0);
            records.push(vec![cid]);
        }
        records
    }

    pub fn nclasses(&self) -> i32 {
        self.nclasses
    }

    pub fn get_posts_per_class(&mut self, cid: DataType) -> i32 {
        self.classes[&cid].clone()
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
