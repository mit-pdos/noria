use crate::Backend;
use noria::DataType;

pub fn create_users(backend: &mut Backend) {
    println!("creating users");
    // username varchar(1024),
    // email varchar(1024),
    // name varchar(1024),
    // affiliation varchar(1024),
    // acm_number varchar(1024),
    // level varchar(12): one of "chair", "pc", "normal"
    let data: Vec<Vec<&str>> = vec![
        vec![
            "kohler",
            "kohler@seas.harvard.edu",
            "Eddie Kohler",
            "Harvard University",
            "0",
            "chair",
        ],
        vec![
            "rtm",
            "rtm@csail.mit.edu",
            "Robert Morris",
            "MIT CSAIL",
            "0",
            "pc",
        ],
        vec![
            "malte",
            "malte@csail.mit.edu",
            "Malte Schwarzkopf",
            "MIT CSAIL",
            "0",
            "normal",
        ],
        vec![
            "lara",
            "larat@mit.edu",
            "Lara Timbo",
            "MIT CSAIL",
            "0",
            "normal",
        ],

        vec![
            "2",
            "2@mit.edu",
            "2",
            "org",
            "0",
            "normal",
        ],
        vec![
            "3",
            "3@mit.edu",
            "3",
            "org",
            "0",
            "chair",
        ],
    ];
    let users: Vec<Vec<DataType>> = data
        .into_iter()
        .map(|v| v.into_iter().map(|e| e.into()).collect::<Vec<DataType>>())
        .collect();

    let mut mutator = backend.g.table("UserProfile").unwrap();
    println!("inserting users");
    mutator.insert_all(users).unwrap();
    println!("inserting users2");
}

pub fn create_papers(backend: &mut Backend) {
    // Paper
    // id int,
    // author varchar(1024),
    // accepted tinyint(1),
    let papers: Vec<Vec<DataType>> = vec![
        vec![1.into(), "malte".into(), "0".into()],
        vec![2.into(), "lara".into(), "0".into()],
        vec![3.into(), "malte".into(), "0".into()],
        vec![4.into(), "2".into(), "0".into()],
        vec![5.into(), "2".into(), "0".into()],
    ];

    // PaperVersion
    // paper int,
    // title varchar(1024),
    // contents varchar(1024),
    // abstract text,
    // time datetime DEFAULT CURRENT_TIMESTAMP,
    let paper_versions: Vec<Vec<DataType>> = vec![
        vec![
            1.into(),
            "Why Soup is Awesome".into(),
            "Text".into(),
            "Soup is tasty.".into(),
            "0".into(),
        ],
        vec![
            2.into(),
            "Is Soup Tasty?".into(),
            "Text".into(),
            "Maybe.".into(),
            "0".into(),
        ],
        vec![
            3.into(),
            "How To Cook Soup".into(),
            "Text".into(),
            "Make it tasty.".into(),
            "0".into(),
        ],
        vec![
            4.into(),
            "title".into(),
            "text".into(),
            "abstract".into(),
            "0".into(),
        ],
        vec![
            5.into(),
            "title2".into(),
            "text2".into(),
            "abstract2".into(),
            "0".into(),
        ],
    ];
    
    // PaperCoauthor
    // paper int,
    // author varchar(1024),
    let paper_coauthors: Vec<Vec<DataType>> = vec![
        vec![
            1.into(),
            "malte".into(),
        ],
        vec![
            1.into(),
            "2".into(),
        ],
        vec![
            2.into(),
            "lara".into(),
        ],
        vec![
            3.into(),
            "malte".into(),
        ],
        vec![
            4.into(),
            "2".into(),
        ],
        vec![
            5.into(),
            "2".into(),
        ],
    ];

    // Review table
    // time datetime DEFAULT CURRENT_TIMESTAMP, -- must be in provided data even though default spec'd.
    // paper int,
    // reviewer varchar(1024),
    // contents text,
    // score_novelty int,
    // score_presentation int,
    // score_technical int,
    // score_confidence int,
    
    let reviews: Vec<Vec<DataType>> = vec![
        vec![
            "0".into(),
            1.into(),
            "lara".into(),
            "great paper".into(),
            10.into(),
            10.into(),
            10.into(),
            10.into(),
        ],
        vec![
            "0".into(),
            2.into(),
            "2".into(),
            "great paper".into(),
            10.into(),
            10.into(),
            10.into(),
            10.into(),
        ],
        vec![
            "0".into(),
            3.into(),
            "2".into(),
            "great paper".into(),
            10.into(),
            10.into(),
            10.into(),
            10.into(),
        ],
        vec![
            "0".into(),
            4.into(),
            "malte".into(),
            "great paper".into(),
            10.into(),
            10.into(),
            10.into(),
            10.into(),
        ],
        vec![
            "0".into(),
            5.into(),
            "lara".into(),
            "great paper".into(),
            10.into(),
            10.into(),
            10.into(),
            10.into(),
        ],
    ];

    // ReviewAssignment table
    // paper int,
    // username varchar(1024),
    // assign_type varchar(8), -- What is assign_type??
    let review_assignments: Vec<Vec<DataType>> = vec![
        vec![
            1.into(),
            "lara".into(),
            "blahblah".into(),
        ],
        vec![
            2.into(),
            "2".into(),
            "blahblah".into(),
        ],
        vec![
            3.into(),
            "2".into(),
            "blahblah".into(),
        ],
        vec![
            4.into(),
            "malte".into(),
            "blahblah".into(),
        ],
        vec![
            5.into(),
            "lara".into(),
            "blahblah".into(),
        ],
    ];
    
    let mut mutator = backend.g.table("Paper").unwrap();
    mutator.insert_all(papers).unwrap();

    let mut mutator = backend.g.table("PaperVersion").unwrap();
    mutator.insert_all(paper_versions).unwrap();

    let mut mutator = backend.g.table("PaperCoauthor").unwrap();
    mutator.insert_all(paper_coauthors).unwrap();

    let mut mutator = backend.g.table("Review").unwrap();
    mutator.insert_all(reviews).unwrap();

    let mut mutator = backend.g.table("ReviewAssignment").unwrap();
    mutator.insert_all(review_assignments).unwrap();

}

pub fn dump_papers(backend: &mut Backend, user: &str) {
    let mut get = backend.g.view(&format!("PaperList_u{}", user)).unwrap();

    println!("user's papers: {:?}", get.lookup(&[0.into()], true)); // 0 is bogo key for id
}

pub fn dump_reviews(backend: &mut Backend, user: &str) {
    let mut get = backend.g.view(&format!("ReviewList_u{}", user)).unwrap();

    println!("user's reviews: {:?}", get.lookup(&[0.into()], true)); // 0 is bogo key for id
}

pub fn dump_all_papers(backend: &mut Backend) {
    let mut get = backend.g.view("PaperList").unwrap();

    println!("all papers: {:?}", get.lookup(&[0.into()], true));
}

pub fn dump_context(backend: &mut Backend, query: &str, lookup_str: &str, lookup_int: i32, use_str: bool) {
    let mut get = backend.g.view(query).unwrap();
    if use_str {
        println!("{}: {:?}", query, get.lookup(&[lookup_str.into()], true));
    } else {
        println!("{}: {:?}", query, get.lookup(&[lookup_int.into()], true));
    }
}
