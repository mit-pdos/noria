use Backend;
use distributary::DataType;

pub fn create_users(backend: &mut Backend) {
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
    ];
    let users: Vec<Vec<DataType>> = data.into_iter()
        .map(|v| v.into_iter().map(|e| e.into()).collect::<Vec<DataType>>())
        .collect();

    let ins = backend.g.inputs();
    let mut mutator = backend.g.get_mutator(ins["UserProfile"]).unwrap();

    mutator.multi_put(users).unwrap();
}

pub fn create_papers(backend: &mut Backend) {
    // Paper
    // id int,
    // author varchar(1024),
    // accepted tinyint(1),
    let papers: Vec<Vec<&str>> = vec![
        vec!["1", "malte", "0"],
        vec!["2", "lara", "0"],
        vec!["3", "malte", "0"],
    ];

    // PaperVersion
    // paper int,
    // title varchar(1024),
    // contents varchar(1024),
    // abstract text,
    // time datetime DEFAULT CURRENT_TIMESTAMP,
    let paper_versions: Vec<Vec<&str>> = vec![
        vec![
            "1",
            "Why Soup is Awesome",
            "Text",
            "Soup is tasty.",
            "0",
            "0",
        ],
        vec!["2", "Is Soup Tasty?", "Text", "Maybe.", "0", "0"],
        vec!["3", "How To Cook Soup", "Text", "Make it tasty.", "0", "0"],
    ];

    let ins = backend.g.inputs();

    let papers: Vec<Vec<DataType>> = papers
        .into_iter()
        .map(|v| v.into_iter().map(|e| e.into()).collect::<Vec<DataType>>())
        .collect();
    let mut mutator = backend.g.get_mutator(ins["Paper"]).unwrap();
    mutator.multi_put(papers).unwrap();

    let paper_versions: Vec<Vec<DataType>> = paper_versions
        .into_iter()
        .map(|v| v.into_iter().map(|e| e.into()).collect::<Vec<DataType>>())
        .collect();
    let mut mutator = backend.g.get_mutator(ins["PaperVersion"]).unwrap();
    mutator.multi_put(paper_versions).unwrap();
}

pub fn dump_papers(backend: &mut Backend, user: &str) {
    let outs = backend.g.outputs();

    let mut get = backend
        .g
        .get_getter(outs[&format!("PaperList_u{}", user)])
        .unwrap();

    for i in 1..4 {
        println!("{:?}", get.lookup(&format!("{}", i).into(), true));
    }
}
