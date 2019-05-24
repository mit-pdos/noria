extern crate noria;

use noria::SyncControllerHandle;
use std::collections::BTreeMap;
use chrono::{NaiveDateTime, Utc};
use std::fs;
use std::vec::Vec;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use rand::Rng;

struct Votes {
    vote_id: i32,
    story_id: i32,
    comment_id: i32,
    vote: bool,
    reason: String,
}

struct Story {
    story_id: i32,
    user_id: i32,
    title: String,
    short_id: String,
}

struct User {
    user_id: i32,
    username: String,
    email: String,
}

impl User {
    fn new_user(username: String) -> User {
        let mut rng = rand::thread_rng();
        User {
            user_id: rng.gen::<i32>(),
            username: username.clone(),
            email: username.clone(),
        }
    }

    fn to_vec(&self) -> Vec<noria::DataType> {
        vec![self.user_id.into(), self.username.clone().into(), self.email.clone().into()]
    }
}

struct Comment {
    comment_id: i32,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    short_id: String,
    story_id: i32,
    user_id: i32,
    parent_comment_id: i32,
    comment: String,
    upvotes: i32,
    confidence: f64,
    markeddown_comment: String,
}

impl Comment {
    fn new_rand_comment(user_id: i32, story_id: i32, parent_comment_id: i32) -> Comment {
        let mut rng = rand::thread_rng();
        Comment {
            comment_id: rng.gen::<i32>(),
            created_at: Utc::now().naive_utc(),
            updated_at: Utc::now().naive_utc(),
            short_id: "Short id".to_string(),
            story_id: story_id,
            user_id: user_id,
            upvotes: 0,
            confidence: 0f64,
            markeddown_comment: "Test comment".to_string(),
            comment: "Test comment".to_string(),
            parent_comment_id: parent_comment_id,
        }
    }

    fn to_vec(&self) -> Vec<noria::DataType> {
        vec![self.comment_id.into(), self.created_at.into(), self.updated_at.into(),
        self.short_id.clone().into(), self.story_id.into(), self.user_id.into(),
        self.parent_comment_id.into(), self.upvotes.into(), self.confidence.into(),
        self.markeddown_comment.clone().into(), self.comment.clone().into()]
    }
}

fn main() {

    let mut s:String = fs::read_to_string("noria/examples/soupy-lobsters-schema.txt").unwrap();
    let lines: Vec<String> = s
        .lines()
        .filter(|l| {
            !l.is_empty()
                && !l.starts_with("--")
                && !l.starts_with('#')
                && !l.starts_with("DROP TABLE")
        })
        .map(|l| {
            if !(l.ends_with('\n') || l.ends_with(';')) {
                String::from(l) + "\n"
            } else {
                String::from(l)
            }
        })
        .collect();

    println!("Get contrller handle");
    let rt = tokio::runtime::Runtime::new().unwrap();
    let executor = rt.executor();
    let mut db = SyncControllerHandle::from_zk("127.0.0.1:2181/lobster", executor).unwrap();
    println!("Got Controller adding sql recipes");
    for (_i, q) in lines.iter().enumerate() {
        // println!("{:?}", q);
        // db.extend_recipe(q).unwrap();
    }
    println!("Graph:\n {}", db.graphviz().unwrap());

    let executor = rt.executor();
    let get_view = move |b: &mut SyncControllerHandle<_, _>, n| loop {
        match b.view(n) {
            Ok(v) => return v.into_sync(),
            Err(err) => {
                println!("{:?}", err);
                thread::sleep(Duration::from_millis(50));
                *b = SyncControllerHandle::from_zk("127.0.0.1:2181/lobster", executor.clone())
                    .unwrap();
            }
        }
    };

    let executor = rt.executor();
    let get_table = move |b: &mut SyncControllerHandle<_, _>, n| loop {
        match b.table(n) {
            Ok(v) => return v.into_sync(),
            Err(_) => {
                thread::sleep(Duration::from_millis(50));
                *b = SyncControllerHandle::from_zk("127.0.0.1:2181/lobster", executor.clone())
                    .unwrap();
            }
        }
    };
    panic!();

    println!("Getting comments table");
    let mut comments = get_table(&mut db, "comments");
    let first_comment = Comment::new_rand_comment(1, 1, 0);
    let comment_id = 1;
    println!("First Comment {:?}",first_comment.to_vec());
    comments.insert(first_comment.to_vec()).unwrap();
    // comments.delete(vec![first_comment.comment_id.into()]);

    // let mut users = get_table(&mut db, "users");
    // let first_user = User::new_user("test_user".to_string());
    // println!("First user {:?}",first_user.to_vec());
    // users.insert(first_user.to_vec()).unwrap();
    // println!("Inserted");

    // let mut username_view =get_view(&mut db, "q_5");
    // let user = username_view.lookup(&vec![comment_id.into()], false).unwrap();
    // println!("User: {:?}", user);

    let mut comments_by_shortid = get_view(&mut db, "q_35");
    println!("Getting first comment");
    let comment = comments_by_shortid.lookup(&vec![first_comment.short_id.into()], false).unwrap();
    println!("commment: {:?}", comment);
}
