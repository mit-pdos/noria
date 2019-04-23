use crate::{Builder, SyncHandle};
use dataflow::{DurabilityMode, PersistenceParameters};
use noria::consensus::LocalAuthority;
use noria::SyncTable;
use noria::SyncView;

use std::sync::Arc;
use std::time::Duration;
use std::{env, thread};

const DEFAULT_SETTLE_TIME_MS: u64 = 200;
const DEFAULT_SHARDING: Option<usize> = Some(2);
type Worker = SyncHandle<LocalAuthority>;

// PersistenceParameters with a log_name on the form of `prefix` + timestamp,
// avoiding collisions between separate test runs (in case an earlier panic causes clean-up to
// fail).
fn get_persistence_params(prefix: &str) -> PersistenceParameters {
    let mut params = PersistenceParameters::default();
    params.mode = DurabilityMode::DeleteOnExit;
    params.log_prefix = String::from(prefix);
    params
}

#[allow(dead_code)]
pub fn start_simple_unsharded(prefix: &str) -> Worker {
    build(prefix, None, false)
}

#[allow(dead_code)]
pub fn start_simple_logging(prefix: &str) -> Worker {
    build(prefix, DEFAULT_SHARDING, true)
}

fn build(prefix: &str, sharding: Option<usize>, log: bool) -> Worker {
    use crate::logger_pls;
    let mut builder = Builder::default();
    if log {
        builder.log_with(logger_pls());
    }
    builder.set_sharding(sharding);
    builder.set_persistence(get_persistence_params(prefix));
    builder.start_simple().unwrap()
}

fn build_authority(
    prefix: &str,
    authority: Arc<LocalAuthority>,
    log: bool,
) -> Worker {
    use crate::logger_pls;
    let mut builder = Builder::default();
    if log {
        builder.log_with(logger_pls());
    }
    builder.set_sharding(None);
    builder.set_persistence(get_persistence_params(prefix));
    builder.start_simple_authority(authority).unwrap()
}

fn get_settle_time() -> Duration {
    let settle_time: u64 = match env::var("SETTLE_TIME") {
        Ok(value) => value.parse().unwrap(),
        Err(_) => DEFAULT_SETTLE_TIME_MS,
    };

    Duration::from_millis(settle_time)
}

// Sleeps for either DEFAULT_SETTLE_TIME_MS milliseconds, or
// for the value given through the SETTLE_TIME environment variable.
fn sleep() {
    thread::sleep(get_settle_time());
}

#[test]
fn aggregations_work_with_replicas() {
    let txt = "CREATE TABLE vote (user int, id int);\n
               QUERY votecount: SELECT id, COUNT(*) AS votes FROM vote WHERE id = ?;";

    let mut g = build("worker-1", None, false);
    g.install_recipe(txt).unwrap();
    assert_eq!(g.inputs().unwrap().len(), 1);
    assert_eq!(g.outputs().unwrap().len(), 1);

    let mut mutx = g.table("vote").unwrap().into_sync();
    let mut q = g.view("votecount").unwrap().into_sync();

    // identity node does not affect results
    let id = 0;
    let votes = 7;
    for _ in 0..votes {
        mutx.insert(vec![1337.into(), id.into()]).unwrap();
    }
    sleep();
    assert_eq!(q.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), votes.into()]]);
}

/*
fn stateless_domain(replay_after_recovery: bool) {
    let txt = "CREATE TABLE vote (user int, id int);\n
               QUERY user: SELECT id, user FROM vote WHERE id = ?;";

    // start a worker for each domain
    let authority = Arc::new(LocalAuthority::new());
    let mut g = build_authority("worker-0", authority.clone(), false);
    let g1 = build_authority("worker-1", authority.clone(), false);
    let _g2 = build_authority("worker-2", authority.clone(), false);
    sleep();

    g.install_recipe(txt).unwrap();
    sleep();
    println!("{}", g.graphviz().unwrap());

    let mut mutx = g.table("vote").unwrap().into_sync();
    let mut q = g.view("user").unwrap().into_sync();
    let id = 0;

    // prime the dataflow graph
    println!("check 1: write");
    mutx.insert(vec![1.into(), id.into()]).unwrap();
    sleep();
    if !replay_after_recovery {
        println!("check 2: lookup to seed replay pieces");
        assert_eq!(q.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), 1.into()]]);
    } else {
        println!("check 2: continue without seeding replay pieces");
    }

    // shutdown the worker with the middle node and write while it is still recovering
    // no writes are reflected because the dataflow graph is disconnected
    println!("check 3: drop middle node");
    drop(g1);
    thread::sleep(Duration::from_secs(3));
    println!("check 4: write before recovery");
    mutx.insert(vec![2.into(), id.into()]).unwrap();
    sleep();
    println!("check 5: lookup before recovery CAN'T HANDLE");
    // assert_eq!(q.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), 1.into()]]);

    // wait for recovery and observe both old and new writes
    println!("check 6: wait for recovery");
    thread::sleep(Duration::from_secs(10));
    println!("check 7: done! write after recovery");
    mutx.insert(vec![3.into(), id.into()]).unwrap();
    sleep();
    let expected = vec![
        vec![id.into(), 1.into()],
        vec![id.into(), 2.into()],
        vec![id.into(), 3.into()],
    ];
    println!("check 8: lookup after recovery");
    assert_eq!(q.lookup(&[id.into()], true).unwrap(), expected);
    println!("success! now clean shutdown...");
}

#[test]
fn lose_stateless_domain_without_replays() {
    stateless_domain(false);
}

#[test]
fn lose_stateless_domain_with_replays() {
    stateless_domain(true);
}
*/

fn multi_child_replica(replay_after_recovery: bool, worker_to_drop: usize) {
    let txt = "CREATE TABLE vote (user int, id int);\n
               QUERY q1: SELECT id, COUNT(*) AS votes FROM vote WHERE id = ?;\n
               QUERY q2: SELECT COUNT(*) AS votes FROM vote WHERE id = ?;";

    // start enough workers for each domain to be on its own worker
    let authority = Arc::new(LocalAuthority::new());
    let mut workers = vec![];
    for i in 0..7 {
        let name = format!("worker-{}", i);
        let worker = build_authority(&name, authority.clone(), false);
        workers.push(worker);
    }
    sleep();

    let g_dropped = workers.remove(worker_to_drop);
    let mut g = workers.remove(0);
    g.install_recipe(txt).unwrap();
    sleep();
    println!("{}", g.graphviz().unwrap());

    let mut mutx = g.table("vote").unwrap().into_sync();
    let mut q1 = g.view("q1").unwrap().into_sync();
    let mut q2 = g.view("q2").unwrap().into_sync();
    let id = 8;
    let row = vec![1337.into(), id.into()];

    // prime the dataflow graph
    println!("check 0: write");
    mutx.insert(row.clone()).unwrap();
    if !replay_after_recovery {
        println!("check 1: lookup q1 and q2 to send initial replay pieces");
        assert_eq!(q1.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), 1.into()]]);
        assert_eq!(q2.lookup(&[id.into()], true).unwrap(), vec![vec![1.into(), id.into()]]);
    } else {
        println!("check 1: continue without sending initial replay pieces");
    }

    // shutdown the bottom replica and write while it is still recovering
    // no writes are reflected because the dataflow graph is disconnected
    println!("check 4: drop the bottom replica");
    drop(g_dropped);
    thread::sleep(Duration::from_secs(3));
    println!("check 5: write before recovery");
    mutx.insert(row.clone()).unwrap();
    sleep();
    println!("check 6: lookup q1 before recovery CAN'T HANDLE");
    // assert_eq!(q1.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), 2.into()]]);
    println!("check 7: lookup q2 before recovery CAN'T HANDLE");
    // assert_eq!(q2.lookup(&[id.into()], true).unwrap(), vec![vec![2.into(), id.into()]]);

    // wait for recovery and observe both old and new writes
    println!("check 8: wait for recovery");
    thread::sleep(Duration::from_secs(10));
    println!("check 9: done! write after recovery");
    mutx.insert(row.clone()).unwrap();
    sleep();
    println!("check 10: lookup q1 after recovery");
    assert_eq!(q1.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), 3.into()]]);
    println!("check 11: lookup q2 after recovery");
    assert_eq!(q2.lookup(&[id.into()], true).unwrap(), vec![vec![3.into(), id.into()]]);

    // if the replays were just set up, perform one more write and lookup
    if replay_after_recovery {
        println!("check 12: write after replays setup");
        mutx.insert(row).unwrap();
        sleep();
        println!("check 13: lookup after replays setup");
        assert_eq!(q1.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), 4.into()]]);
        assert_eq!(q2.lookup(&[id.into()], true).unwrap(), vec![vec![4.into(), id.into()]]);
    }

    println!("success! now clean shutdown...");
}

#[test]
fn lose_bottom_multi_child_replica_without_replays() {
    multi_child_replica(false, 2);
}

#[test]
fn lose_bottom_multi_child_replica_with_replays() {
    multi_child_replica(true, 2);
}

#[test]
fn lose_top_multi_child_replica_without_replays() {
    multi_child_replica(false, 1);
}

#[test]
fn lose_top_multi_child_replica_with_replays() {
    multi_child_replica(true, 1);
}

fn stateless_multi_child_domain(replay_after_recovery: bool) {
    let txt = "CREATE TABLE data (id int, x int, y int);\n
               QUERY x: SELECT id, x FROM data WHERE x > 2019;\n
               QUERY y: SELECT id, y FROM data WHERE x > 2019;";

    // start enough workers for the multi-child filter node to be in its own domain
    let authority = Arc::new(LocalAuthority::new());
    let mut g = build_authority("worker-0", authority.clone(), false);
    let g1 = build_authority("worker-1", authority.clone(), false);
    let _g2 = build_authority("worker-2", authority.clone(), false);
    let _g3 = build_authority("worker-3", authority.clone(), false);
    let _g4 = build_authority("worker-4", authority.clone(), false);
    sleep();

    g.install_recipe(txt).unwrap();
    sleep();
    println!("{}", g.graphviz().unwrap());

    let mut mutx = g.table("data").unwrap().into_sync();
    let mut q1 = g.view("x").unwrap().into_sync();
    let mut q2 = g.view("y").unwrap().into_sync();
    let id = 7;
    let y = 100;

    // prime the dataflow graph
    println!("check 1: write");
    mutx.insert(vec![id.into(), 2020.into(), y.into()]).unwrap();
    sleep();
    if !replay_after_recovery {
        println!("check 2: lookup q1 to seed replay pieces");
        assert_eq!(q1.lookup(&[0.into()], true).unwrap().len(), 1);
        println!("check 3: lookup q2 to seed replay pieces");
        assert_eq!(q2.lookup(&[0.into()], true).unwrap().len(), 1);
    } else {
        println!("check 2/3: continue without seeding replay pieces");
    }

    // shutdown the node with multile children and write while it is still recovering
    // no writes are reflected because the dataflow graph is disconnected
    println!("check 4: drop g1");
    drop(g1);
    thread::sleep(Duration::from_secs(3));
    println!("check 5: write before recovery");
    for x in 2018..2021 {
        mutx.insert(vec![id.into(), x.into(), y.into()]).unwrap();
    }
    sleep();
    println!("check 6: lookup q1 before recovery CAN'T HANDLE");
    // assert_eq!(q1.lookup(&[0.into()], true).unwrap().len(), 1);
    println!("check 7: lookup q2 before recovery CAN'T HANDLE");
    // assert_eq!(q2.lookup(&[0.into()], true).unwrap().len(), 1);

    // wait for recovery and observe both old (some were filtered out) and new writes
    println!("check 8: wait for recovery");
    thread::sleep(Duration::from_secs(10));
    println!("check 9: done! write after recovery");
    mutx.insert(vec![id.into(), 2020.into(), y.into()]).unwrap();
    sleep();
    println!("check 10: lookup q1 after recovery");
    assert_eq!(q1.lookup(&[0.into()], true).unwrap().len(), 3);
    println!("check 11: lookup q2 after recovery");
    assert_eq!(q2.lookup(&[0.into()], true).unwrap().len(), 3);
    println!("success! now clean shutdown...");
}

#[test]
fn lose_stateless_multi_child_domain_without_replays() {
    stateless_multi_child_domain(false);
}

#[test]
fn lose_stateless_multi_child_domain_with_replays() {
    stateless_multi_child_domain(true);
}

#[test]
fn lose_stateless_multi_parent_domain() {
    let txt = "CREATE TABLE a (id int, x int);\n
               CREATE TABLE b (id int, y int);\n
               QUERY q: SELECT id FROM a UNION SELECT id FROM b;";

    // start enough workers for the multi-parent join node to be on its own worker
    let authority = Arc::new(LocalAuthority::new());
    let mut g = build_authority("worker-0", authority.clone(), false);
    let _g1 = build_authority("worker-1", authority.clone(), false);
    let _g2 = build_authority("worker-1", authority.clone(), false);
    let _g3 = build_authority("worker-1", authority.clone(), false);
    let g4 = build_authority("worker-2", authority.clone(), false);
    sleep();

    g.install_recipe(txt).unwrap();
    sleep();
    println!("{}", g.graphviz().unwrap());

    let mut muta = g.table("a").unwrap().into_sync();
    let mut mutb = g.table("b").unwrap().into_sync();
    // TODO(ygina): ignore lookups for now - why is the reader for UNION partial?
    // let mut q = g.view("q").unwrap().into_sync();
    let n = 3;
    let id = 1;
    let row = vec![id.into(), n.into()];

    // prime the dataflow graph
    println!("check 1: write a");
    muta.insert(row.clone()).unwrap();
    println!("check 2: write a");
    muta.insert(row.clone()).unwrap();
    println!("check 3: write b");
    mutb.insert(row.clone()).unwrap();
    sleep();
    // assert_eq!(q.lookup(&[0.into()], true).unwrap().len(), 3);

    // shutdown the union node and write while it is still recovering
    // no writes are reflected because the dataflow graph is disconnected
    println!("check 4: drop the union node");
    drop(g4);
    thread::sleep(Duration::from_secs(3));
    println!("check 5: write a and b before recovery");
    muta.insert(row.clone()).unwrap();
    muta.insert(row.clone()).unwrap();
    mutb.insert(row.clone()).unwrap();
    sleep();
    // assert_eq!(q.lookup(&[0.into()], true).unwrap().len(), 3);

    // wait for recovery and observe both old and new writes
    println!("check 6: wait for recovery");
    thread::sleep(Duration::from_secs(10));
    println!("check 7: done! write b after recovery");
    mutb.insert(row).unwrap();
    sleep();
    // assert_eq!(q.lookup(&[0.into()], true).unwrap().len(), 7);
    println!("success! now clean shutdown...");
}

fn test_single_child_parent_replica(replay_after_recovery: bool, worker_to_drop: usize) {
    let txt = "CREATE TABLE vote (user int, id int);\n
               QUERY votecount: SELECT id, COUNT(*) AS votes FROM vote WHERE id = ?;";

    // start enough workers for each domain to be assigned its own worker
    let authority = Arc::new(LocalAuthority::new());
    let mut workers = vec![];
    for i in 0..5 {
        let name = format!("worker-{}", i);
        let worker = build_authority(&name, authority.clone(), false);
        workers.push(worker);
    }
    sleep();

    let g_dropped = workers.remove(worker_to_drop);
    let mut g = workers.remove(0);
    g.install_recipe(txt).unwrap();
    sleep();
    println!("{}", g.graphviz().unwrap());

    let mut mutx = g.table("vote").unwrap().into_sync();
    let mut q = g.view("votecount").unwrap().into_sync();
    let id = 0;
    let row = vec![1337.into(), id.into()];

    // prime the dataflow graph
    println!("check 0: write");
    mutx.insert(row.clone()).unwrap();
    if !replay_after_recovery {
        println!("check 1: send initial replay pieces before failure");
        assert_eq!(q.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), 1.into()]]);
    } else {
        println!("check 1: continue without sending initial replay pieces");
    }

    // shutdown a worker and write while it is still recovering
    // no writes are reflected because the dataflow graph is disconnected
    println!("check 2: drop worker {}", worker_to_drop);
    drop(g_dropped);
    thread::sleep(Duration::from_secs(3));
    println!("check 3: write before recovery");
    mutx.insert(row.clone()).unwrap();
    sleep();
    println!("check 4: lookup before recovery CAN'T HANDLE");
    // assert_eq!(q.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), 2.into()]]);

    // wait for recovery and observe both old and new writes
    println!("check 5: wait for recovery");
    thread::sleep(Duration::from_secs(10));
    println!("check 6: write after recovery");
    mutx.insert(row.clone()).unwrap();
    sleep();
    let res = q.lookup(&[id.into()], true);
    let res = if res.is_err() {
        println!("check 7: lookup after recovery (new client)");
        q = g.view("votecount").unwrap().into_sync();
        q.lookup(&[id.into()], true).unwrap()
    } else {
        println!("check 7: lookup after recovery");
        res.unwrap()
    };
    assert_eq!(res, vec![vec![id.into(), 3.into()]]);

    // if the replays were just set up, perform one more write and lookup
    if replay_after_recovery {
        println!("check 8: write after replays setup");
        mutx.insert(row).unwrap();
        sleep();
        println!("check 9: lookup after replays setup");
        assert_eq!(q.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), 4.into()]]);
    }

    println!("success! now clean shutdown...");
}

#[test]
fn lose_top_replica_without_replays() {
    test_single_child_parent_replica(false, 1);
}

#[test]
fn lose_bottom_replica_without_replays() {
    test_single_child_parent_replica(false, 2);
}

#[test]
fn lose_top_replica_with_replays() {
    test_single_child_parent_replica(true, 1);
}

#[test]
fn lose_bottom_replica_with_replays() {
    test_single_child_parent_replica(true, 2);
}

#[test]
fn lose_reader_with_state() {
    test_single_child_parent_replica(false, 4);
}

#[test]
fn lose_reader_without_state() {
    test_single_child_parent_replica(true, 4);
}

/// Returns a list of all the workers, and handles to Article, Vote, ArticleWithVoteCount.
fn setup_vote() -> (Vec<Worker>, SyncTable, SyncTable, SyncView) {
    let txt = "
        # base tables
        CREATE TABLE Article (id int, title varchar(255), PRIMARY KEY(id));
        CREATE TABLE Vote (article_id int, user int);

        # read queries
        QUERY ArticleWithVoteCount: SELECT Article.id, title, VoteCount.votes AS votes \
                    FROM Article \
                    LEFT JOIN (SELECT Vote.article_id, COUNT(user) AS votes \
                               FROM Vote GROUP BY Vote.article_id) AS VoteCount \
                    ON (Article.id = VoteCount.article_id) WHERE Article.id = ?;
    ";

    // g - Article
    // 1 - Vote
    // 2 - *aggregation
    // 3 - *replica
    // 4 - *project
    // 5 - join
    // 6 - *project
    // 7 - reader
    let authority = Arc::new(LocalAuthority::new());
    let mut workers = vec![];
    for i in 0..8 {
        let name = format!("worker-{}", i);
        let worker = build_authority(&name, authority.clone(), false);
        workers.push(worker);
    }
    sleep();

    let mut g = workers.remove(0);
    g.install_recipe(txt).unwrap();
    sleep();
    println!("{}", g.graphviz().unwrap());

    let a = g.table("Article").unwrap().into_sync();
    let v = g.table("Vote").unwrap().into_sync();
    let q = g.view("ArticleWithVoteCount").unwrap().into_sync();

    workers.insert(0, g);
    (workers, a, v, q)
}

fn vote(replay_after_recovery: bool, mut workers_to_drop: Vec<usize>) {
    let (mut workers, mut a, mut v, mut q) = setup_vote();

    // prime the dataflow graph
    println!("check 1: write");
    a.insert(vec![0i64.into(), "Article".into()]).unwrap();
    a.insert(vec![1i64.into(), "Article".into()]).unwrap();
    a.insert(vec![2i64.into(), "Article".into()]).unwrap();
    v.insert(vec![0i64.into(), 0.into()]).unwrap();
    v.insert(vec![1i64.into(), 0.into()]).unwrap();
    v.insert(vec![0i64.into(), 0.into()]).unwrap();
    sleep();
    if !replay_after_recovery {
        println!("check 2: lookup to initialize replay");
        assert_eq!(q.lookup(&[0i64.into()], true).unwrap()[0][2], 2.into());
    } else {
        println!("check 2: continue without initializing replay");
    }
    sleep();

    // shutdown the bottom replica and write while it is still recovering
    // no writes are reflected because the dataflow graph is disconnected
    println!("check 4: drop {:?}", workers_to_drop);
    workers_to_drop.sort();
    for i in 0..workers_to_drop.len() {
        let wi = workers_to_drop[workers_to_drop.len() - i - 1];
        let worker = workers.remove(wi);
        drop(worker);
    }
    thread::sleep(Duration::from_secs(3));
    println!("check 5: send votes before recovery");
    v.insert(vec![0i64.into(), 0.into()]).unwrap();
    v.insert(vec![0i64.into(), 0.into()]).unwrap();
    v.insert(vec![1i64.into(), 0.into()]).unwrap();
    a.insert(vec![3i64.into(), "Article".into()]).unwrap();
    sleep();
    println!("check 7: lookup before recovery CAN'T HANDLE");
    // assert_eq!(q.lookup(&[0i64.into()], true).unwrap()[0][2], 2.into());
    // assert_eq!(q.lookup(&[1i64.into()], true).unwrap()[0][2], 1.into());

    // wait for recovery and observe both old and new writes
    println!("check 8: wait for recovery");
    thread::sleep(Duration::from_secs(8));
    println!("check 9: send votes after recovery");
    v.insert(vec![1i64.into(), 0.into()]).unwrap();
    sleep();
    println!("check 10: lookup after recovery");
    assert_eq!(q.lookup(&[0i64.into()], true).unwrap()[0][2], 4.into());
    assert_eq!(q.lookup(&[1i64.into()], true).unwrap()[0][2], 3.into());
    println!("success! now clean shutdown...");
}

#[test]
fn vote_top_replica_with_replays() {
    vote(true, vec![2]);
}

#[test]
fn vote_bottom_replica_with_replays() {
    vote(true, vec![3]);
}

#[test]
fn vote_upper_projection_with_replays() {
    vote(true, vec![4]);
}

#[test]
fn vote_lower_projection_with_replays() {
    vote(true, vec![6]);
}

#[test]
fn vote_top_replica_without_replays() {
    vote(true, vec![2]);
}

#[test]
fn vote_bottom_replica_without_replays() {
    vote(true, vec![3]);
}

#[test]
fn vote_upper_projection_without_replays() {
    vote(true, vec![4]);
}

#[test]
fn vote_lower_projection_without_replays() {
    vote(true, vec![6]);
}
