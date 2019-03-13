use crate::{Builder, SyncHandle};
use dataflow::{DurabilityMode, PersistenceParameters};
use noria::consensus::LocalAuthority;

use std::sync::Arc;
use std::time::Duration;
use std::{env, thread};

const DEFAULT_SETTLE_TIME_MS: u64 = 200;
const DEFAULT_SHARDING: Option<usize> = Some(2);

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
pub fn start_simple_unsharded(prefix: &str) -> SyncHandle<LocalAuthority> {
    build(prefix, None, false)
}

#[allow(dead_code)]
pub fn start_simple_logging(prefix: &str) -> SyncHandle<LocalAuthority> {
    build(prefix, DEFAULT_SHARDING, true)
}

fn build(prefix: &str, sharding: Option<usize>, log: bool) -> SyncHandle<LocalAuthority> {
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
) -> SyncHandle<LocalAuthority> {
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

#[test]
fn lose_bottom_replica() {
    let txt = "CREATE TABLE vote (user int, id int);\n
               QUERY votecount: SELECT id, COUNT(*) AS votes FROM vote WHERE id = ?;";

    // start a worker for each domain in the graph
    let authority = Arc::new(LocalAuthority::new());
    let mut g = build_authority("worker-0", authority.clone(), false);
    let _g1 = build_authority("worker-1", authority.clone(), false);
    let g2 = build_authority("worker-2", authority.clone(), false);
    sleep();

    g.install_recipe(txt).unwrap();
    sleep();

    let mut mutx = g.table("vote").unwrap().into_sync();
    let mut q = g.view("votecount").unwrap().into_sync();
    let id = 0;

    // prime the dataflow graph
    mutx.insert(vec![1337.into(), id.into()]).unwrap();
    assert_eq!(q.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), 1.into()]]);

    // shutdown the bottom replica and write while it is still recovering
    // no writes are reflected because the dataflow graph is disconnected
    drop(g2);
    thread::sleep(Duration::from_secs(3));
    for _ in 0..7 {
        mutx.insert(vec![1337.into(), id.into()]).unwrap();
    }
    sleep();
    assert_eq!(q.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), 1.into()]]);

    // wait for recovery and observe both old and new writes
    thread::sleep(Duration::from_secs(10));
    mutx.insert(vec![1337.into(), id.into()]).unwrap();
    sleep();
    assert_eq!(q.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), 9.into()]]);
    println!("success! now clean shutdown...");
}

#[test]
fn lose_top_replica() {
    let txt = "CREATE TABLE vote (user int, id int);\n
               QUERY votecount: SELECT id, COUNT(*) AS votes FROM vote WHERE id = ?;";

    // start enough workers for the top replica to be assigned its own worker
    let authority = Arc::new(LocalAuthority::new());
    let mut g = build_authority("worker-0", authority.clone(), false);
    let g1 = build_authority("worker-1", authority.clone(), false);
    let _g2 = build_authority("worker-2", authority.clone(), false);
    let _g3 = build_authority("worker-3", authority.clone(), false);
    sleep();

    g.install_recipe(txt).unwrap();
    sleep();

    let mut mutx = g.table("vote").unwrap().into_sync();
    let mut q = g.view("votecount").unwrap().into_sync();
    let id = 0;

    // prime the dataflow graph
    mutx.insert(vec![1337.into(), id.into()]).unwrap();
    assert_eq!(q.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), 1.into()]]);

    // shutdown the top replica and write while it is still recovering
    // no writes are reflected because the dataflow graph is disconnected
    drop(g1);
    thread::sleep(Duration::from_secs(3));
    for _ in 0..7 {
        mutx.insert(vec![1337.into(), id.into()]).unwrap();
    }
    sleep();
    assert_eq!(q.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), 1.into()]]);

    // wait for recovery and observe both old and new writes
    thread::sleep(Duration::from_secs(10));
    mutx.insert(vec![1337.into(), id.into()]).unwrap();
    sleep();
    assert_eq!(q.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), 9.into()]]);
    println!("success! now clean shutdown...");
}

#[test]
fn lose_stateless_domain() {
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

    let mut mutx = g.table("vote").unwrap().into_sync();
    let mut q = g.view("user").unwrap().into_sync();
    let id = 0;

    // prime the dataflow graph
    mutx.insert(vec![1.into(), id.into()]).unwrap();
    sleep();
    assert_eq!(q.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), 1.into()]]);

    // shutdown the worker with the middle node and write while it is still recovering
    // no writes are reflected because the dataflow graph is disconnected
    drop(g1);
    thread::sleep(Duration::from_secs(3));
    mutx.insert(vec![2.into(), id.into()]).unwrap();
    sleep();
    assert_eq!(q.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), 1.into()]]);

    // wait for recovery and observe both old and new writes
    thread::sleep(Duration::from_secs(10));
    mutx.insert(vec![3.into(), id.into()]).unwrap();
    sleep();
    let expected = vec![
        vec![id.into(), 1.into()],
        vec![id.into(), 2.into()],
        vec![id.into(), 3.into()],
    ];
    assert_eq!(q.lookup(&[id.into()], true).unwrap(), expected);
    println!("success! now clean shutdown...");
}

#[test]
fn lose_multi_child_bottom_replica() {
    let txt = "CREATE TABLE vote (user int, id int);\n
               QUERY q1: SELECT id, COUNT(*) AS votes FROM vote WHERE id = ?;\n
               QUERY q2: SELECT COUNT(*) AS votes FROM vote WHERE id = ?;";

    // start enough workers for the bottom replica to be in its own domain
    let authority = Arc::new(LocalAuthority::new());
    let mut g = build_authority("worker-0", authority.clone(), false);
    let _g1 = build_authority("worker-1", authority.clone(), false);
    let g2 = build_authority("worker-2", authority.clone(), false);
    let _g3 = build_authority("worker-3", authority.clone(), false);
    let _g4 = build_authority("worker-4", authority.clone(), false);
    sleep();

    g.install_recipe(txt).unwrap();
    sleep();

    let mut mutx = g.table("vote").unwrap().into_sync();
    let mut q1 = g.view("q1").unwrap().into_sync();
    let mut q2 = g.view("q2").unwrap().into_sync();
    let id = 8;

    // prime the dataflow graph
    mutx.insert(vec![1337.into(), id.into()]).unwrap();
    assert_eq!(q1.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), 1.into()]]);
    assert_eq!(q2.lookup(&[id.into()], true).unwrap(), vec![vec![1.into(), id.into()]]);

    // shutdown the bottom replica and write while it is still recovering
    // no writes are reflected because the dataflow graph is disconnected
    drop(g2);
    thread::sleep(Duration::from_secs(3));
    for _ in 0..7 {
        mutx.insert(vec![1337.into(), id.into()]).unwrap();
    }
    sleep();
    assert_eq!(q1.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), 1.into()]]);
    assert_eq!(q2.lookup(&[id.into()], true).unwrap(), vec![vec![1.into(), id.into()]]);

    // wait for recovery and observe both old and new writes
    thread::sleep(Duration::from_secs(10));
    mutx.insert(vec![1337.into(), id.into()]).unwrap();
    sleep();
    assert_eq!(q1.lookup(&[id.into()], true).unwrap(), vec![vec![id.into(), 9.into()]]);
    assert_eq!(q2.lookup(&[id.into()], true).unwrap(), vec![vec![9.into(), id.into()]]);
    println!("success! now clean shutdown...");
}

#[test]
fn lose_stateless_multi_child_domain() {
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

    let mut mutx = g.table("data").unwrap().into_sync();
    let mut q1 = g.view("x").unwrap().into_sync();
    let mut q2 = g.view("y").unwrap().into_sync();
    let id = 7;
    let y = 100;

    // prime the dataflow graph
    mutx.insert(vec![id.into(), 2020.into(), y.into()]).unwrap();
    sleep();
    assert_eq!(q1.lookup(&[0.into()], true).unwrap().len(), 1);
    assert_eq!(q2.lookup(&[0.into()], true).unwrap().len(), 1);

    // shutdown the bottom replica and write while it is still recovering
    // no writes are reflected because the dataflow graph is disconnected
    drop(g1);
    thread::sleep(Duration::from_secs(3));
    for x in 2018..2021 {
        mutx.insert(vec![id.into(), x.into(), y.into()]).unwrap();
    }
    sleep();
    assert_eq!(q1.lookup(&[0.into()], true).unwrap().len(), 1);
    assert_eq!(q2.lookup(&[0.into()], true).unwrap().len(), 1);

    // wait for recovery and observe both old (some were filtered out) and new writes
    thread::sleep(Duration::from_secs(10));
    mutx.insert(vec![id.into(), 2020.into(), y.into()]).unwrap();
    sleep();
    assert_eq!(q1.lookup(&[0.into()], true).unwrap().len(), 3);
    assert_eq!(q2.lookup(&[0.into()], true).unwrap().len(), 3);
    println!("success! now clean shutdown...");
}

#[test]
fn lose_stateless_multi_parent_domain() {
    let txt = "CREATE TABLE a (id int, x int);\n
               CREATE TABLE b (id int, y int);\n
               QUERY q: SELECT x, y FROM a, b WHERE a.id = b.id;";

    // start enough workers for the multi-parent join node to be on its own worker
    let authority = Arc::new(LocalAuthority::new());
    let mut g = build_authority("worker-0", authority.clone(), true);
    let _g1 = build_authority("worker-1", authority.clone(), false);
    let g2 = build_authority("worker-2", authority.clone(), false);
    sleep();

    g.install_recipe(txt).unwrap();
    sleep();

    let mut muta = g.table("a").unwrap().into_sync();
    let mut mutb = g.table("b").unwrap().into_sync();
    let mut q = g.view("q").unwrap().into_sync();
    let n = 3;
    let id = 1;
    let row = vec![id.into(), n.into()];

    // prime the dataflow graph
    muta.insert(row.clone()).unwrap();
    muta.insert(row.clone()).unwrap();
    mutb.insert(row.clone()).unwrap();
    sleep();
    assert_eq!(q.lookup(&[0.into()], true).unwrap().len(), 2);

    // shutdown the bottom replica and write while it is still recovering
    // no writes are reflected because the dataflow graph is disconnected
    drop(g2);
    thread::sleep(Duration::from_secs(3));
    muta.insert(row.clone()).unwrap();
    muta.insert(row.clone()).unwrap();
    mutb.insert(row.clone()).unwrap();
    sleep();
    assert_eq!(q.lookup(&[0.into()], true).unwrap().len(), 2);

    // wait for recovery and observe both old and new writes
    thread::sleep(Duration::from_secs(10));
    mutb.insert(row).unwrap();
    sleep();
    assert_eq!(q.lookup(&[0.into()], true).unwrap().len(), 12);
    println!("success! now clean shutdown...");
}
