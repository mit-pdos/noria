use clap::value_t_or_exit;
use noria::{FrontierStrategy, ReuseConfigType};
use std::collections::{HashMap, HashSet};
use std::time::{Instant, Duration};
use std::thread;
use std::error::Error;
use noria::{logger_pls, DurabilityMode, PersistenceParameters, DataType};
use noria::manual::Base;
use noria::manual::ops::join::JoinSource::*;
use noria::manual::ops::join::{Join, JoinType};
use noria::manual::ops::union::Union;
use noria::manual::ops::rewrite::Rewrite;
use noria::manual::ops::filter::{Filter, FilterCondition, Value};   
use noria::manual::ops::grouped::aggregate::Aggregation;  
use nom_sql::Operator;
use noria::{Builder, Handle, LocalAuthority};
use std::future::Future;
use rand::prelude::*;

#[macro_use]
mod populate;
use crate::populate::Populate;


pub struct Backend {
    g: Handle<LocalAuthority>,
    done: Box<dyn Future<Output = ()> + Unpin>,
}


impl Backend {
    async fn make(verbose: bool) -> Box<Backend> {
        let mut b = Builder::default();
      
        b.set_sharding(None);
        b.disable_partial(); 
        b.set_persistence(PersistenceParameters::new(
            DurabilityMode::MemoryOnly,
            Duration::from_millis(1),
            Some(String::from("manual_policy_graph")),
            1,
        ));
        if verbose {
            b.log_with(logger_pls());
        }

        let (g, done) = b.start_local().await.unwrap();
        
        let reuse = true; 
    
        Box::new(Backend {
            g,
            done: Box::new(done),
        })
    }

    pub async fn populate(&mut self, name: &'static str, records: Vec<Vec<DataType>>) -> usize {
        let mut mutator = self.g.table(name).await.unwrap();

        let i = records.len();
        mutator.perform_all(records).await.unwrap();
        i
    }
}


 // Mem stats 
async fn memstats(g: &mut noria::Handle<LocalAuthority>) {
    let at = "done"; 
    if let Ok(mem) = std::fs::read_to_string("/proc/self/statm") {
        let vmrss = mem.split_whitespace().nth(2 - 1).unwrap();
        let data = mem.split_whitespace().nth(6 - 1).unwrap();
        println!("# VmRSS @ {}: {} ", at, vmrss);
        println!("# VmData @ {}: {} ", at, data);
    }

    let mut reader_mem : u64 = 0;
    let mut base_mem : u64 = 0;
    let mut mem : u64 = 0;
    let stats = g.statistics().await.unwrap();
    let mut filter_mem : u64 = 0;
    for (nid, nstats) in stats.values() {
        for (nid, nstat) in nstats {
            println!("[{}] node {:?} ({}): {:?}", at, nid, nstat.desc, nstat.mem_size);
            if nstat.desc == "B" {
                base_mem += nstat.mem_size;
            } else if nstat.desc == "reader node" {
                reader_mem += nstat.mem_size;
            } else {
                mem += nstat.mem_size;
                if nstat.desc.contains("f0") {
                    filter_mem += nstat.mem_size;
                }
            }
        }
    }
    println!("# base memory @ {}: {}", at, base_mem);
    println!("# reader memory @ {}: {}", at, reader_mem);
    println!("# materialization memory @ {}: {} (filters: {})", at, mem, filter_mem);
}


#[tokio::main]
async fn main() {
    use clap::{App, Arg};
    let args = App::new("manual-graph")
        .version("0.1")
        .about("Benchmarks Twitter main timeline graph with security policies.")
        .arg(
            Arg::with_name("reuse")
                .long("reuse")
                .default_value("no")
                .possible_values(&["no", "finkelstein", "relaxed", "full"])
                .help("Query reuse algorithm"),
        )
        .arg(
            Arg::with_name("materialization")
                .long("materialization")
                .short("m")
                .default_value("full")
                .possible_values(&["full", "partial", "shallow-readers", "shallow-all"])
                .help("Set materialization strategy for the benchmark"),
        )
        .arg(
            Arg::with_name("ntweets")
                .short("n")
                .takes_value(true)
                .default_value("10000")
        )
        .arg(
            Arg::with_name("nusers")
                .short("u")
                .takes_value(true)
                .default_value("10000")
        )
        .arg(
            Arg::with_name("logged-in")
                .short("l")
                .default_value("1.0")
                .help("Fraction of users that are logged in."),
        )
        .arg(
            Arg::with_name("percent-private")
                .short("p")
                .default_value("0.2")
                .help("Percentage of private users")
            )
        .arg(
            Arg::with_name("graph")
                .short("g")
                .takes_value(true)
                .help("File to dump application's soup graph, if set"),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .multiple(true)
                .help("Enable verbose output"),
        )
        .get_matches();


    let verbose = args.is_present("verbose");
    let loggedf = value_t_or_exit!(args, "logged-in", f64);
    let nusers = value_t_or_exit!(args, "nusers", usize); 
    let ntweets = value_t_or_exit!(args, "ntweets", usize); 
    let private = value_t_or_exit!(args, "percent-private", f64); 

    println!("starting noria!");        

    let mut backend = Backend::make(verbose).await;

    let init = Instant::now();
    thread::sleep(Duration::from_millis(2000));

    let uid : usize = 1; 
    let (tweets, users, blocked, follows) = backend.g.migrate(move |mig| { 
        let tweets = mig.add_base("Tweets", &["userId", "id", "content", "time", "retweetId"], Base::default()); 
        let users = mig.add_base("Users", &["userId", "name", "isPrivate", 
                                            "birthdayMonth", "birthdayDay", 
                                            "birthdayYear", "email", "password"], Base::default()); 
        let blocked = mig.add_base("BlockedAccounts", &["userId", "blockedId"], Base::default()); 
        let follows = mig.add_base("Follows", &["userId", "followedId"], Base::default()); 

        (tweets, users, blocked, follows)
    }).await; 


    let (tweets_with_user_info, retweets, all_tweets) = backend.g.migrate(move |mig| {
        let tweets_with_user_info = mig.add_ingredient("TweetsWithUserInfo", 
                                                        &["userId", "id", "content", "time", "retweetId", "name", "isPrivate"], 
                                                        Join::new(tweets, users, JoinType::Inner, vec![B(0, 0), L(1), L(2), L(3), L(4), R(1), R(2)])); 
        let retweets = mig.add_ingredient("RetweetsWithUserInfo",
                                          &["userId", "id", "content", "time", "retweetId", "name", "isPrivate"],
                                          Join::new(tweets_with_user_info,
                                                    tweets_with_user_info,
                                                    JoinType::Inner,
                                                    vec![L(0), B(1, 4), L(2), L(3), L(4), L(5), L(6)]));

        let mut emits = HashMap::new(); 
        emits.insert(tweets_with_user_info, vec![0, 1, 2, 3, 4, 5, 6]); 
        emits.insert(retweets, vec![0, 1, 2, 3, 4, 5, 6]); 

        let all_tweets = mig.add_ingredient("AllTweetsWithUserInfo", 
                                            &["userId", "id", "content", "time", "retweetId", "name", "isPrivate"],
                                            Union::new(emits)); 
        
        (tweets_with_user_info, retweets, all_tweets)
    }).await; 


    let (blocked_accounts, blocked_by_accounts) = backend.g.migrate(move |mig| {
        let blocked_accounts = mig.add_ingredient("BlockedAccounts", 
                                                  &["userId", "blockedId"], 
                                                Filter::new(blocked, 
                                                &[(0, FilterCondition::Comparison(Operator::Equal, Value::Constant(uid.into())))])); 

        let blocked_by_accounts = mig.add_ingredient("BlockedByAccounts", 
                                                &["userId", "blockedId"], 
                                                Filter::new(blocked, 
                                                &[(1, FilterCondition::Comparison(Operator::Equal, Value::Constant(uid.into())))])); 
        
        (blocked_accounts, blocked_by_accounts) 
    }).await; 

    let (users_you_follow, private_users) = backend.g.migrate(move |mig| {
        let users_you_follow = mig.add_ingredient("UsersYouFollow", 
                                                  &["userId", "followedId"], 
                                                  Filter::new(follows, 
                                                  &[(0, FilterCondition::Comparison(Operator::Equal, Value::Constant(uid.into())))])); 
        
        let private_users = mig.add_ingredient("PrivateUsers", 
                                                &["userId", "name", "isPrivate", 
                                                "birthdayMonth", "birthdayDay", 
                                                "birthdayYear", "email", "password"], 
                                                Filter::new(users, &[(2, FilterCondition::Comparison(Operator::Equal, Value::Constant(1.into())))])); 
        (users_you_follow, private_users)
    }).await; 


    let (visible_tweets1, visible_tweets2, visible_tweets3, visible_tweets4, visible_tweets5) = backend.g.migrate(move |mig| {
        let visible_tweets1 = mig.add_ingredient("PublicTweets", &["userId", "id", "content", "time", "retweetId", "name", "isPrivate"], 
                                                 Filter::new(all_tweets, &[(6, FilterCondition::Comparison(Operator::Equal, Value::Constant(1.into())))]));   
    
        let visible_tweets2 = mig.add_ingredient("FollowedTweets", &["userId", "id", "content", "time", "retweetId", "name", "isPrivate"], 
                                                 Join::new(users_you_follow, all_tweets, 
                                                 JoinType::Inner, vec![B(1, 0), R(1), R(2), R(3), R(4), R(5), R(6)])); 
        
        let mut emits = HashMap::new(); 
        emits.insert(visible_tweets1, vec![0, 1, 2, 3, 4, 5, 6]);
        emits.insert(visible_tweets2, vec![0, 1, 2, 3, 4, 5, 6]);

        let visible_tweets3 = mig.add_ingredient("PublicAndFollowedTweets", &["userId", "id", "content", "time", "retweetId", "name", "isPrivate"],
                                                 Union::new(emits)); 
        
        let visible_tweets4a = mig.add_ingredient("AllExcludingBlocked", &["userId", "id", "content", "time", "retweetId", "name", "isPrivate"], 
                                                 Join::new(visible_tweets3, blocked_accounts,
                                                 JoinType::Left, vec![B(0, 1), L(1), L(2), L(3), L(4), L(5), L(6), R(0)])); 
        
        // TODO how are NULLs represented? Almost certainly incorrect below
        let visible_tweets4 = mig.add_ingredient("AllExcludingBlockedFinal", &["userId", "id", "content", "time", "retweetId", "name", "isPrivate"], 
                                                 Filter::new(visible_tweets4a, &[(7, FilterCondition::Comparison(Operator::NotEqual, Value::Constant(0.into())))]));  
    
        let visible_tweets5a = mig.add_ingredient("AllExcludingBlockedBy", &["userId", "id", "content", "time", "retweetId", "name", "isPrivate"], 
                                                 Join::new(visible_tweets3, blocked_by_accounts,
                                                 JoinType::Left, vec![B(0, 1), L(1), L(2), L(3), L(4), L(5), L(6), R(0)])); 
        
        // TODO how are NULLs represented? Almost certainly incorrect below
        let visible_tweets5 = mig.add_ingredient("AllExcludingBlockedByFinal", &["userId", "id", "content", "time", "retweetId", "name", "isPrivate"], 
                                                 Filter::new(visible_tweets5a, &[(7, FilterCondition::Comparison(Operator::NotEqual, Value::Constant(0.into())))]));  
    
        (visible_tweets1, visible_tweets2, visible_tweets3, visible_tweets4, visible_tweets5)
    }).await;
    

    let mut p = Populate::new(nusers, ntweets, private); 
        
    let mut users = p.get_users();
    let mut follows = p.get_follows(); 
    let mut tweets = p.get_tweets(); 
    let mut blocks = p.get_blocks(); 

    backend.populate("Users", users.clone()).await;
    backend.populate("Follows", follows.clone()).await; 
    backend.populate("Tweets", tweets.clone()).await; 
    backend.populate("BlockedAccounts", blocks.clone()).await; 
        
    memstats(&mut backend.g).await;

    println!("{}", backend.g.graphviz().await.unwrap());
}
