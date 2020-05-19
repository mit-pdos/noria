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
use std::fs::File;
use std::io::prelude::*;

#[macro_use]
mod populate;
use crate::populate::Populate;


pub struct Backend {
    g: Handle<LocalAuthority>,
    done: Box<dyn Future<Output = ()> + Unpin>,
}

impl Backend {
    async fn make(verbose: bool, partial: bool) -> Box<Backend> {
        let mut b = Builder::default();
      
        b.set_sharding(None);

        if !partial {
            b.disable_partial(); 
        }
        
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
        println!("Inserting {:?} records into {}.", i, name); 
        mutator.perform_all(records).await.unwrap();
        i
    }
}


// Mem stats 
async fn memstats(g: &mut noria::Handle<LocalAuthority>, materialization: String, nusers: usize, ntweets: usize, read_latencies: Option<String>) {
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

    let base = format!("# base memory @ {}: {}", at, base_mem);
    let reader = format!("# reader memory @ {}: {}", at, reader_mem);
    let mat = format!("# materialization memory @ {}: {} (filters: {})", at, mem, filter_mem);
    let mult = format!("multiple: {:?}", ((mem + reader_mem) as f64) / (base_mem as f64)); 
    
    println!("{}", base); 
    println!("{}", reader); 
    println!("{}", mat); 
    println!("{}", mult); 


    let exp_summary = format!("{}\n{}\n{}\n{:?}\n{:?}\n", base, reader, mat, mult, read_latencies.unwrap()); 
    let mut file = File::create(format!("exp-{}mat-{}nusers-{}ntweets.txt", materialization, nusers, ntweets)).unwrap();
    file.write_all(exp_summary.as_bytes());

}



async fn construct_graph(backend: &mut Box<Backend>, verbose: bool, partial: bool, nusers: usize, ntweets: usize, private: f64, materialization: String) {
    println!("constructing graph: materialization: {}, partial: {}, nusers: {}, ntweets: {} percent private: {}", materialization, partial, nusers, ntweets, private); 

    if materialization == "client".to_string() {
        let init = Instant::now();
        thread::sleep(Duration::from_millis(2000));

        let uid : usize = 1; 
        let (tweets, users, blocked, follows) = backend.g.migrate(move |mig| { 
            let tweets = mig.add_base("Tweets", &["userId", "id", "content", "time", "retweetId", "bogo"], Base::default()); 
            let users = mig.add_base("Users", &["userId", "name", "isPrivate", 
                                                "birthdayMonth", "birthdayDay", 
                                                "birthdayYear", "email", "password"], Base::default()); 
            let blocked = mig.add_base("BlockedAccounts", &["userId", "blockedId"], Base::default()); 
            let follows = mig.add_base("Follows", &["userId", "followedId"], Base::default()); 

            let ri = mig.maintain("Blocked".to_string(), blocked, &[0]);
            let r2 = mig.maintain_anonymous(follows, &[0]); 
            let r3 = mig.maintain_anonymous(tweets, &[5]); 
            (tweets, users, blocked, follows)
        }).await; 


        let (all_tweets) = backend.g.migrate(move |mig| {
            let tweets_with_user_info = mig.add_ingredient("TweetsWithUserInfo", 
                                                            &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "bogo"], 
                                                            Join::new(tweets, users, JoinType::Inner, vec![B(0, 0), L(1), L(2), L(3), L(4), R(1), R(2), L(5)])); 
            
            let tweets_with_user_info2 = mig.add_ingredient("TweetsWithUserInfo2", 
                                                            &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "bogo"], 
                                                            Join::new(tweets, users, JoinType::Inner, vec![B(0, 0), L(1), L(2), L(3), L(4), R(1), R(2), L(5)])); 
            
            // let prelim = mig.maintain_anonymous(tweets_with_user_info, &[7]); 

            let all_tweets = mig.add_ingredient("AllTweetsWithUserInfo",
                                            &["id", "time", "content", "userId", "name", 
                                              "retweetId", "retweetTime", "retweetContent", 
                                              "retweetUserId", "retweetName", "isPrivate", "bogo"],
                                            Join::new(tweets_with_user_info,
                                                      tweets_with_user_info2,
                                                      JoinType::Left,
                                                      vec![L(1), L(3), L(2), L(0), L(5), B(4, 1), R(3), R(2), R(0), R(5), R(6), L(7)])); 

            let mut ri = mig.maintain_anonymous(all_tweets, &[11]);    

            (all_tweets) 
        }).await; 

        // let public_tweets = backend.g.migrate(move |mig| {
        //     let public_tweets = mig.add_ingredient("PublicTweets", &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "bogo"], 
        //                                         Filter::new(all_tweets, &[(6, FilterCondition::Comparison(Operator::Equal, Value::Constant(0.into())))]));  
        //     let mut ri = mig.maintain_anonymous(public_tweets, &[7]); 
        // }).await; 

        // let private_users = backend.g.migrate(move |mig| {
        //     let private_users = mig.add_ingredient("PrivateUsers", 
        //                                             &["userId", "name", "isPrivate", 
        //                                             "birthdayMonth", "birthdayDay", 
        //                                             "birthdayYear", "email", "password"], 
        //                                             Filter::new(users, &[(2, FilterCondition::Comparison(Operator::Equal, Value::Constant(1.into())))])); 
        //     (private_users)
        // }).await; 

    } else if materialization == "full".to_string() || materialization == "partial".to_string() {
        let init = Instant::now();
        thread::sleep(Duration::from_millis(2000));

        let (tweets, users, blocked, follows) = backend.g.migrate(move |mig| { 
            let tweets = mig.add_base("Tweets", &["userId", "id", "content", "time", "retweetId", "bogo"], Base::default()); 
            let users = mig.add_base("Users", &["userId", "name", "isPrivate", 
                                                "birthdayMonth", "birthdayDay", 
                                                "birthdayYear", "email", "password"], Base::default()); 
            let blocked = mig.add_base("BlockedAccounts", &["userId", "blockedId"], Base::default()); 
            let follows = mig.add_base("Follows", &["userId", "followedId"], Base::default()); 

            (tweets, users, blocked, follows)
        }).await; 

        let (all_tweets) = backend.g.migrate(move |mig| {
            let tweets_with_user_info = mig.add_ingredient("TweetsWithUserInfo", 
                                                            &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "bogo"], 
                                                            Join::new(tweets, users, JoinType::Inner, vec![B(0, 0), L(1), L(2), L(3), L(4), R(1), R(2), L(5)])); 
            
            let tweets_with_user_info2 = mig.add_ingredient("TweetsWithUserInfo2", 
                                                            &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "bogo"], 
                                                            Join::new(tweets, users, JoinType::Inner, vec![B(0, 0), L(1), L(2), L(3), L(4), R(1), R(2), L(5)])); 
            
            // let prelim = mig.maintain_anonymous(tweets_with_user_info, &[7]); 

            let all_tweets = mig.add_ingredient("AllTweetsWithUserInfo",
                                            &["id", "time", "content", "userId", "name", 
                                              "retweetId", "retweetTime", "retweetContent", 
                                              "retweetUserId", "retweetName", "isPrivate", "bogo"],
                                            Join::new(tweets_with_user_info,
                                                      tweets_with_user_info2,
                                                      JoinType::Left,
                                                      vec![L(1), L(3), L(2), L(0), L(5), B(4, 1), R(3), R(2), R(0), R(5), R(6), L(7)])); 

            let mut ri = mig.maintain_anonymous(all_tweets, &[11]);    

            (all_tweets) 
        }).await; 


        for user in 0..nusers {
            backend.g.migrate(move |mig| {
                let blocked_accounts = mig.add_ingredient(format!("UsersBlockedAccounts_{:?}", user), 
                                                        &["Id", "blockedId"], 
                                                        Filter::new(blocked, 
                                                        &[(0, FilterCondition::Comparison(Operator::Equal, Value::Constant(user.into())))])); 
        
                let blocked_by_accounts = mig.add_ingredient(format!("UsersBlockedByAccounts_{:?}", user), 
                                                        &["Id", "blockedId"], 
                                                        Filter::new(blocked, 
                                                        &[(1, FilterCondition::Comparison(Operator::Equal, Value::Constant(user.into())))])); 
                
                let users_you_follow = mig.add_ingredient(format!("UsersYouFollow_{:?}", user), 
                                                        &["userId", "followedId"], 
                                                        Filter::new(follows, 
                                                        &[(0, FilterCondition::Comparison(Operator::Equal, Value::Constant(user.into())))]));  
                
                
                let visible_tweets2 = mig.add_ingredient(format!("FollowedTweets_{:?}", user), &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "bogo"], 
                                                        Join::new(users_you_follow, all_tweets, 
                                                        JoinType::Inner, vec![B(1, 0), R(1), R(2), R(3), R(4), R(5), R(6), R(7)])); 
           
        
                // let mut emits = HashMap::new(); 
                // emits.insert(visible_tweets1, vec![0, 1, 2, 3, 4, 5, 6]);
                // emits.insert(visible_tweets2, vec![0, 1, 2, 3, 4, 5, 6]);
        
                // let visible_tweets3 = mig.add_ingredient(format!("PublicAndFollowedTweets_{:?}", user), &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "bogo"],
                //                                         Union::new(emits)); 
        
        
                let visible_tweets4a = mig.add_ingredient(format!("AllExcludingBlocked_{:?}", user), &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "Id", "bogo"], 
                                                        Join::new(visible_tweets2, blocked_accounts,
                                                        JoinType::Left, vec![B(0, 1), L(1), L(2), L(3), L(4), L(5), L(6), R(0), L(7)])); 
                                                    
        
                let visible_tweets4 = mig.add_ingredient(format!("AllExcludingBlockedFinal_{:?}", user), &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "Id", "bogo"], 
                                                        Filter::new(visible_tweets4a, &[(7, FilterCondition::Comparison(Operator::Equal, Value::Constant(DataType::None)))]));  
            
            
                let visible_tweets5a = mig.add_ingredient(format!("AllExcludingBlockedBy_{:?}", user), &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "Id", "bogo"], 
                                                        Join::new(visible_tweets4, blocked_by_accounts,
                                                        JoinType::Left, vec![B(0, 1), L(1), L(2), L(3), L(4), L(5), L(6), R(0), L(7)])); 
                
                let visible_tweets5 = mig.add_ingredient(format!("AllExcludingBlockedByFinal_{:?}", user), &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "Id", "bogo"], 
                                                        Filter::new(visible_tweets5a, &[(7, FilterCondition::Comparison(Operator::Equal, Value::Constant(DataType::None)))]));  
            
                let ri = mig.maintain_anonymous(visible_tweets5, &[7]);
            }).await;

        }
    }
    let mut file = File::create(format!("{}mat-{}nusers.dot", materialization, nusers)).unwrap();
    file.write_all(backend.g.graphviz().await.unwrap().as_bytes());
}


async fn construct_baseline_graph(backend: &mut Box<Backend>, verbose: bool, partial: bool, nusers: usize, ntweets: usize, private: f64, materialization: String) {
    let init = Instant::now();
    thread::sleep(Duration::from_millis(2000));
    println!("constructing baseline graph: materialization: {}, partial: {}, nusers: {}, ntweets: {} percent private: {}", materialization, partial, nusers, ntweets, private);

    let (tweets, users, blocked, follows) = backend.g.migrate(move |mig| { 
        let tweets = mig.add_base("Tweets", &["userId", "id", "content", "time", "retweetId", "bogo"], Base::default()); 
        let users = mig.add_base("Users", &["userId", "name", "isPrivate", 
                                            "birthdayMonth", "birthdayDay", 
                                            "birthdayYear", "email", "password"], Base::default()); 
        let blocked = mig.add_base("BlockedAccounts", &["userId", "blockedId"], Base::default()); 
        let follows = mig.add_base("Follows", &["userId", "followedId"], Base::default()); 

        let ri = mig.maintain("Blocked".to_string(), blocked, &[0]);
        let r2 = mig.maintain_anonymous(follows, &[0]); 
        let r3 = mig.maintain_anonymous(tweets, &[5]); 
        (tweets, users, blocked, follows)
    }).await; 

    let (all_tweets) = backend.g.migrate(move |mig| {
        let tweets_with_user_info = mig.add_ingredient("TweetsWithUserInfo", 
                                                        &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "bogo"], 
                                                        Join::new(tweets, users, JoinType::Inner, vec![B(0, 0), L(1), L(2), L(3), L(4), R(1), R(2), L(5)])); 
        
        let tweets_with_user_info2 = mig.add_ingredient("TweetsWithUserInfo2", 
                                                        &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "bogo"], 
                                                        Join::new(tweets, users, JoinType::Inner, vec![B(0, 0), L(1), L(2), L(3), L(4), R(1), R(2), L(5)])); 
        
        let all_tweets = mig.add_ingredient("AllTweetsWithUserInfo",
                                        &["id", "time", "content", "userId", "name", 
                                          "retweetId", "retweetTime", "retweetContent", 
                                          "retweetUserId", "retweetName", "isPrivate", "bogo"],
                                        Join::new(tweets_with_user_info,
                                                  tweets_with_user_info2,
                                                  JoinType::Left,
                                                  vec![L(1), L(3), L(2), L(0), L(5), B(4, 1), R(3), R(2), R(0), R(5), R(6), L(7)])); 

        let mut ri = mig.maintain_anonymous(all_tweets, &[11]);    

        (all_tweets) 
    }).await; 

    for user in 0..nusers {
        backend.g.migrate(move |mig| {
           
            let users_you_follow = mig.add_ingredient(format!("UsersYouFollow_{:?}", user), 
                                                    &["userId", "followedId"], 
                                                    Filter::new(follows, 
                                                    &[(0, FilterCondition::Comparison(Operator::Equal, Value::Constant(user.into())))]));  
            
            
            let visible_tweets2 = mig.add_ingredient(format!("FollowedTweets_{:?}", user), &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "bogo"], 
                                                    Join::new(users_you_follow, all_tweets, 
                                                    JoinType::Inner, vec![B(1, 0), R(1), R(2), R(3), R(4), R(5), R(6), R(7)])); 
        
            let ri = mig.maintain_anonymous(visible_tweets2, &[7]);
        }).await;

    }
    
    let mut file = File::create(format!("baseline-{}mat-{}nusers.dot", materialization, nusers)).unwrap();
    file.write_all(backend.g.graphviz().await.unwrap().as_bytes());
}


async fn run_experiment(backend: &mut Box<Backend>, verbose: bool, partial: bool, nusers: usize, ntweets: usize, private: f64, materialization: String) {
    let mut p = Populate::new(nusers, ntweets, private); 
        
    let mut users = p.get_users();
    let mut follows = p.get_follows(); 
    let mut tweets = p.get_tweets(); 
    let mut blocks = p.get_blocks(); 

    backend.populate("Users", users.clone()).await;
    backend.populate("Follows", follows.clone()).await; 
    backend.populate("Tweets", tweets.clone()).await; 
    backend.populate("BlockedAccounts", blocks.clone()).await; 

    use std::{thread, time};

    let ten_millis = time::Duration::from_millis(10000);
    let now = time::Instant::now();

    thread::sleep(ten_millis);

    if materialization == "client".to_string() {
        let uid = 0;
        let leaf = format!("Blocked");
        let mut blocked_user = HashSet::new(); 
        let mut users_followed = HashSet::new();
        let mut visible_tweets = Vec::new(); 

        let mut getter1 = backend.g.view(&leaf).await.unwrap();

        let leaf = format!("Follows");
        let mut getter2 = backend.g.view(&leaf).await.unwrap();

        let leaf = format!("AllTweetsWithUserInfo");
        let mut getter3 = backend.g.view(&leaf).await.unwrap();

        // let leaf = format!("PublicTweets");
        // let mut getter4 = backend.g.view(&leaf).await.unwrap();

        let start = time::Instant::now();
       
        let mut res1 = getter1.lookup(&[0.into()], true).await.unwrap(); // Blocked tweets
        
        // Blocked accounts and blocked by accounts: 
        for record in res1.iter() {
            if record[0] == uid.into() {
                blocked_user.insert(record[1].clone()); 
            }
        }

        // Followed 
        let mut res2 = getter2.lookup(&[0.into()], true).await.unwrap(); // Followed users

        for record in res2.iter() {
            if record[0] == uid.into() {
                users_followed.insert(record[1].clone()); 
            }
        }

        // User-specific timeline
        let mut res3 = getter3.lookup(&[0.into()], true).await.unwrap(); // ALl tweets
        
        if res3.len() == 0 {
            let ten_millis = time::Duration::from_millis(20000);
            thread::sleep(ten_millis);
            assert!(res3.len() > 0); 
        }

        for record in res3.iter() { // Get all followed tweets 
            // println!("followed: {:?}", users_followed); 
            // println!("checking if {:?} is in followed or is self", record[3]); 
            if users_followed.contains(&record[3]) || record[3] == 0.into(){
                visible_tweets.push(record); 
            }
        }

        // Remove blocked tweets from all public tweets, union with followed tweets 
     
        let mut visible_tweets_no_blocked = Vec::new(); 
        for record in visible_tweets.iter() {
            if !blocked_user.contains(&record[0]) {
                visible_tweets_no_blocked.push(record); 
            }
        }

        let dur = start.elapsed().as_secs_f64();

        // let read_lat_str = format!(
        //     "Read {} keys in {:.2}s ({:.2} GETs/sec)!",
        //     visible_tweets.len(),
        //     dur,
        //     (visible_tweets.len() as f64) / dur,
        // );

        let read_lat_str = format!("{:?}", dur); 
         
        // println!("len visible: {:?}", visible_tweets.len()); 

        memstats(&mut backend.g, materialization.clone(), nusers, ntweets, Some(read_lat_str)).await;
        
    } else if materialization == "full".to_string() || materialization == "partial".to_string() {
        
        let mut dur = time::Duration::from_millis(0); 
        let mut total_num_keys_read = 0; 
        for user in 0..nusers {
            let leaf = format!("AllExcludingBlockedByFinal_{:?}", user);
            let mut getter = backend.g.view(&leaf).await.unwrap();
            let start = time::Instant::now();
            let mut res = getter.lookup(&[0.into()], true).await.unwrap();
            dur += start.elapsed();
            total_num_keys_read += res.len(); 
        }   

        let dur = dur.as_secs_f64(); 

        let read_lat_str = format!(
            "Read {} keys in {:.2}s ({:.2} GETs/sec)!",
            total_num_keys_read,
            dur,
            (total_num_keys_read as f64) / dur,
        );
    
        memstats(&mut backend.g, materialization.clone(), nusers, ntweets, Some(read_lat_str)).await;
    } else {
        if materialization == "baseline" {
            println!("baselines!");
            let mut dur = time::Duration::from_millis(0); 
            let mut total_num_keys_read = 0; 
            for user in 0..nusers {
                let leaf = format!("FollowedTweets_{:?}", user);
                let mut getter = backend.g.view(&leaf).await.unwrap();
                let start = time::Instant::now();
                let mut res = getter.lookup(&[0.into()], true).await.unwrap();
                dur += start.elapsed();
                total_num_keys_read += res.len(); 
            }   
    
            let dur = dur.as_secs_f64(); 
    
            let read_lat_str = format!(
                "Read {} keys in {:.2}s ({:.2} GETs/sec)!",
                total_num_keys_read,
                dur,
                (total_num_keys_read as f64) / dur,
            );
        
            memstats(&mut backend.g, materialization.clone(), nusers, ntweets, Some(read_lat_str)).await;
        } else { 
            // Shallow server. Same as baseline, but compute policies (blocked/blocked by) on the fly. 
            let mut dur = time::Duration::from_millis(0); 
            let mut total_num_keys_read = 0; 
            for user in 0..nusers {
                let leaf = format!("FollowedTweets_{:?}", user);
                let mut getter = backend.g.view(&leaf).await.unwrap();

                let leaf = format!("Blocked");
                let mut blocked_user = HashSet::new(); 
            
                let mut getter1 = backend.g.view(&leaf).await.unwrap();
                let mut res1= getter1.lookup(&[0.into()], true).await.unwrap();

                // Blocked accounts and blocked by accounts: 
                for record in res1.iter() {
                    if record[0] == user.into() {
                        blocked_user.insert(record[1].clone()); 
                    }
                }
                
                let mut visible_tweets = Vec::new(); 
                let start = time::Instant::now();
                let mut res = getter.lookup(&[0.into()], true).await.unwrap();
                for record in res.iter() {
                    if !blocked_user.contains(&record[0]) {
                        visible_tweets.push(record); 
                    }
                }
                dur += start.elapsed();
                total_num_keys_read += visible_tweets.len(); 

            }   
    
            let dur = dur.as_secs_f64(); 
    
            let read_lat_str = format!(
                "Read {} keys in {:.2}s ({:.2} GETs/sec)!",
                total_num_keys_read,
                dur,
                (total_num_keys_read as f64) / dur,
            );
        
            memstats(&mut backend.g, materialization.clone(), nusers, ntweets, Some(read_lat_str)).await;
        } 
    }

}


async fn it_works_client_side() {
    let verbose = false; 
    let partial = false; 
    let nusers = 3 as usize; 
    let ntweets = 4 as usize; 
    let private = 0.0 as f64;
    let materialization = "client".to_string(); 

    // Construct client side graph
    let mut backend = Backend::make(verbose, partial).await;
    construct_graph(&mut backend, verbose, partial, nusers, ntweets, private, materialization).await; 

    // Populate tables 
    let nusers = 3; 
    let mut users = Vec::new(); 
    let mut tweets = Vec::new(); 
    let mut follows = Vec::new(); 
    let mut blocks = Vec::new(); 

    // Add users to the system
    for i in 0..nusers {
        let mut name = "name".to_string(); 
        let mut handle : String = format!("user{}", i); 
        let mut bday_month: usize = 0;
        let mut bday_day: usize = 1; 
        let mut bday_year: usize = 2; 
        let mut password: String = "password".to_string(); 
        let mut email: String = "email".to_string(); 
                
        let mut new_record : Vec<DataType> = vec![
                                                i.into(),
                                                name.into(), 
                                                0.into(), 
                                                bday_month.into(), 
                                                bday_day.into(), 
                                                bday_year.into(), 
                                                email.into(), 
                                                password.into(), 
                                                ]; 
        users.push(new_record); 
    }

    // Add follows/blocks --- user 0 follows user 2 
    let mut rec = Vec::new(); 
    rec.push((0 as i32).into()); 
    rec.push((2 as i32).into()); 
    follows.push(rec.clone()); 
    
    // Add follows/blocks --- user 0 blocks user 1
    let mut rec = Vec::new(); 
    rec.push((0 as i32).into()); 
    rec.push((1 as i32).into()); 
    blocks.push(rec.clone()); 


    // Add tweets --- user 2 tweets twice, user 1 tweets once, user 0 tweets once 
    let mut new_record: Vec<DataType> = vec![
        2.into(), 
        1.into(),
        "x".into(),
        0.into(), 
        0.into(), 
        0.into()
    ]; 

    let mut new_record2: Vec<DataType> = vec![
        2.into(), 
        2.into(),
        "x2".into(),
        0.into(), 
        0.into(), 
        0.into()
    ]; 

    let mut new_record3: Vec<DataType> = vec![
        1.into(), 
        3.into(),
        "y".into(),
        0.into(), 
        1.into(), 
        0.into()
    ]; 

    let mut new_record4: Vec<DataType> = vec![
        0.into(), 
        4.into(),
        "z".into(),
        0.into(), 
        0.into(), 
        0.into()
    ]; 
    
    tweets.push(new_record);
    tweets.push(new_record2); 
    tweets.push(new_record3); 
    tweets.push(new_record4); 

    backend.populate("Users", users.clone()).await;
    backend.populate("Follows", follows.clone()).await; 
    backend.populate("BlockedAccounts", blocks.clone()).await; 
    backend.populate("Tweets", tweets.clone()).await; 

    thread::sleep(Duration::from_millis(10000));

    let uid = 0; 

    // Do client side reads from user 0's perspective  
    let leaf = format!("Blocked");
    let mut getter = backend.g.view(&leaf).await.unwrap();
    let mut res = getter.lookup(&[0.into()], true).await.unwrap();
    
    // Blocked accounts and blocked by accounts: 
    let mut blocked_user = HashSet::new(); 
    for record in res.iter() {
        if record[0] == uid.into() {
            blocked_user.insert(record[1].clone()); 
        }
    }

    // Followed 
    let leaf = format!("Follows");
    let mut getter = backend.g.view(&leaf).await.unwrap();
    let mut res = getter.lookup(&[0.into()], true).await.unwrap();
    let mut users_followed = HashSet::new();

    println!("follows: {:?}", res);
    for record in res.iter() {
        println!("follows {:?}", record); 
        if record[0] == uid.into() {
            println!("adding to follows");
            users_followed.insert(record[1].clone()); 
        }
    }

    // User-specific timeline
    let leaf = format!("AllTweetsWithUserInfo");
    let mut getter = backend.g.view(&leaf).await.unwrap();
    let mut res = getter.lookup(&[0.into()], true).await.unwrap();
    let mut visible_tweets = Vec::new(); 

    assert!(res.len() == 4); 

    for record in res.iter() {
        if users_followed.contains(&record[3]) || record[3] == 0.into(){
            visible_tweets.push(record); 
        }
    }

    // Remove blocked tweets from all public tweets, union with followed tweets 
    // let leaf = format!("PublicTweets");
    // let mut getter = backend.g.view(&leaf).await.unwrap();
    // let mut res = getter.lookup(&[0.into()], true).await.unwrap();

    // for record in res.iter() {
    //     if !blocked_user.contains(&record[0]) {
    //         visible_tweets.push(record); 
    //     }
    // }

    assert!(visible_tweets.len() == 3); 
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
                .default_value("client")
                .possible_values(&["full", "partial", "shallow-readers", "shallow-all", "client", "baseline", "shallow-server"])
                .help("Set materialization strategy for the benchmark"),
        )
        .arg(
            Arg::with_name("ntweets")
                .short("n")
                .takes_value(true)
                .default_value("1000")
        )
        .arg(
            Arg::with_name("nusers")
                .short("u")
                .takes_value(true)
                .default_value("100")
        )
        .arg(
            Arg::with_name("percent-private")
                .short("p")
                .default_value("0.3")
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
        .arg(
            Arg::with_name("full-suite")
                .short("f")
                .multiple(true)
                .help("Run full experimental suite"),
        )
        .get_matches();

    let verbose = args.is_present("verbose");
    let full_suite = args.is_present("full-suite");

    let nusers = value_t_or_exit!(args, "nusers", usize); 
    let ntweets = value_t_or_exit!(args, "ntweets", usize); 
    let private = value_t_or_exit!(args, "percent-private", f64); 
    let materialization = args.value_of("materialization").unwrap_or("full").to_string();

    let mut partial = false; 
    if materialization == "partial".to_string() {
        partial = true; 
    } 
   
    let mut experiment_materialization_config = Vec::new(); 
    let mut experiment_ntweets_nusers_config = Vec::new(); 
    
    if full_suite {
        // experiment_materialization_config.push("full".to_string()); 
        experiment_materialization_config.push("client".to_string());     
        experiment_ntweets_nusers_config.push((1000 as usize, 100 as usize)); 
        experiment_ntweets_nusers_config.push((10000 as usize, 1000 as usize));
        experiment_ntweets_nusers_config.push((100000 as usize, 10000 as usize)); 
        experiment_ntweets_nusers_config.push((10000 as usize, 10000 as usize)); 
        experiment_ntweets_nusers_config.push((10000 as usize, 100 as usize)); 
    } else {
        experiment_materialization_config.push(materialization.clone()); 
        experiment_ntweets_nusers_config.push((ntweets, nusers)); 
    } 

    if materialization == "baseline" || materialization == "shallow-server" { 
        let partial = false; 
        let mut backend = Backend::make(verbose, partial).await;
        for (ntweets, nuse) in  experiment_ntweets_nusers_config.iter() {
            for mat in experiment_materialization_config.iter() {
                construct_baseline_graph(&mut backend, verbose, partial, *nuse, *ntweets, private, "baseline".to_string()).await; 
                run_experiment(&mut backend, verbose, partial, *nuse, *ntweets, private, mat.clone()).await;
            }                   
        }
    } else {
        let mut backend = Backend::make(verbose, partial).await;
        for (ntweets, nuse) in  experiment_ntweets_nusers_config.iter() {
            for mat in experiment_materialization_config.iter() {
                construct_graph(&mut backend, verbose, partial, *nuse, *ntweets, private, mat.clone()).await; 
                run_experiment(&mut backend, verbose, partial, *nuse, *ntweets, private, mat.clone()).await;
            }                   
        }
    }

}
