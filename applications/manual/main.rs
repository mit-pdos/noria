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
mod utils; 
mod tests; 
mod graphs; 

use crate::populate::Populate;
use crate::utils::{Backend, memstats};
use crate::tests::{it_works_full_mat, it_works_client_side}; 
use crate::graphs::{construct_baseline_graph, construct_graph}; 


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
        
        println!("read {:?} keys", res3.len());  
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

        // memstats(&mut backend.g, materialization.clone(), nusers, ntweets, Some(read_lat_str)).await;
        
    } else if materialization == "full".to_string() || materialization == "partial".to_string() {
        
        let mut dur = time::Duration::from_millis(0); 
        let mut total_num_keys_read = 0; 
        for user in 0..nusers {
            let leaf = format!("AllExcludingBlockedByFinal_{:?}", user);
            let mut getter = backend.g.view(&leaf).await.unwrap();
            let start = time::Instant::now();
            let mut res = getter.lookup(&[0.into()], true).await.unwrap();
            if res.len() == 0 {
                let ten_millis = time::Duration::from_millis(20000);
                thread::sleep(ten_millis);
                assert!(res.len() > 0); 
            }
            println!("read {:?} keys", res.len());  
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
    
        // memstats(&mut backend.g, materialization.clone(), nusers, ntweets, Some(read_lat_str)).await;
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
                if res.len() == 0 {
                    let ten_millis = time::Duration::from_millis(20000);
                    thread::sleep(ten_millis);
                    assert!(res.len() > 0); 
                }
                println!("read {:?} keys", res.len());  
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
        
            // memstats(&mut backend.g, materialization.clone(), nusers, ntweets, Some(read_lat_str)).await;
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
                println!("read {:?} keys", res.len()); 
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
        
            // memstats(&mut backend.g, materialization.clone(), nusers, ntweets, Some(read_lat_str)).await;
        } 
    }

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
                // construct_graph(&mut backend, verbose, partial, *nuse, *ntweets, private, mat.clone()).await; 
                it_works_full_mat().await; 
                // run_experiment(&mut backend, verbose, partial, *nuse, *ntweets, private, mat.clone()).await;
            }                   
        }
    }

}
