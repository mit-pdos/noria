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


use crate::utils::{memstats, Backend}; 
use crate::graphs::{construct_baseline_graph, construct_graph}; 


pub async fn it_works_full_mat() { 
    let verbose = false; 
    let partial = false; 
    let nusers = 3 as usize; 
    let ntweets = 4 as usize; 
    let private = 0.0 as f64;
    let materialization = "full".to_string(); 

    // Construct graph 
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

    thread::sleep(Duration::from_millis(20000));

    let uid = 0; 

    let leaf = format!("UsersYouFollow_0");
    let mut getter = backend.g.view(&leaf).await.unwrap();
    let mut res = getter.lookup(&[0.into()], true).await.unwrap();
    println!("Users that user 0 follows: {:?}", res); 

    let leaf = format!("TweetsWithUserInfo");
    let mut getter = backend.g.view(&leaf).await.unwrap();
    let mut res = getter.lookup(&[0.into()], true).await.unwrap();
    println!("All tweets: {:?}", res); 

    let leaf = format!("AllExcludingBlockedByFinal_0");
    let mut getter = backend.g.view(&leaf).await.unwrap();
    let mut res = getter.lookup(&[0.into()], true).await.unwrap();
    println!("Followed tweets: {:?}", res); 
    // let mut visible_tweets = Vec::new(); 

    // assert!(res.len() == 4); 
  
}


pub async fn it_works_client_side() {
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
    println!("read {:?} keys", res.len()); 
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
