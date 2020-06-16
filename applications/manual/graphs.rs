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

use crate::utils::Backend;

pub async fn construct_graph(backend: &mut Box<Backend>, verbose: bool, partial: bool, nusers: usize, ntweets: usize, private: f64, materialization: String) {
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
            
            // let tweets_with_user_info2 = mig.add_ingredient("TweetsWithUserInfo2", 
            //                                                 &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "bogo"], 
            //                                                 Join::new(tweets, users, JoinType::Inner, vec![B(0, 0), L(1), L(2), L(3), L(4), R(1), R(2), L(5)])); 
            
            // let prelim = mig.maintain_anonymous(tweets_with_user_info, &[7]); 

            // let all_tweets = mig.add_ingredient("AllTweetsWithUserInfo",
            //                                 &["id", "time", "content", "userId", "name", 
            //                                   "retweetId", "retweetTime", "retweetContent", 
            //                                   "retweetUserId", "retweetName", "isPrivate", "bogo"],
            //                                 Join::new(tweets_with_user_info,
            //                                           tweets_with_user_info2,
            //                                           JoinType::Left,
            //                                           vec![L(1), L(3), L(2), L(0), L(5), B(4, 1), R(3), R(2), R(0), R(5), R(6), L(7)])); 

            let mut ri = mig.maintain_anonymous(tweets_with_user_info, &[7]);    
// 
            // (all_tweets)
            (tweets_with_user_info) 
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
                

                                                        // &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "bogo"],
                
                let visible_tweets2 = mig.add_ingredient(format!("FollowedTweets_{:?}", user), &["userId", "id", "content", "time",
                                                        "retweetId", "name", "isPrivate", "bogo"], 
                                                        Join::new(users_you_follow, all_tweets, 
                                                        JoinType::Inner, vec![B(1, 0), R(1), R(2), R(3), R(4), R(5), R(6), R(7)])); 

                let r2 = mig.maintain_anonymous(users_you_follow, &[0]);
                // let visible_tweets2 = mig.add_ingredient(format!("FollowedTweets_{:?}", user), &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "bogo"], 
                //                                         Join::new(users_you_follow, all_tweets, 
                //                                         JoinType::Inner, vec![B(0, 3), R(1), R(2), R(3), R(4), R(5), R(6), R(7)])); 
           
                // let mut emits = HashMap::new(); 
                // emits.insert(visible_tweets1, vec![0, 1, 2, 3, 4, 5, 6]);
                // emits.insert(visible_tweets2, vec![0, 1, 2, 3, 4, 5, 6]);
        
                // let visible_tweets3 = mig.add_ingredient(format!("PublicAndFollowedTweets_{:?}", user), &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "bogo"],
                //                                         Union::new(emits)); 
        
        
                // let visible_tweets4a = mig.add_ingredient(format!("AllExcludingBlocked_{:?}", user), &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "Id", "bogo"], 
                //                                         Join::new(visible_tweets2, blocked_accounts,
                //                                         JoinType::Left, vec![B(0, 1), L(1), L(2), L(3), L(4), L(5), L(6), R(0), L(7)])); 
                                                    
        
                // let visible_tweets4 = mig.add_ingredient(format!("AllExcludingBlockedFinal_{:?}", user), &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "Id", "bogo"], 
                //                                         Filter::new(visible_tweets4a, &[(7, FilterCondition::Comparison(Operator::Equal, Value::Constant(DataType::None)))]));  
            
            
                // let visible_tweets5a = mig.add_ingredient(format!("AllExcludingBlockedBy_{:?}", user), &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "Id", "bogo"], 
                //                                         Join::new(visible_tweets4, blocked_by_accounts,
                //                                         JoinType::Left, vec![B(0, 1), L(1), L(2), L(3), L(4), L(5), L(6), R(0), L(7)])); 
                
                // let visible_tweets5 = mig.add_ingredient(format!("AllExcludingBlockedByFinal_{:?}", user), &["userId", "id", "content", "time", "retweetId", "name", "isPrivate", "Id", "bogo"], 
                //                                         Filter::new(visible_tweets5a, &[(7, FilterCondition::Comparison(Operator::Equal, Value::Constant(DataType::None)))]));  
            
                // let ri = mig.maintain_anonymous(visible_tweets5, &[7]);
            }).await;

        }
    }
    let mut file = File::create(format!("{}mat-{}nusers.dot", materialization, nusers)).unwrap();
    file.write_all(backend.g.graphviz().await.unwrap().as_bytes());
}


pub async fn construct_baseline_graph(backend: &mut Box<Backend>, verbose: bool, partial: bool, nusers: usize, ntweets: usize, private: f64, materialization: String) {
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
