use noria::DataType;
use rand::prelude::*;
use std::collections::HashMap;

extern crate rand;

use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;


pub struct Populate {
    nusers: usize,
    ntweets: usize,
    private: f64,
    users: HashMap<usize, Vec<DataType>>, 
    tweets: Vec<Vec<DataType>>,
    follows: HashMap<usize, Vec<usize>>, 
    blocks: HashMap<usize, Vec<usize>>, 
    rng: ThreadRng,
}


impl Populate {
    pub fn new(nusers: usize, ntweets: usize, private: f64) -> Populate {
        Populate {
            nusers,
            ntweets,
            private,
            users: HashMap::new(), 
            tweets: Vec::new(), 
            follows: HashMap::new(),
            blocks: HashMap::new(),
            rng: rand::thread_rng(),
        }
    }

    pub fn get_users(&mut self) -> Vec<Vec<DataType>> {
        // ["userId", "name", "isPrivate", "birthdayMonth", "birthdayDay", "birthdayYear", "email", "password"]

        let mut user_records = Vec::new(); 
        for i in 0..self.nusers {
            let mut name : String = thread_rng()
                            .sample_iter(&Alphanumeric)
                            .take(20)
                            .collect();
            
            let mut handle : String = thread_rng()
                            .sample_iter(&Alphanumeric)
                            .take(20)
                            .collect();
                    
            let mut bday_month: usize = thread_rng().gen_range(0, 13);
            let mut bday_day: usize = thread_rng().gen_range(0, 32); 
            let mut bday_year: usize = thread_rng().gen_range(1940, 2020); 
        
            let mut password : String = thread_rng()
                                .sample_iter(&Alphanumeric)
                                .take(20)
                                .collect();
            
            let mut email : String = thread_rng()
                            .sample_iter(&Alphanumeric)
                            .take(20)
                            .collect();
                            
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

            self.users.insert(i, new_record.clone()); 

            user_records.push(new_record.clone()); 
        }

        return user_records
    }

    pub fn get_follows(&mut self) -> Vec<Vec<DataType>> {
        // let mut follower_stats = Vec::new(); 
        // let total = 500 + 1000 + 5000 + 10000 + 20000; 
        // follower_stats.push(((((500 as f64) / (total as f64) * (self.nusers as f64)) as usize), (0.9584 as f64)));
        // follower_stats.push(((((1000 as f64) / (total as f64) * (self.nusers as f64)) as usize), (0.0214 as f64))); 
        // follower_stats.push(((((5000 as f64) / (total as f64) * (self.nusers as f64)) as usize), (0.0172 as f64))); 
        // follower_stats.push(((((10000 as f64) / (total as f64) * (self.nusers as f64)) as usize), (0.0016 as f64))); 
        // follower_stats.push(((((20000 as f64) / (total as f64) * (self.nusers as f64)) as usize), (0.0005 as f64))); 
    
        // println!("scaled follower stats: {:?}", follower_stats);

        // let mut barriers = Vec::new(); 
        // let mut prior_barrier = 0.0;
        // for (follower_count, percentage) in follower_stats.iter() {
        //     let barrier = (self.nusers as f64) * (percentage.clone() as f64); 
        //     let interval = follower_count; 
        //     barriers.push(((prior_barrier, barrier + prior_barrier), interval)); 
        //     prior_barrier = barrier; 
        // }
        
        // println!("barriers: {:?}", barriers);
        let mut all_follows = Vec::new();

        let mut ind = 0;
        let mut lower_bound = 0;
        for i in 0..self.nusers {
            // let (mut proposed_interval, mut upper_bound) = barriers[ind]; 
            // let (mut proposed_start, mut proposed_end) = proposed_interval; 
            
            // if (i as f64) > proposed_end {
            //     ind += 1; 
            //     lower_bound = *upper_bound; 
            //     proposed_interval = barriers[ind].0; 
            //     upper_bound = barriers[ind].1; 
            //     proposed_start = proposed_interval.0; 
            //     proposed_end = proposed_interval.1; 
            // }

            let mut rng = rand::thread_rng();
            let mut n1 = rng.gen::<f64>(); 
            let mut upper_bound; 

            if n1 > 0.95 {
                upper_bound = 2000; 
            } else {
                upper_bound = 200; 
            }

            let mut num_to_follow: usize = thread_rng().gen_range(lower_bound, upper_bound) as usize;
            let mut following: Vec<usize> = Vec::new(); 

            // println!("user {} follows {} others", i, num_to_follow);

            for j in 0..num_to_follow {
                let mut follow: usize = thread_rng().gen_range(0, self.nusers);
                following.push(follow.into()); 
            }

            for fllw in following.iter() {
                let mut rec = Vec::new(); 
                rec.push((i as i32).into()); 
                rec.push((fllw.clone() as i32).into()); 
                all_follows.push(rec.clone()); 
            }
 
            self.follows.insert(i, following.clone()); 
        }
    
        return all_follows; 
    }

    pub fn get_blocks(&mut self) -> Vec<Vec<DataType>> {
        let mut all_follows = Vec::new();

        for i in 0..self.nusers {
            // let mut num_to_follow: usize = thread_rng().gen_range(0, self.nusers);
            let mut num_to_follow: usize = 2; 

            let mut following : Vec<usize> = Vec::new(); 
            
            for j in 0..num_to_follow {
                let mut follow: usize = thread_rng().gen_range(0, self.nusers);
                following.push(follow.into()); 
            }

            for fllw in following.iter() {
                let mut rec = Vec::new(); 
                rec.push((i as i32).into()); 
                rec.push((fllw.clone() as i32).into()); 
                all_follows.push(rec.clone()); 
            }
 
            self.blocks.insert(i, following.clone()); 
        }
    
        return all_follows; 
    }

    pub fn get_tweets(&mut self) -> Vec<Vec<DataType>> {
        // &["userId", "id", "content", "time", "retweetId", "bogo"]
        let mut tweets = Vec::new();
        println!("num tweets: {:?}", self.ntweets); 
        for i in 0..self.ntweets {
            let mut user_who_tweeted = thread_rng().gen_range(0, self.nusers);
            let mut content : String = thread_rng()
                            .sample_iter(&Alphanumeric)
                            .take(50)
                            .collect();
            let mut rt : usize = thread_rng().gen_range(0, self.ntweets*2);
            let mut retweet_id : usize; 

            if rt > self.ntweets {
                retweet_id = i; 
            } else {
                retweet_id = rt; 
            }
            let mut new_record : Vec<DataType> = vec![
                user_who_tweeted.into(), 
                i.into(),
                content.into(),
                0.into(), 
                retweet_id.into(), 
                0.into()
            ]; 
            tweets.push(new_record.clone()); 
        }
        self.tweets = tweets.clone(); 
        return tweets; 
    }

}


