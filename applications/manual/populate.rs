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
                bday_month.into(), 
                bday_day.into(), 
                bday_year.into(), 
                handle.into(), 
                password.into(), 
                email.into(), 
                ]; 

            self.users.insert(i, new_record.clone()); 

            user_records.push(new_record.clone()); 
        }

        return user_records
    }

    pub fn get_follows(&mut self) -> Vec<Vec<DataType>> {
        let mut all_follows = Vec::new(); 
        for i in 0..self.nusers {

            let mut num_to_follow: usize = thread_rng().gen_range(0, 1000);
            let mut following : Vec<usize> = Vec::new(); 
            for j in 0..num_to_follow {
                let mut follow: usize = thread_rng().gen_range(0, self.nusers);
                following.push(follow.into()); 
            }
            for fllw in following.iter() {
                let mut rec = Vec::new(); 
                rec.push(i.into()); 
                rec.push(fllw.clone().into()); 
                all_follows.push(rec.clone()); 
            }
 
            self.follows.insert(i, following.clone()); 
        }
        return all_follows; 
    }

    pub fn get_blocks(&mut self) -> Vec<Vec<DataType>> {
        let mut all_blocks = Vec::new(); 
        for i in 0..self.nusers {
            let mut num_to_follow: usize = thread_rng().gen_range(0, 10);
            let mut following : Vec<usize> = Vec::new(); 
            for j in 0..num_to_follow {
                let mut follow: usize = thread_rng().gen_range(0, self.nusers);
                following.push(follow.into()); 
            }

            for fllw in following.iter() {
                let mut rec = Vec::new(); 
                rec.push(i.into()); 
                rec.push(fllw.clone().into()); 
                all_blocks.push(rec.clone()); 
            }

            self.blocks.insert(i, following.clone()); 
        }
        return all_blocks; 
    }

    pub fn get_tweets(&mut self) -> Vec<Vec<DataType>> {
        let mut tweets = Vec::new(); 
        for i in 0..self.ntweets {
            let mut user_who_tweeted = thread_rng().gen_range(0, self.nusers);
            let mut content : String = thread_rng()
                            .sample_iter(&Alphanumeric)
                            .take(50)
                            .collect();
            let mut new_record : Vec<DataType> = vec![
                i.into(),
                user_who_tweeted.into(), 
                content.into(), 
                ]; 
            tweets.push(new_record.clone()); 
        }
        self.tweets = tweets.clone(); 
        return tweets; 
    }

}


