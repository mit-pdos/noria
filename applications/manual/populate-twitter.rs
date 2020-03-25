use noria::DataType;
use rand::prelude::*;
use std::collections::HashMap;


pub struct Populate {
    nusers: usize,
    ntweets: usize,
    private: f32,
    logged_in: f32, 
    users: HashMap<usize, Vec<DataType>>, 
    tweets: Vec<DataType>,
    follows: HashMap<usize, Vec<usize>, 
    blocks: HashMap<usize, Vec<usize>, 
    rng: ThreadRng,
}


impl Populate {
    pub fn new(nusers: usize, ntweets: usize, private: f32) -> Populate {
        Populate {
            nusers,
            ntweets,
            private,
            HashMap::new(), 
            Vec::new(), 
            follows: HashMap::new(),
            blocks: HashMap::new(),
            rng: rand::thread_rng(),
        }
    }

    pub fn get_users() -> Vec<Vec<DataType>> {
        let mut user_records = Vec::new(); 
        for i in 0..self.nusers {
            let mut name = rand::thread_rng()
                                .gen_ascii_chars()
                                .take(20)
                                .collect::<String>();
            
            let mut handle = rand::thread_rng()
                                .gen_ascii_chars()
                                .take(10)
                                .collect::<String>();
                                
            let mut bday_month: usize = task_rng().gen_range(0, 13);
            let mut bday_day: usize = task_rng().gen_range(0, 32); 
            let mut bday_year: usize = task_rng().gen_range(1940, 2020); 
        
            let mut password = rand::thread_rng()
                                .gen_ascii_chars()
                                .take(10)
                                .collect::<String>();
            
            let mut email = rand::thread_rng()
                                .gen_ascii_chars()
                                .take(10)
                                .collect::<String>();
                                
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

    pub fn get_follows() -> Vec<Vec<DataType>> {
        for i in 0..self.nusers {
            let mut num_to_follow: usize = task_rng().gen_range(0, 1000);
            let mut following = Vec::new(); 
            for j in 0..num_to_follow {
                let mut follow: usize = task_rng().gen_range(0, self.nusers);
                following.push(follow.into()); 
            }
            self.follows.insert(i, following.clone()); 
        }
        return self.follows; 
    }

    pub fn get_blocks() -> Vec<Vec<DataType>> {
        for i in 0..self.nusers {
            let mut num_to_follow: usize = task_rng().gen_range(0, 10);
            let mut following = Vec::new(); 
            for j in 0..num_to_follow {
                let mut follow: usize = task_rng().gen_range(0, self.nusers);
                following.push(follow.into()); 
            }
            self.blocks.insert(i, following.clone()); 
        }
        return self.blocks; 
    }

    pub fn get_tweets() -> Vec<Vec<DataType>> {
        let mut tweets = Vec::new(); 
        for i in 0..self.ntweets {
            let mut user_who_tweeted = task_rng().gen_range(0, self.nusers);
            let mut content = rand::thread_rng()
                                    .gen_ascii_chars()
                                    .take(10)
                                    .collect::<String>();
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


