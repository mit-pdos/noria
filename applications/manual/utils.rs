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


pub struct Backend {
    pub g: Handle<LocalAuthority>,
    pub done: Box<dyn Future<Output = ()> + Unpin>,
}

impl Backend {
    pub async fn make(verbose: bool, partial: bool) -> Box<Backend> {
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
pub async fn memstats(g: &mut noria::Handle<LocalAuthority>, materialization: String, nusers: usize, ntweets: usize, read_latencies: Option<String>) {
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
