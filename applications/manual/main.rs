extern crate csv;
use csv::Writer;
use clap::value_t_or_exit;
use hdrhistogram::Histogram;
use noria::{FrontierStrategy, ReuseConfigType};
use rand::seq::SliceRandom;
use slog::{crit, debug, error, info, o, trace, warn, Logger};
use std::collections::{HashMap, HashSet};
use std::time::{Instant, Duration};
use std::thread;
use std::error::Error;
use noria::{DurabilityMode, PersistenceParameters, DataType};
use noria::manual::Base;
use noria::manual::Reader;
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


const PAPERS_PER_REVIEWER: usize = 3;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Ord, PartialOrd)]
enum Operation {
    ReadPaperList,
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Operation::ReadPaperList => write!(f, "plist"),
        }
    }
}

struct Paper {
    accepted: bool,
    title: String,
    authors: Vec<usize>,
}

#[derive(Debug)]
struct Review {
    paper: usize,
    rating: usize,
    confidence: usize,
    
}


pub struct Backend {
    g: Handle<LocalAuthority>,
    done: Box<dyn Future<Output = ()> + Unpin>,
}


impl Backend {
    async fn make() -> Box<Backend> {
        let mut b = Builder::default();
      
        b.set_sharding(None);
        b.disable_partial(); 
        b.set_persistence(PersistenceParameters::new(
            DurabilityMode::MemoryOnly,
            Duration::from_millis(1),
            Some(String::from("manual_policy_graph")),
            1,
        ));
        
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
 async fn memstats(g: &mut noria::Handle<LocalAuthority>, at: &str, mut wtr: Writer<std::fs::File>, loggedf: f64, npapers: usize, nauthors: usize, nreviewers: usize) {
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
    for (_, nstats) in stats.values() {
        for nstat in nstats.values() {
            println!("[{}] {}: {:?}", at, nstat.desc, nstat.mem_size);
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
    wtr.write_record(&[format!("{}", loggedf),
                        format!("{}", npapers),
                        format!("{}", nauthors),
                        format!("{}", nreviewers),
                        format!("{}", at),
                        format!("{}", base_mem),
                        format!("{}", reader_mem),
                        format!("{}", mem)]);
    println!("# base memory @ {}: {}", at, base_mem);
    println!("# reader memory @ {}: {}", at, reader_mem);
    println!("# materialization memory @ {}: {} (filters: {})", at, mem, filter_mem);
}


#[tokio::main]
async fn main() {
    use clap::{App, Arg};
    let args = App::new("manual-graph")
        .version("0.1")
        .about("Benchmarks HotCRP-like application with security policies.")
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
            Arg::with_name("source")
                .long("source")
                .default_value("https://openreview.net/group?id=ICLR.cc/2018/Conference")
                .takes_value(true)
                .help("Source to pull paper data from"),
        )
        .arg(
            Arg::with_name("npapers")
                .short("n")
                .takes_value(true)
                .default_value("10000")
                .help("Only fetch first n papers"),
        )
        .arg(
            Arg::with_name("logged-in")
                .short("l")
                .default_value("1.0")
                .help("Fraction of authors & reviewers that are logged in."),
        )
        .arg(
            Arg::with_name("iter")
                .long("iter")
                .default_value("1")
                .help("Number of iterations to run"),
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
    let verbose = args.occurrences_of("verbose");
    let loggedf = value_t_or_exit!(args, "logged-in", f64);
    let source = value_t_or_exit!(args, "source", url::Url);

    assert!(loggedf >= 0.0);
    assert!(loggedf <= 1.0);

    let log = if verbose != 0 {
        noria::logger_pls()
    } else {
        Logger::root(slog::Discard, o!())
    };
    let mut rng = rand::thread_rng();


    let conf = source
        .query_pairs()
        .find(|(arg, _)| arg == "id")
        .expect("could not find conference id in url")
        .1;
    info!(log, "fetching source data"; "conf" => &*conf);
    // https://openreview.net/api/#/Notes/findNotes
    let url = format!(
        "https://openreview.net/notes?invitation={}%2F-%2FBlind_Submission&limit={}",
        url::percent_encoding::utf8_percent_encode(
            &*conf,
            url::percent_encoding::DEFAULT_ENCODE_SET
        ),
        value_t_or_exit!(args, "npapers", usize),
    );
    debug!(log, "sending request for paper list"; "url" => &url);
    let all = reqwest::get(&url)
        .expect("failed to fetch source")
        .json::<serde_json::Value>()
        .expect("invalid source json");
    let all = all
        .as_object()
        .expect("root of source data is not a json object as expected");
    let all = all
        .get("notes")
        .and_then(|a| a.as_array())
        .expect("source data has a weird structure");
    let url = format!(
        "https://openreview.net/notes?invitation={}%2F-%2FAcceptance_Decision&limit=10000",
        url::percent_encoding::utf8_percent_encode(
            &*conf,
            url::percent_encoding::DEFAULT_ENCODE_SET
        )
    );


    debug!(log, "fetching list of accepted papers"; "url" => &url);
    let accept = reqwest::get(&url)
        .expect("failed to fetch accepted list")
        .json::<serde_json::Value>()
        .expect("invalid acceptance list json");
    let accept = accept
        .as_object()
        .expect("root of acceptance list is not a json object as expected");
    let accept = accept
        .get("notes")
        .and_then(|a| a.as_array())
        .expect("acceptance list data has a weird structure");
    let mut accepted = HashSet::new();
    for decision in accept {
        let decision = decision.as_object().expect("acceptance info is weird");
        let id = decision["forum"]
            .as_str()
            .expect("listed acceptance forum is not a string");
        let was_accepted = decision["content"]
            .as_object()
            .expect("acceptance info content is weird")["decision"]
            .as_str()
            .expect("listed acceptance decision is not a string")
            .starts_with("Accept");
        if was_accepted {
            trace!(log, "noted acceptance decision"; "paper" => id);
            accepted.insert(id);
        }
    }

    let mut author_set = HashMap::new();
    let mut authors = Vec::new();
    let mut papers = Vec::new();
    let mut reviews = Vec::new();
    debug!(log, "processing paper list"; "n" => all.len());
    for paper in all {
        let paper = paper.as_object().expect("paper info isn't a json object");
        let id = paper["id"].as_str().expect("paper id is weird");
        let number = paper["number"].as_u64().expect("paper number is weird");
        let content = paper["content"]
            .as_object()
            .expect("paper info doesn't have content");
        let title = content["title"].as_str().unwrap().to_string();
        let authors: Vec<_> = content["authorids"]
            .as_array()
            .expect("author list is not an array")
            .iter()
            .map(|author| {
                let author = author.as_str().expect("author id is not a string");
                *author_set.entry(author.to_string()).or_insert_with(|| {
                    trace!(log, "adding author"; "name" => author, "uid" => author.len() + 1);
                    authors.push(author);
                    authors.len() - 1
                })
            })
            .collect();


        let pid = papers.len() + 1;
        trace!(log, "adding paper"; "title" => &title, "id" => pid, "accepted" => accepted.contains(id));
        papers.push(Paper {
            title,
            accepted: accepted.contains(id),
            authors,
        });

        let url = format!(
            "https://openreview.net/notes?forum={}&invitation={}/-/Paper{}/Official_Review",
            url::percent_encoding::utf8_percent_encode(
                &*id,
                url::percent_encoding::DEFAULT_ENCODE_SET
            ),
            url::percent_encoding::utf8_percent_encode(
                &*conf,
                url::percent_encoding::DEFAULT_ENCODE_SET
            ),
            format!("{}", number),
        );
        trace!(log, "fetching paper reviews"; "url" => &url);
        let paper_reviews = reqwest::get(&url)
            .expect("failed to fetch paper reviews")
            .json::<serde_json::Value>()
            .expect("invalid paper review json");
        let paper_reviews = paper_reviews
            .as_object()
            .expect("paper reviews is not a json object as expected");
        let paper_reviews = paper_reviews
            .get("notes")
            .and_then(|rs| rs.as_array())
            .expect("paper reviews has a weird structure");
        for review in paper_reviews {
            let content = review.as_object().expect("review was not an object")["content"]
                .as_object()
                .expect("review did not have regular contents");
            let r = Review {
                paper: pid,
                rating: content["rating"]
                    .as_str()
                    .expect("rating wasn't a string")
                    .split_whitespace()
                    .next()
                    .unwrap()
                    .trim_end_matches(':')
                    .parse()
                    .expect("rating did not start with a number"),
                confidence: content["confidence"]
                    .as_str()
                    .expect("confidence wasn't a string")
                    .split_whitespace()
                    .next()
                    .unwrap()
                    .trim_end_matches(':')
                    .parse()
                    .expect("confidence did not start with a number"),
            };
            trace!(log, "adding review"; "rating" => r.rating, "confidence" => r.confidence);
            reviews.push(r);
        }
        break; 
    }

    drop(author_set);
    let nauthors = authors.len();

    let mut backend = Backend::make().await; 
 
    let nreviewers = (reviews.len() + (PAPERS_PER_REVIEWER - 1)) / PAPERS_PER_REVIEWER;
    let mut rlogged = (loggedf * nreviewers as f64) as usize;
    let mut alogged = (loggedf * nauthors as f64) as usize;
    
    println!("# nauthors: {}", authors.len());
    println!("# nreviewers: {}", nreviewers);
    println!("# npapers: {}", papers.len());
    println!("# nreviews: {}", reviews.len());
    println!("# logged-in authors: {}", alogged);
    println!("# logged-in reviewers: {}", rlogged);
    println!(
        "# materialization: {}",
        args.value_of("materialization").unwrap()
    );

    let iter = value_t_or_exit!(args, "iter", usize);
    let npapers = value_t_or_exit!(args, "npapers", usize);
    let mut wtr = Writer::from_path(format!("no_fiter_res_n{}.csv", npapers)).unwrap();
    wtr.write_record(&["loggedf",
                       "npapers",
                       "nauthors".into(),
                       "nreviewers".into(),
                       "at".into(),
                       "base".into(),
                       "reader".into(),
                       "mem".into()]);    

    println!("starting noria!");        

    let init = Instant::now();
    thread::sleep(Duration::from_millis(2000));

    let (paper, paper_reviews, paper_conflict) = backend.g.migrate(|mig| {
        let paper = mig.add_base("Paper", &["paperId", "leadContactId", "authorInformation"], Base::default());
        let paper_reviews = mig.add_base("PaperReview", &["paperId", "reviewId", "contactId", "reviewSubmitted"], Base::default());
        let paper_conflict = mig.add_base("PaperConflict", &["paperId", "contactId"], Base::default());
    
        (paper, paper_reviews, paper_conflict)
    }).await;
    
    let uid : usize = 4; // why not
    
    let my_conflicts = backend.g.migrate(move |mig| {
        let my_conflicts = mig.add_ingredient(
            "MyConflicts",
            &["paperId", "contactId"],
            Filter::new(paper_conflict,
                        &[(1, FilterCondition::Comparison(Operator::Equal, Value::Constant(uid.into())))]));
        my_conflicts 
    }).await;
    

    let (my_submitted_reviews0, my_submitted_reviews) = backend.g.migrate(move |mig| {
        let my_submitted_reviews0 = mig.add_ingredient(
            "MySubmittedReviews0",
            &["paperId", "reviewId", "contactId", "reviewSubmitted"],
            Filter::new(paper_reviews, 
                        &[(2, FilterCondition::Comparison(
                        Operator::Equal,
                        Value::Constant(uid.into())))]
            ));
    
        let my_submitted_reviews = mig.add_ingredient(
            "MySubmittedReviews",
            &["paperId", "reviewId", "contactId", "reviewSubmitted"],
            Filter::new(my_submitted_reviews0, 
                        &[(3, FilterCondition::Comparison(
                        Operator::Equal,
                        Value::Constant(1.into())))])); // TODO this should be true 
            
        (my_submitted_reviews0, my_submitted_reviews)
    }).await; 


    let (unconflicted_papers, unconflicted_papers0, 
        unconflicted_paper_reviews, unconflicted_paper_reviews0) = backend.g.migrate(move |mig| {
        let unconflicted_papers0 = mig.add_ingredient(
            "UnconflictedPapers0",
            &["paperId", "leadContactId", "authorInformation", "contactId"],
            Join::new(paper, my_conflicts, JoinType::Left, vec![B(0, 0), L(1), L(2), R(1)])
        ); 
    
        let unconflicted_papers = mig.add_ingredient(
            "UnconflictedPapers", 
            &["paperId", "leadContactId", "authorInformation", "contactId"], 
            Filter::new(unconflicted_papers0, 
                &[(3, FilterCondition::Comparison(
                Operator::NotEqual,
                Value::Constant(0.into())))]));  // TODO this should be None
    
        let unconflicted_paper_reviews0 = mig.add_ingredient(
            "UnconflictedPaperReviews0",
            &["paperId", "reviewId", "contactId", "reviewSubmitted"],
            Join::new(paper_reviews, my_conflicts, JoinType::Left, vec![B(0, 0), L(1), L(2), L(3)])
        );
    
        let unconflicted_paper_reviews = mig.add_ingredient(
            "UnconflictedPaperReviews", 
            &["paperId", "reviewId", "contactId", "reviewSubmitted"], 
            Filter::new(unconflicted_paper_reviews0, 
                &[(3, FilterCondition::Comparison(
                Operator::NotEqual,
                Value::Constant(0.into())))] // TODO this should be None
        )); 
    
        (unconflicted_papers, unconflicted_papers0, unconflicted_paper_reviews, unconflicted_paper_reviews0) 
    }).await; 

    
    let (visible_reviews, visible_reviews_anonymized) = backend.g.migrate(move |mig| {
        let visible_reviews = mig.add_ingredient(
            "VisibleReviews",
            &["paperId", "reviewId", "contactId", "reviewSubmitted"],
            Join::new(unconflicted_paper_reviews, 
                      my_submitted_reviews, 
                      JoinType::Inner, vec![B(0, 0), L(1), L(2), L(3)])
        );
    
        let visible_reviews_anonymized = mig.add_ingredient(
            "VisibleReviewsAnonymized", 
            &["paperId", "reviewId", "contactId", "reviewSubmitted"], 
            Rewrite::new(visible_reviews, visible_reviews, 3, "Anonymous".into(), 0) // TODO ensure signal key is correct. 
        ); 
    
        (visible_reviews, visible_reviews_anonymized)
    }).await; 


    let (paper_paper_review0, paper_paper_review) = backend.g.migrate(move |mig| {
        let paper_paper_review0 = mig.add_ingredient(
            "Paper_PaperReview0",
            &["paperId", "leadContactId", "authorInformation", "reviewId", "contactId", "reviewSubmitted"],
            Join::new(unconflicted_papers, 
                      visible_reviews_anonymized, 
                      JoinType::Left, vec![B(0, 0), L(1), L(2), R(1), R(2), R(3)])
        );
    
        let paper_paper_review = mig.add_ingredient(
            "Paper_PaperReview", 
            &["paperId", "reviewId", "contactId", "reviewSubmitted"], 
            Filter::new(paper_paper_review0, 
                &[(2, FilterCondition::Comparison(
                Operator::Equal,
                Value::Constant(uid.into())))]
        )); 
    
        (paper_paper_review0, paper_paper_review)
    }).await; 
    
    let (r_submitted, final_node) = backend.g.migrate(move |mig| {
        let r_submitted = mig.add_ingredient(
            "R_submitted", 
            &["paperId"], 
            Aggregation::COUNT.over(visible_reviews_anonymized, 3, &[0]) // TODO i think this is wrong 
        ); 
    
        let final_node = mig.add_ingredient(
            "Final",
            &["paperId", "authorInformation", "reviewId", "contactId", "reviewSubmitted"],
            Join::new(paper_paper_review, 
                      r_submitted, 
                      JoinType::Left, vec![B(0, 0), L(2), L(3), L(4), L(5), R(1)])
        );
        (r_submitted, final_node)
    }).await; 

    // let paper = mig.add_base("Paper", &["paperId", "leadContactId", "authorInformation"], Base::default());
    // let paper_reviews = mig.add_base("PaperReview", &["paperId", "reviewId", "contactId", "reviewSubmitted"], Base::default());
    // let paper_conflict = mig.add_base("PaperConflict", &["paperId", "contactId"], Base::default());
    
    let mut review_records = Vec::new(); 
    let mut i = 0; 
    for r in reviews.iter() {
        let mut submitted : usize = 1; 
        let mut new_record : Vec<DataType> = vec![
            r.paper.into(),
            format!("r{}", i + nauthors + 1).into(),
            format!("{},{}", r.paper, i + nauthors + 1).into(),
            submitted.into()]; 
        review_records.push(new_record.clone()); 
        i += 1; 
    }

    backend.populate("PaperReview", review_records).await;  


    let mut paper_records = Vec::new(); 
    let mut i = 0; 
    for r in papers.iter() {
        let mut submitted : usize = 1; 
        let mut new_record : Vec<DataType> = vec![
            i.into(),
            format!("{},{}", i, i + nauthors + 1).into(),
            submitted.into()]; 
        paper_records.push(new_record.clone()); 
        i += 1; 
    }

    backend.populate("Paper", paper_records).await;  
        
    memstats(&mut backend.g, "populated", wtr, loggedf, npapers, nauthors, nreviewers).await;


}