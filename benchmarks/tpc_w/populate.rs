use std::io::{BufRead, BufReader};
use std::fs::File;
use std::str::FromStr;
use std::time;

use distributary::Token;
use super::Backend;

pub fn populate_countries(backend: &Backend, data_location: &str) {
    let country_putter = backend.g.get_mutator(backend.r.node_addr_for("country").unwrap());

    let f = File::open(format!("{}/countries.tsv", data_location)).unwrap();
    let mut reader = BufReader::new(f);

    let mut s = String::new();
    let start = time::Instant::now();
    let mut i = 0;
    while reader.read_line(&mut s).unwrap() > 0 {
        {
            let fields: Vec<&str> = s.split("\t").collect();
            let co_id = i64::from_str(fields[0]).unwrap();
            let co_name = fields[1];
            let co_exchange = fields[2]; // XXX(malte): DataType doesn't support floats
            let co_currency = fields[3];
            country_putter.put(vec![co_id.into(),
                                    co_name.into(),
                                    co_exchange.into(),
                                    co_currency.into()]);
        }
        i += 1;
        s.clear();
    }
    println!("Wrote {} countries in {:.2}s!",
             i,
             start.elapsed().as_secs());
}

pub fn populate_orders(backend: &Backend, data_location: &str) {
    let order_putter = backend.g.get_mutator(backend.r.node_addr_for("orders").unwrap());

    let f = File::open(format!("{}/orders.tsv", data_location)).unwrap();
    let mut reader = BufReader::new(f);

    let mut s = String::new();
    let start = time::Instant::now();
    let mut i = 0;
    while reader.read_line(&mut s).unwrap() > 0 {
        {
            let fields: Vec<&str> = s.split("\t").collect();
            let o_id = i64::from_str(fields[0]).unwrap();
            let o_c_id = i64::from_str(fields[1]).unwrap();
            let o_date = fields[2];
            let o_sub_total = fields[3]; // XXX(malte): DataType doesn't support floats
            let o_tax = fields[4]; // XXX(malte): DataType doesn't support floats
            let o_total = fields[5]; // XXX(malte): DataType doesn't support floats
            let o_ship_type = fields[6];
            let o_ship_date = fields[7];
            let o_bill_addr_id = i64::from_str(fields[8]).unwrap();
            let o_ship_addr_id = i64::from_str(fields[9]).unwrap();
            let o_status = fields[10];

            order_putter.put(vec![o_id.into(),
                                  o_c_id.into(),
                                  o_date.into(),
                                  o_sub_total.into(),
                                  o_tax.into(),
                                  o_total.into(),
                                  o_ship_type.into(),
                                  o_ship_date.into(),
                                  o_bill_addr_id.into(),
                                  o_ship_addr_id.into(),
                                  o_status.into()]);
        }
        i += 1;
        s.clear();
    }
    println!("Wrote {} orders in {:.2}s!", i, start.elapsed().as_secs());
}
