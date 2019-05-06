use tiny_http::{Method, Response, StatusCode};
use std::collections::HashMap;
use std::thread;
use std::time::Duration;
use tokio;

use noria::{
    SyncControllerHandle,
    prelude::{SyncTable, SyncView},
};

type Data = String;

/// HTTP proxy for building and using local handles to views and tables.
pub struct NoriaHttpProxy {
    zk: String,
    rt: tokio::runtime::Runtime,
    tables: HashMap<String, SyncTable>,
    views: HashMap<String, SyncView>,
}

impl Default for NoriaHttpProxy {
    fn default() -> NoriaHttpProxy {
        NoriaHttpProxy {
            zk: "127.0.0.1:2181/gina".to_string(),
            rt: tokio::runtime::Runtime::new().unwrap(),
            tables: Default::default(),
            views: Default::default(),
        }
    }
}

impl NoriaHttpProxy {
    /// Forwards a request to the controller through the HTTP proxy, or uses existing state.
    ///
    /// POST /build_table
    /// POST /build_view
    pub fn handle_request(
        &mut self,
        method: Method,
        path: String,
        body: String,
    ) -> Result<Option<Data>, StatusCode> {
        use serde_json as json;
        match (method, path.as_ref()) {
            (Method::Post, "/build_table") => json::from_str(&body)
                .map(|args| self.build_table(args))
                .unwrap_or_else(|_| Err(StatusCode(400))),
            (Method::Post, "/build_view") => json::from_str(&body)
                .map(|args| self.build_view(args))
                .unwrap_or_else(|_| Err(StatusCode(400))),
            _ => Err(StatusCode(404)),
        }
    }

    /// Caches a table for future writes.
    fn build_table(&mut self, base: String) -> Result<Option<Data>, StatusCode> {
        println!("build_table {}", base);
        let zk = &self.zk;
        let executor = self.rt.executor();
        let mut db = SyncControllerHandle::from_zk(zk, executor).unwrap();

        let executor = self.rt.executor();
        let get_table = move |b: &mut SyncControllerHandle<_, _>, n| loop {
            match b.table(n) {
                Ok(v) => {
                    return v.into_sync();
                },
                Err(_) => {
                    thread::sleep(Duration::from_millis(50));
                    *b = SyncControllerHandle::from_zk(zk, executor.clone())
                        .unwrap();
                }
            }
        };

        let table = get_table(&mut db, &base);
        self.tables.insert(base, table);
        Ok(None)
    }

    /// Caches a view for future reads.
    fn build_view(&mut self, name: String) -> Result<Option<Data>, StatusCode> {
        println!("build_view {}", name);
        let zk = &self.zk;
        let executor = self.rt.executor();
        let mut db = SyncControllerHandle::from_zk(zk, executor).unwrap();

        let executor = self.rt.executor();
        let get_view = move |b: &mut SyncControllerHandle<_, _>, n| loop {
            match b.view(n) {
                Ok(v) => return v.into_sync(),
                Err(_) => {
                    thread::sleep(Duration::from_millis(50));
                    *b = SyncControllerHandle::from_zk(zk, executor.clone())
                        .unwrap();
                }
            }
        };

        let view = get_view(&mut db, &name);
        self.views.insert(name, view);
        Ok(None)
    }
}

fn main() {
    let mut proxy: NoriaHttpProxy = Default::default();
    let server = tiny_http::Server::http("127.0.0.1:6036").unwrap();

    // Synchronously handle all incoming requests
    for mut request in server.incoming_requests() {
        let method = request.method().clone();
        let path = request.url().to_string();
        let mut body = String::new();
        request.as_reader().read_to_string(&mut body).unwrap();

        let response = proxy
            .handle_request(method, path, body)
            .map(|data| data.unwrap_or("".to_string()))
            .map(|data| Response::from_string(data).with_status_code(200))
            .unwrap_or_else(|code| Response::from_string("".to_string()).with_status_code(code));
        request.respond(response).unwrap();
    }
}
