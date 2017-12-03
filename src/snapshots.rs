use bincode;

use std::sync::mpsc;
use std::fs::File;
use std::io::BufWriter;
use std::net::SocketAddr;

use channel::tcp::TcpSender;
use coordination::{CoordinationMessage, CoordinationPayload};
use dataflow::PersistSnapshotRequest;

/// Receives cloned states from a domain and persists snapshot files.
pub struct SnapshotPersister {
    controller_addr: Option<SocketAddr>,
    receiver: mpsc::Receiver<PersistSnapshotRequest>,
    sender: mpsc::Sender<PersistSnapshotRequest>,
}

impl SnapshotPersister {
    pub fn new(controller_addr: Option<SocketAddr>) -> Self {
        let (sender, receiver) = mpsc::channel();
        Self {
            controller_addr,
            sender,
            receiver,
        }
    }

    pub fn start(self) {
        let mut coordination_tx = self.controller_addr.and_then(|ref addr| {
            Some(TcpSender::connect(&addr, None).expect("Could not connect to Controller"))
        });

        for event in self.receiver {
            for (filename, state) in event.node_states.iter() {
                let file = File::create(&filename)
                    .expect(&format!("Failed creating snapshot file: {}", filename));
                let mut writer = BufWriter::new(file);
                bincode::serialize_into(&mut writer, &state, bincode::Infinite)
                    .expect("bincode serialization of snapshot failed");
            }

            if let Some(ref mut tx) = coordination_tx {
                let payload =
                    CoordinationPayload::SnapshotCompleted(event.domain_id, event.snapshot_id);
                tx.send(CoordinationMessage {
                    payload,
                    // TODO(ekmartin): SnapshotPersisters may run on both separate Souplet
                    // instances, or below a local worker pool. We're (ab)using
                    // CoordinationMessages to notify the controller about our snapshot ACKs, but
                    // the local/remote compromise means we won't always have a source addr. If
                    // we're going to keep sending snapshot ACKs over the coordination plane we
                    // should probably solve this somehow.
                    source: "127.0.0.1:0".parse().unwrap(),
                }).unwrap();
            }
        }
    }

    pub fn sender(&self) -> mpsc::Sender<PersistSnapshotRequest> {
        self.sender.clone()
    }
}
