use bincode;
use serde::Serialize;

use std::sync::mpsc;
use std::fs::File;
use std::io::BufWriter;
use std::net::SocketAddr;

use channel::tcp::TcpSender;
use controller::ControlEvent;
use coordination::{CoordinationMessage, CoordinationPayload};
use dataflow::PersistSnapshotRequest;

/// Receives cloned states from a domain and persists snapshot files.
pub(crate) struct SnapshotPersister {
    coordination_method: SnapshotCoordination,
    receiver: mpsc::Receiver<PersistSnapshotRequest>,
    sender: mpsc::Sender<PersistSnapshotRequest>,
}

/// Snapshots confirmations can be sent in both local and remote settings.
/// In the former the ACKs are sent directly to the controller's main_loop,
/// whereas we make use of the coordination layer for the latter.
pub(crate) enum SnapshotCoordination {
    Local(mpsc::Sender<ControlEvent>),
    Remote {
        local_addr: SocketAddr,
        controller_addr: SocketAddr,
    },
}

impl SnapshotPersister {
    pub fn new(coordination_method: SnapshotCoordination) -> Self {
        let (sender, receiver) = mpsc::channel();
        Self {
            coordination_method,
            sender,
            receiver,
        }
    }

    fn serialize<T: Serialize>(filename: String, state: T) {
        let file =
            File::create(&filename).expect(&format!("Failed creating snapshot file: {}", filename));
        let mut writer = BufWriter::new(file);
        bincode::serialize_into(&mut writer, &state, bincode::Infinite)
            .expect("bincode serialization of snapshot failed");
    }

    pub fn start(self) {
        let (local, mut remote) = match self.coordination_method {
            SnapshotCoordination::Local(sender) => (Some(sender), None),
            SnapshotCoordination::Remote {
                local_addr,
                controller_addr,
            } => {
                let connection: TcpSender<CoordinationMessage> =
                    TcpSender::connect(&controller_addr, None)
                        .expect("Could not connect to Controller");
                (None, Some((local_addr, connection)))
            }
        };

        for event in self.receiver {
            for (filename, state) in event.reader_states {
                Self::serialize(filename, state);
            }

            for (filename, state) in event.node_states {
                Self::serialize(filename, state);
            }

            if let Some(ref local_s) = local {
                local_s
                    .send(ControlEvent::SnapshotCompleted(
                        event.domain_id,
                        event.snapshot_id,
                    ))
                    .unwrap();
            } else if let Some((source, ref mut remote_s)) = remote {
                remote_s
                    .send(CoordinationMessage {
                        source,
                        payload: CoordinationPayload::SnapshotCompleted(
                            event.domain_id,
                            event.snapshot_id,
                        ),
                    })
                    .unwrap();
            }
        }
    }

    pub fn sender(&self) -> mpsc::Sender<PersistSnapshotRequest> {
        self.sender.clone()
    }
}
