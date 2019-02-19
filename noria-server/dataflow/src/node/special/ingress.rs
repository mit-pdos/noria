use prelude::*;
use std::collections::HashMap;

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Ingress {
    /// The last packet received from each parent
    last_packet_received: HashMap<NodeIndex, usize>,
}

impl Ingress {
    pub fn new() -> Ingress {
        Ingress::default()
    }

    /// Receive a packet, keeping track of the latest packet received from each parent. If the
    /// parent crashes, we can tell the parent's replacement where to resume sending messages.
    pub fn receive_packet(&mut self, m: &Box<Packet>) {
        let (from, label) = match m {
            box Packet::Message { id, .. } => (id.from(), id.label()),
            box Packet::ReplayPiece { id, .. } => (id.from(), id.label()),
            _ => unreachable!(),
        };

        // println!( "{} RECEIVE #{} from {:?}", self.global_addr().index(), label, from);

        // labels are not necessarily sequential, but must be increasing
        let old_label = self.last_packet_received.insert(from, label);
        assert!(label > old_label.unwrap_or(0));
    }

    /// Replace an incoming connection from `old` with `new`.
    /// Returns the label of the next message expected from the new connection.
    pub fn new_incoming(&mut self, old: NodeIndex, new: NodeIndex) -> usize {
        let label = self.last_packet_received.remove(&old).unwrap_or(0);
        self.last_packet_received.insert(new, label);
        label + 1
    }

    pub(in crate::node) fn take(&mut self) -> Self {
        Clone::clone(self)
    }
}
