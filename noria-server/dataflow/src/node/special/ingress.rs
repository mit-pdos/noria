use prelude::*;
use std::collections::HashMap;

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Ingress {
    /// Parent egress node
    src: Option<NodeIndex>,
    /// The last packet received from each parent
    last_packet_received: HashMap<NodeIndex, usize>,
}

impl Ingress {
    pub fn new() -> Ingress {
        Ingress::default()
    }

    pub fn set_src(&mut self, src: NodeIndex) {
        assert!(self.src.is_none());
        self.src = Some(src);
    }

    pub fn src(&self) -> NodeIndex {
        self.src.expect("ingress should have an egress parent")
    }

    /// Receive a packet, keeping track of the latest packet received from each parent. If the
    /// parent crashes, we can tell the parent's replacement where to resume sending messages.
    pub fn receive_packet(&mut self, m: &Box<Packet>) {
        let (from, label) = {
            let id = match m {
                box Packet::Message { id, .. } => id.unwrap(),
                box Packet::ReplayPiece { id, .. } => id.unwrap(),
                _ => unreachable!(),
            };
            (id.from, id.label)
        };

        // println!("RECEIVE PACKET #{} <- {}", label, from.index());

        // labels must be sequential
        let old_label = self.last_packet_received.get(&from);
        assert!(old_label.is_none() || label == old_label.unwrap() + 1);
        self.last_packet_received.insert(from, label);
    }

    /// Replace an incoming connection from `old` with `new`.
    /// Returns the label of the next message expected from the new connection.
    pub fn new_incoming(&mut self, old: NodeIndex, new: NodeIndex) -> usize {
        assert_eq!(self.src, Some(old));
        self.src = Some(new);
        let label = self.last_packet_received.remove(&old).unwrap_or(0);
        self.last_packet_received.insert(new, label);
        label + 1
    }

    pub(in crate::node) fn take(&mut self) -> Self {
        Clone::clone(self)
    }
}
