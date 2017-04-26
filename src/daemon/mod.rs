use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::io;

use mio::*;
use mio::tcp::{TcpListener, TcpStream};

use serde_json;
use serde::{Serialize, Deserialize};

use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};

use slog;

use flow::prelude;
use flow::domain;
use checktable;

trait SerializeAndSend: WriteBytesExt {
    fn send<T: Serialize>(&mut self, tag: u64, data: &T) -> io::Result<()> {
        let data = serde_json::to_vec(data).unwrap();

        try!(self.write_u64::<BigEndian>(tag));
        try!(self.write_u64::<BigEndian>(data.len() as u64));
        try!(self.write_all(&data[..]));
        Ok(())
    }
}
impl<W: WriteBytesExt> SerializeAndSend for W {}

trait ReceiveAndDeserialize: ReadBytesExt {
    fn recv_with_tag<T: Deserialize>(&mut self, anticipated_tag: u64) -> Result<T, ()> {
        let tag: u64 = self.read_u64::<BigEndian>().unwrap();
        let len: u64 = self.read_u64::<BigEndian>().unwrap();
        let mut data = Vec::with_capacity(len as usize);
        try!(self.read_exact(&mut data[..]).map_err(|_| {}));

        if tag != anticipated_tag {
            return Err(());
        }
        serde_json::from_slice(&data[..]).map_err(|_| {})
    }
    fn recv<T: Deserialize>(&mut self) -> Result<T, ()> {
        let len: u64 = self.read_u64::<BigEndian>().unwrap();
        let mut data = Vec::with_capacity(len as usize);
        try!(self.read_exact(&mut data[..]).map_err(|_| {}));
        serde_json::from_slice(&data[..]).map_err(|_| {})
    }
}
impl<W: ReadBytesExt> ReceiveAndDeserialize for W {}

type RecvHandler = Box<Fn(&mut TcpStream) + Send>;

const CONTROL_MESSAGE_TOKEN: Token = Token(2);
const TOKEN_INDEX_START: usize = 3;

enum InternalMessage {
    AddPeer(SocketAddr),
    StartDomainOnWorker {
        worker: SocketAddr,
        domain: domain::Index,
        nodes: prelude::DomainNodes,
    },
}

#[derive(Serialize, Deserialize)]
pub enum Command {
    StartDomain {
        domain: domain::Index,
        nodes: prelude::DomainNodes,
    },
}

enum ControlMessage {
    Internal(InternalMessage),
    Outgoing(Command),
}

struct ControlChannel {
    messages: Vec<ControlMessage>,
    set_readiness: SetReadiness,
}

impl ControlChannel {
    pub fn new(poll: &Poll) -> ControlChannel {
        let (registration, set_readiness) = Registration::new2();
        poll.register(&registration,
                      CONTROL_MESSAGE_TOKEN,
                      Ready::readable(),
                      PollOpt::edge());

        ControlChannel {
            messages: Vec::new(),
            set_readiness,
        }
    }
}

struct DaemonInner {
    poll: Poll,

    tokens: HashMap<Token, SocketAddr>,
    peers: HashMap<SocketAddr, TcpStream>,
    handlers: HashMap<(SocketAddr, u64), RecvHandler>,

    control_channel: Arc<Mutex<ControlChannel>>,

    next_token_index: usize,
}

impl DaemonInner {
    pub fn start_polling(self) {
        thread::spawn(move || self.poll());
    }

    fn poll(mut self) {
        let mut events = Events::with_capacity(1024);
        loop {
            self.poll.poll(&mut events, None).unwrap();
            for event in &events {
                match event.token() {
                    CONTROL_MESSAGE_TOKEN => self.handle_control_messages(),
                    token => {
                        let peer = self.tokens.get(&token).unwrap().clone();
                        let stream = self.peers.get_mut(&peer).unwrap();

                        let tag: u64 = stream.read_u64::<BigEndian>().unwrap();
                        if tag == 0 {
                            let command: Command = stream.recv().unwrap();
                            match command {
                                Command::StartDomain { domain, nodes } => {
                                    let mock_logger = slog::Logger::root(slog::Discard, None);
                                    let mock_checktable =
                                        Arc::new(Mutex::new(checktable::CheckTable::new()));

                                    let domain = domain::Domain::new(mock_logger,
                                                                     domain,
                                                                     nodes,
                                                                     mock_checktable,
                                                                     0);
                                }
                            }
                        } else {
                            self.handlers.get(&(peer, tag)).unwrap()(stream);
                        }
                    }
                }
            }
        }
    }

    fn handle_control_messages(&mut self) {
        let mut control_channel = self.control_channel.lock().unwrap();
        for m in control_channel.messages.drain(..) {
            match m {
                ControlMessage::Internal(InternalMessage::AddPeer(addr)) => {
                    let mut events = Events::with_capacity(1);
                    let mut sock = TcpStream::connect(&addr).unwrap();
                    let token = Token(0);
                    self.poll
                        .register(&sock, token, Ready::readable(), PollOpt::edge())
                        .unwrap();

                    self.poll.poll(&mut events, None).unwrap();
                    let event = events.iter().next().unwrap();
                    assert_eq!(event.token(), token);

                    let peers: HashSet<SocketAddr> = self.peers.keys().cloned().collect();
                    sock.send(0, &peers).unwrap();
                }
                ControlMessage::Internal(InternalMessage::StartDomainOnWorker {
                                             worker,
                                             domain,
                                             nodes,
                                         }) => {
                    let command = Command::StartDomain { domain, nodes };
                    if let Some(peer) = self.peers.get_mut(&worker) {
                        peer.send(1, &command).unwrap();
                    }
                }
                ControlMessage::Outgoing(_) => unimplemented!(),
            }
        }
        control_channel
            .set_readiness
            .set_readiness(Ready::empty())
            .unwrap();
    }

    fn register_handler(&mut self, from_addr: SocketAddr, tag: u64, handler: RecvHandler) {
        self.handlers.insert((from_addr, tag), handler);
    }
}

pub struct Daemon {
    me: SocketAddr,
    is_master: bool,
    control_channel: Arc<Mutex<ControlChannel>>,
}

impl Daemon {
    fn is_master(&self) -> bool {
        self.is_master
    }

    pub fn new_master(me: SocketAddr) -> Self {
        let poll = Poll::new().unwrap();
        let control_channel = Arc::new(Mutex::new(ControlChannel::new(&poll)));

        DaemonInner {
                poll,
                tokens: HashMap::new(),
                peers: HashMap::new(),
                handlers: HashMap::new(),
                control_channel: control_channel.clone(),
                next_token_index: TOKEN_INDEX_START,
            }
            .start_polling();

        Self {
            me,
            is_master: true,
            control_channel,
        }
    }

    pub fn new_worker(me: SocketAddr) -> Self {
        // Create an poll instance
        let poll = Poll::new().unwrap();

        // Create storage for events
        let mut events = Events::with_capacity(1);

        // Setup the server socket
        let server = TcpListener::bind(&me).unwrap();

        let token = Token(0);
        // Start listening for incoming connections
        poll.register(&server, token, Ready::readable(), PollOpt::edge())
            .unwrap();

        poll.poll(&mut events, None).unwrap();
        let event = events.iter().next().unwrap();
        assert_eq!(event.token(), token);

        let mut master = server.accept().unwrap();
        let peers: HashSet<SocketAddr> = master.0.recv_with_tag(0).unwrap();

        // TODO: connect to peers

        let control_channel = Arc::new(Mutex::new(ControlChannel::new(&poll)));

        DaemonInner {
                poll: Poll::new().unwrap(),
                tokens: HashMap::new(),
                peers: HashMap::new(),
                handlers: HashMap::new(),
                control_channel: control_channel.clone(),
                next_token_index: TOKEN_INDEX_START,
            }
            .start_polling();

        Self {
            me,
            is_master: false,
            control_channel,
        }
    }

    pub fn add_worker(&mut self, addr: SocketAddr) {
        assert!(self.is_master);

        let mut control_channel = self.control_channel.lock().unwrap();
        control_channel
            .messages
            .push(ControlMessage::Internal(InternalMessage::AddPeer(addr)));
        control_channel
            .set_readiness
            .set_readiness(Ready::readable())
            .unwrap();
    }

    pub fn start_domain_on_worker(&mut self,
                                  worker: SocketAddr,
                                  domain: domain::Index,
                                  nodes: prelude::DomainNodes) {
        assert!(self.is_master);

        let mut control_channel = self.control_channel.lock().unwrap();
        control_channel
            .messages
            .push(ControlMessage::Internal(InternalMessage::StartDomainOnWorker {
                                               worker,
                                               domain,
                                               nodes,
                                           }));
        control_channel
            .set_readiness
            .set_readiness(Ready::readable())
            .unwrap();
    }
}

/// Object that can establish a connection to a master instance to offload work to this node.
pub struct WorkerDaemon {}

impl WorkerDaemon {
    /// Create a new `WorkerDaemon`
    pub fn start(addr: SocketAddr) {
        let _ = Daemon::new_worker(addr);
    }
}
