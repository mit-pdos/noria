use whoami;
use std::net::TcpStream;
use std::error::Error;
use std::io::prelude::*;
use ssh2::{Channel, Session};
use shellwords;

pub(crate) struct Ssh(TcpStream, Session);

use std::ops::{Deref, DerefMut};
impl Deref for Ssh {
    type Target = Session;
    fn deref(&self) -> &Self::Target {
        &self.1
    }
}
impl DerefMut for Ssh {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.1
    }
}

impl Ssh {
    pub(crate) fn connect(addr: &str) -> Result<Ssh, Box<Error>> {
        // hostname or user@hostname
        // no proxy support for now :(
        let mut server_parts = addr.split('@');
        let username = whoami::username();
        let mut username = &*username;
        let mut server = server_parts.next().unwrap();
        if let Some(host) = server_parts.next() {
            username = server;
            server = host;
        }
        let mut server_parts = server.split(':');
        server = server_parts.next().unwrap();
        let mut port = 22;
        if let Some(p) = server_parts.next() {
            port = p.parse().unwrap();
        }

        // try connecting a couple of times
        let mut iter = 0;
        let tcp = loop {
            match TcpStream::connect((server, port)) {
                Ok(s) => break Ok(s),
                Err(_) if iter < 10 => {
                    ::std::thread::sleep(::std::time::Duration::from_secs(1));
                    iter += 1;
                }
                Err(e) => {
                    break Err(e);
                }
            }
        }?;
        let mut sess = Session::new().unwrap();
        sess.handshake(&tcp)?;

        // try all agent identities in order
        {
            let mut agent = sess.agent()?;
            agent.connect()?;
            agent.list_identities()?;

            for identity in agent.identities() {
                let identity = identity?;
                if agent.userauth(username, &identity).is_ok() && sess.authenticated() {
                    break;
                }
            }
        }

        if !sess.authenticated() {
            Err(format!("failed to authenticate to '{}'", addr))?;
        }

        Ok(Ssh(tcp, sess))
    }

    pub(crate) fn in_distributary(
        &self,
        has_pl: bool,
        cmd: &[&str],
    ) -> Result<Result<(), String>, Box<Error>> {
        let mut args = vec!["cd", "distributary", "&&"];
        if has_pl {
            args.push("pls");
        }
        args.extend(cmd);

        self.just_exec(&args[..])
    }

    pub(crate) fn just_exec(&self, cmd: &[&str]) -> Result<Result<(), String>, Box<Error>> {
        let mut c = self.exec(cmd)?;

        let mut stderr = String::new();
        c.stderr().read_to_string(&mut stderr)?;
        c.wait_eof()?;

        if c.exit_status()? != 0 {
            return Ok(Err(stderr));
        }
        Ok(Ok(()))
    }

    pub(crate) fn exec<'a>(&'a self, cmd: &[&str]) -> Result<Channel<'a>, Box<Error>> {
        let mut c = self.channel_session()?;
        let cmd: Vec<_> = cmd.iter()
            .map(|&arg| match arg {
                "&&" | "<" | ">" | "2>" | "2>&1" | "|" => arg.to_string(),
                _ => shellwords::escape(arg),
            })
            .collect();
        let cmd = cmd.join(" ");
        eprintln!("    :> {}", cmd);

        // ensure we're using a Bourne shell (that's what shellwords supports too)
        let cmd = format!("bash -c {}", shellwords::escape(&cmd));
        c.exec(&cmd)?;
        Ok(c)
    }
}
