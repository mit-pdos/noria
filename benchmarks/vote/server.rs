use super::ssh::Ssh;
use super::Backend;
use ssh2;
use std::error::Error;
use std::borrow::Cow;
use std::{thread, time};

pub(crate) enum ServerHandle<'a> {
    Netsoup(ssh2::Channel<'a>, ssh2::Channel<'a>),
    HandledBySystemd,
}

impl<'a> ServerHandle<'a> {
    fn end(self, server: &Ssh, backend: &Backend) -> Result<(), Box<Error>> {
        match self {
            ServerHandle::Netsoup(mut c, mut w) => {
                // assert_eq!(backend, &Backend::Netsoup);

                // we need to terminate the controller and the worker.
                // the SSH spec does have support for sending signals to remote workers, but
                // neither OpenSSH nor libssh2 support that feature:
                //
                //  - https://bugzilla.mindrot.org/show_bug.cgi?id=1424
                //  - https://www.libssh2.org/mail/libssh2-devel-archive-2009-09/0079.shtml
                //
                // so, instead we have to hack around it by finding the pid and killing it
                if c.eof() | w.eof() {
                    // one of them terminated prematurely!
                    unimplemented!();
                }

                let c_killed = server.just_exec(&["pkill", "-f", "target/release/controller"])?;
                let w_killed = server.just_exec(&["pkill", "-f", "target/release/souplet"])?;
                if !c_killed.is_ok() || !w_killed.is_ok() {
                    unimplemented!();
                }

                c.wait_eof()?;
                w.wait_eof()?;

                // also stop zookeeper
                match server.just_exec(&["sudo", "systemctl", "stop", "zookeeper"]) {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => Err(e)?,
                    Err(e) => Err(e)?,
                }
            }
            ServerHandle::HandledBySystemd => match server.just_exec(&[
                "sudo",
                "systemctl",
                "stop",
                backend.systemd_name().unwrap(),
            ]) {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => Err(e)?,
                Err(e) => Err(e)?,
            },
        }
        Ok(())
    }
}

#[must_use]
pub(crate) struct Server<'a> {
    server: &'a Ssh,
    listen_addr: &'a str,
    has_pl: bool,

    handle: ServerHandle<'a>,
}

impl<'a> Server<'a> {
    pub(crate) fn end(self, backend: &Backend) -> Result<(), Box<Error>> {
        self.handle.end(self.server, backend)
    }

    pub(crate) fn between_targets(self, backend: &Backend) -> Result<Self, Box<Error>> {
        match *backend {
            Backend::Netsoup { .. } | Backend::Memcached => {
                let s = self.server;
                let a = self.listen_addr;
                let p = self.has_pl;

                // these backends need to be cleared after every run
                eprintln!(" -> restarting server");
                self.end(backend)?;

                // give it some time to shut down
                thread::sleep(time::Duration::from_secs(1));

                // start a new one!
                let s = start(s, a, p, backend)??;
                eprintln!(" .. server restart completed");
                Ok(s)
            }
            _ => Ok(self),
        }
    }
}

pub(crate) fn start<'a>(
    server: &'a Ssh,
    listen_addr: &'a str,
    has_pl: bool,
    b: &Backend,
) -> Result<Result<Server<'a>, String>, Box<Error>> {
    let sh = match *b {
        Backend::Netsoup {
            workers,
            readers,
            shards,
        } => {
            // first, build controller if it hasn't been built already
            match server
                .in_distributary(has_pl, &["cargo", "b", "--release", "--bin", "controller"])
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Ok(Err(e)),
                Err(e) => return Err(e),
            }

            // then, build worker if it hasn't been built already
            match server.in_distributary(has_pl, &["cargo", "b", "--release", "--bin", "souplet"]) {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Ok(Err(e)),
                Err(e) => return Err(e),
            }

            // now that server components have been built, start zookeeper
            match server.just_exec(&["sudo", "systemctl", "start", "zookeeper"]) {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Ok(Err(e)),
                Err(e) => return Err(e),
            }

            // wait for zookeeper to be running
            let start = time::Instant::now();
            while server
                .just_exec(&["nc", "-z", "localhost", "2181"])?
                .is_err()
            {
                if start.elapsed() > time::Duration::from_secs(2) {
                    Err("zookeeper wouldn't start")?;
                }
            }

            // then start the controller
            let shards = format!("{}", shards.unwrap_or(0));
            let c = {
                let mut cmd = vec!["cd", "distributary", "&&"];
                cmd.extend(vec![
                    "target/release/controller",
                    "--remote-workers=1",
                    "--shards",
                    &shards,
                    "--address",
                    listen_addr,
                ]);
                server.exec(&cmd[..]).unwrap()
            };

            // and the remote worker
            let mut cmd: Vec<Cow<str>> = ["cd", "distributary", "&&"]
                .into_iter()
                .map(|&s| s.into())
                .collect();
            if has_pl {
                cmd.push("perflock".into());
            }
            cmd.extend(vec![
                "target/release/souplet".into(),
                "--zookeeper".into(),
                listen_addr.into(),
                "-w".into(),
                format!("{}", workers).into(),
                "-r".into(),
                format!("{}", readers).into(),
            ]);
            let cmd: Vec<_> = cmd.iter().map(|s| &**s).collect();
            let w = server.exec(&cmd[..]).unwrap();

            ServerHandle::Netsoup(c, w)
        }
        Backend::Memcached | Backend::Mysql | Backend::Mssql => {
            match server.just_exec(&["sudo", "systemctl", "start", b.systemd_name().unwrap()]) {
                Ok(Ok(_)) => ServerHandle::HandledBySystemd,
                Ok(Err(e)) => return Ok(Err(e)),
                Err(e) => return Err(e),
            }
        }
    };

    Ok(Ok(Server {
        server,
        listen_addr,
        has_pl,
        handle: sh,
    }))
}
