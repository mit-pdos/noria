use super::ssh::Ssh;
use super::Backend;
use ssh2;
use std::error::Error;
use std::borrow::Cow;

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
                if c.eof() || w.eof() {
                    // one of them terminated prematurely!
                    unimplemented!();
                }

                let w_killed = server.just_exec(&["pkill", "-f", "target/release/souplet"])?;
                let c_killed = server.just_exec(&["pkill", "-f", "target/release/controller"])?;
                if !w_killed.is_ok() || !c_killed.is_ok() {
                    unimplemented!();
                }

                c.wait_eof()?;
                w.wait_eof()?;
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
    handle: ServerHandle<'a>,
}

impl<'a> Server<'a> {
    pub(crate) fn end(self, backend: &Backend) -> Result<(), Box<Error>> {
        self.handle.end(self.server, backend)
    }
}

pub(crate) fn start<'a>(
    server: &'a Ssh,
    listen_addr: &str,
    has_pl: bool,
    b: &Backend,
) -> Result<Result<Server<'a>, String>, Box<Error>> {
    match *b {
        Backend::Netsoup {
            workers,
            readers,
            shards,
        } => {
            // first, build controller if it hasn't been built already
            match server.in_distributary(
                has_pl,
                &[
                    "cargo",
                    "b",
                    "--release",
                    "--manifest-path benchmarks/Cargo.toml",
                    "--bin",
                    "controller",
                ],
            ) {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Ok(Err(e)),
                Err(e) => return Err(e),
            }

            // then, build worker if it hasn't been built already
            match server.in_distributary(
                has_pl,
                &[
                    "cargo",
                    "b",
                    "--release",
                    "--manifest-path benchmarks/Cargo.toml",
                    "--bin",
                    "souplet",
                ],
            ) {
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

            // then start the controller
            // TODO: shards
            let mut cmd = vec!["cd", "distributary", "&&"];
            cmd.extend(vec![
                "target/release/controller",
                "--remote-workers=1",
                "--address",
                listen_addr,
            ]);
            let c = server.exec(&cmd[..]).unwrap();

            // and the remote worker
            // TODO: workers + readers
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

            Ok(Ok(Server {
                server,
                handle: ServerHandle::Netsoup(c, w),
            }))
        }
        Backend::Memcached | Backend::Mysql | Backend::Mssql => {
            match server.just_exec(&["sudo", "systemctl", "start", b.systemd_name().unwrap()]) {
                Ok(Ok(_)) => Ok(Ok(Server {
                    server,
                    handle: ServerHandle::HandledBySystemd,
                })),
                Ok(Err(e)) => Ok(Err(e)),
                Err(e) => Err(e),
            }
        }
    }
}
