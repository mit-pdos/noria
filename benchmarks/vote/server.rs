use super::ssh::Ssh;
use super::Backend;
use ssh2;
use std::io::prelude::*;
use std::error::Error;

pub(crate) enum ServerHandle<'a> {
    Netsoup(ssh2::Channel<'a>, ssh2::Channel<'a>),
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

                let w_killed = server.just_exec("pkill -f target/release/souplet")?;
                let c_killed = server.just_exec("pkill -f target/release/controller")?;
                if !w_killed || !c_killed {
                    unimplemented!();
                }

                c.wait_eof()?;
                w.wait_eof()?;
            }
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
                b"cargo b --release --manifest-path benchmarks/Cargo.toml --bin controller",
            ) {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Ok(Err(e)),
                Err(e) => return Err(e),
            }

            // then, build worker if it hasn't been built already
            match server.in_distributary(
                has_pl,
                b"cargo b --release --manifest-path benchmarks/Cargo.toml --bin souplet",
            ) {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Ok(Err(e)),
                Err(e) => return Err(e),
            }

            // now that server components have been built, start zookeeper
            match server.in_distributary(false, b"sudo systemctl start zookeeper") {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Ok(Err(e)),
                Err(e) => return Err(e),
            }

            // then start the controller
            // TODO: shards
            let mut c = server.channel_session().unwrap();
            c.write_all(b"cd distributary\n").unwrap();
            c.write_all(b"target/release/controller --remote-workers=1 --address=0.0.0.0")
                .unwrap();
            c.send_eof().unwrap();

            // and the remote worker
            // TODO: workers + readers
            let mut w = server.channel_session().unwrap();
            w.write_all(b"cd distributary\n").unwrap();
            if has_pl {
                w.write_all(b"perflock ").unwrap();
            }
            w.write_all(b"target/release/souplet --zookeeper=0.0.0.0")
                .unwrap();
            w.send_eof().unwrap();

            Ok(Ok(Server {
                server,
                handle: ServerHandle::Netsoup(c, w),
            }))
        }
    }
}
