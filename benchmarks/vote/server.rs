use super::ssh::Ssh;
use super::Backend;
use ssh2;
use std::error::Error;
use std::borrow::Cow;
use std::{thread, time};
use std::io;
use std::io::prelude::*;

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

                /*
                let mut cstdout = String::new();
                let mut cstderr = String::new();
                c.stderr().read_to_string(&mut cstderr)?;
                c.read_to_string(&mut cstdout)?;
                println!("{}", cstdout);
                println!("{}", cstderr);

                let mut wstdout = String::new();
                let mut wstderr = String::new();
                w.stderr().read_to_string(&mut wstderr)?;
                w.read_to_string(&mut wstdout)?;
                println!("{}", wstdout);
                println!("{}", wstderr);
                */

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
            Backend::Mysql | Backend::Mssql => Ok(self),
        }
    }

    pub(crate) fn wait(&mut self, client: &Ssh, backend: &Backend) -> Result<(), Box<Error>> {
        if let Backend::Netsoup { .. } = *backend {
            // netsoup *worker* doesn't have a well-defined port :/
            thread::sleep(time::Duration::from_secs(10));
            return Ok(());
        }

        let start = time::Instant::now();
        client.set_timeout(5000);
        // sql server can be *really* slow to start
        while start.elapsed() < time::Duration::from_secs(30) {
            let e: Result<(), ssh2::Error> = do catch {
                let mut c = client.channel_direct_tcpip(self.listen_addr, backend.port(), None)?;
                c.send_eof()?;
                c.wait_eof()?;
                Ok(())
            };

            if let Err(e) = e {
                if e.code() == -21 {
                    // "connect failed"
                    continue;
                }
                Err(e)?;
            } else {
                return Ok(());
            }
        }
        Err("server never started".into())
    }

    fn get_pid(&mut self, pgrep: &str) -> Result<Option<usize>, Box<Error>> {
        let mut c = self.server.exec(&["pgrep", pgrep])?;
        let mut stdout = String::new();
        c.read_to_string(&mut stdout)?;
        c.wait_eof()?;

        Ok(stdout.lines().next().and_then(|line| line.parse().ok()))
    }

    fn get_mem(&mut self, pgrep: &str) -> Result<Option<usize>, Box<Error>> {
        let pid = self.get_pid(pgrep)?.ok_or("couldn't find server pid")?;
        let f = format!("/proc/{}/status", pid);
        let mut c = self.server.exec(&["grep", "VmRSS", &f])?;

        let mut stdout = String::new();
        c.read_to_string(&mut stdout)?;
        c.wait_eof()?;

        Ok(stdout
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().skip(1).next())
            .and_then(|col| col.parse().ok()))
    }

    pub(crate) fn write_stats(
        &mut self,
        backend: &Backend,
        w: &mut io::Write,
    ) -> Result<(), Box<Error>> {
        match *backend {
            Backend::Memcached => {
                let mem = self.get_mem("memcached")?
                    .ok_or("couldn't find memcached memory usage")?;
                w.write_all(format!("memory: {}", mem).as_bytes())?;
            }
            Backend::Netsoup { .. } => {
                let mem = self.get_mem("souplet")?
                    .ok_or("couldn't find souplet memory usage")?;
                w.write_all(format!("memory: {}", mem).as_bytes())?;
            }
            Backend::Mysql => {
                let mut c = self.server.exec(&[
                    "mysql",
                    "-N",
                    "-t",
                    "-u",
                    "soup",
                    "soup",
                    "<",
                    "distributary/benchmarks/vote/mysql_stat.sql",
                ])?;

                w.write_all(b"tables:\n")?;
                io::copy(&mut c, w)?;
                c.wait_eof()?;
            }
            Backend::Mssql => {
                let mut c = self.server.exec(&["du", "-s", "/opt/mssql-ramdisk/data/"])?;
                w.write_all(b"disk:\n")?;
                io::copy(&mut c, w)?;
                c.wait_eof()?;

                let mut c = self.server.exec(&[
                    "/opt/mssql-tools/bin/sqlcmd",
                    "-U",
                    "SA",
                    "-i",
                    "distributary/benchmarks/vote/mssql_stat.sql",
                    // assume password is set in SQLCMDPASSWORD
                    "-S",
                    "127.0.0.1",
                    "-I",
                    "-d",
                    "soup",
                    "-h",
                    "-1",
                ])?;
                w.write_all(b"tables:\n")?;
                io::copy(&mut c, w)?;
                c.wait_eof()?;
            }
        }
        Ok(())
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
                .just_exec(&["echo", "-n", ">", "/dev/tcp/127.0.0.1/2181"])?
                .is_err()
            {
                if start.elapsed() > time::Duration::from_secs(10) {
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
            // TODO: should we worry about the running directory being on an SSD here?
            let mut cmd: Vec<Cow<str>> = ["cd", "distributary", "&&"]
                .into_iter()
                .map(|&s| s.into())
                .collect();
            if has_pl {
                cmd.push("perflock".into());
            }
            cmd.extend(vec![
                "target/release/souplet".into(),
                "--listen".into(),
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
