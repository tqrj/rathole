//! Persisted remote-port allocation table, online state and the mapping directory.

use crate::config::{ServiceType, UserConfig};
use crate::protocol::{Digest, Mapping};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Serialize, Deserialize, Default)]
struct AllocFile {
    #[serde(default)]
    alloc: Vec<Alloc>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct Alloc {
    user: String,
    proto: ServiceType,
    local_port: u16,
    remote_port: u16,
}

/// The current session of an online user
struct Session {
    nonce: Digest,
    /// Local ports registered by that session
    ports: Vec<(ServiceType, u16)>,
    /// Local ports the client turned off
    disabled: Vec<u16>,
}

pub struct Registry {
    path: PathBuf,
    allocs: Vec<Alloc>,
    online: HashMap<String, Session>,
    dir_tx: watch::Sender<Arc<Vec<Mapping>>>,
}

// ponytail: linear scans over the alloc table and a whole-file rewrite on
// every change. Fine for hundreds of users; index by user if it ever isn't.
impl Registry {
    /// Load the table from `path`. A missing file is an empty table.
    pub fn load(path: PathBuf) -> Result<Registry> {
        let allocs = match std::fs::read_to_string(&path) {
            Ok(s) => {
                toml::from_str::<AllocFile>(&s)
                    .with_context(|| format!("Failed to parse {:?}", path))?
                    .alloc
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e).with_context(|| format!("Failed to read {:?}", path)),
        };
        let (dir_tx, _) = watch::channel(Arc::new(Vec::new()));
        let r = Registry {
            path,
            allocs,
            online: HashMap::new(),
            dir_tx,
        };
        r.publish();
        Ok(r)
    }

    pub fn subscribe(&self) -> watch::Receiver<Arc<Vec<Mapping>>> {
        self.dir_tx.subscribe()
    }

    /// Allocate (or reuse) remote ports for the user's configured local ports,
    /// persist, mark the user online and broadcast the new directory.
    /// Returns this user's mappings.
    pub fn register(&mut self, user: &UserConfig, nonce: Digest) -> Result<Vec<Mapping>> {
        let (lo, hi) = user.block;
        let mut wanted: Vec<(ServiceType, u16)> = user
            .tcp_ports
            .iter()
            .map(|p| (ServiceType::Tcp, *p))
            .chain(user.udp_ports.iter().map(|p| (ServiceType::Udp, *p)))
            .collect();
        wanted.sort_unstable();
        wanted.dedup();
        // Local ports that fit in the block get their own number (allocated
        // first so a sequential allocation doesn't take it), the rest the
        // lowest free port
        wanted.sort_by_key(|(_, p)| !(lo..=hi).contains(p));

        // Work on a copy so a failed persist (or an exhausted block) leaves
        // the in-memory table untouched and the next attempt retries the write
        let mut allocs = self.allocs.clone();
        let mut changed = false;
        for (proto, local_port) in &wanted {
            let existing = allocs.iter().position(|a| {
                a.user == user.name && a.proto == *proto && a.local_port == *local_port
            });
            if let Some(i) = existing {
                let rp = allocs[i].remote_port;
                if (lo..=hi).contains(&rp) {
                    continue;
                }
                // Stale allocation outside the current block
                allocs.remove(i);
            }
            let used: Vec<u16> = allocs
                .iter()
                .filter(|a| a.user == user.name)
                .map(|a| a.remote_port)
                .collect();
            let same = Some(*local_port).filter(|p| (lo..=hi).contains(p) && !used.contains(p));
            let remote_port = match same.or_else(|| (lo..=hi).find(|p| !used.contains(p))) {
                Some(p) => p,
                None => bail!(
                    "Port block {}-{} of user {} is exhausted",
                    lo,
                    hi,
                    user.name
                ),
            };
            allocs.push(Alloc {
                user: user.name.clone(),
                proto: *proto,
                local_port: *local_port,
                remote_port,
            });
            changed = true;
        }
        if changed {
            Self::persist(&self.path, &allocs)?;
            self.allocs = allocs;
        }

        self.online.insert(
            user.name.clone(),
            Session {
                nonce,
                ports: wanted,
                disabled: Vec::new(),
            },
        );
        self.publish();

        Ok(self
            .directory()
            .into_iter()
            .filter(|m| m.user == user.name && m.online)
            .collect())
    }

    /// Mark the session offline. Ignored if `nonce` is not the current session of `user`.
    pub fn unregister(&mut self, user: &str, nonce: Digest) {
        if self.online.get(user).map(|s| s.nonce) == Some(nonce) {
            self.online.remove(user);
            self.publish();
        }
    }

    /// Local ports the client turned off. Ignored if `nonce` is not the current session.
    pub fn set_disabled(&mut self, user: &str, nonce: Digest, disabled: Vec<u16>) {
        if let Some(s) = self.online.get_mut(user).filter(|s| s.nonce == nonce) {
            s.disabled = disabled;
            self.publish();
        }
    }

    pub fn directory(&self) -> Vec<Mapping> {
        let mut v: Vec<Mapping> = self
            .allocs
            .iter()
            .map(|a| Mapping {
                user: a.user.clone(),
                proto: a.proto,
                local_port: a.local_port,
                remote_port: a.remote_port,
                online: self.online.get(&a.user).map_or(false, |s| {
                    s.ports.contains(&(a.proto, a.local_port))
                        && !s.disabled.contains(&a.local_port)
                }),
            })
            .collect();
        v.sort_by(|a, b| (&a.user, a.remote_port).cmp(&(&b.user, b.remote_port)));
        v
    }

    fn publish(&self) {
        // send_replace also stores the value when no client is subscribed yet
        self.dir_tx.send_replace(Arc::new(self.directory()));
    }

    fn persist(path: &PathBuf, allocs: &[Alloc]) -> Result<()> {
        let s = toml::to_string(&AllocFile {
            alloc: allocs.to_vec(),
        })?;
        std::fs::write(path, s).with_context(|| format!("Failed to write {:?}", path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(name: &str, lo: u16, hi: u16, tcp: &[u16], udp: &[u16]) -> UserConfig {
        UserConfig {
            name: name.into(),
            block: (lo, hi),
            tcp_ports: tcp.to_vec(),
            udp_ports: udp.to_vec(),
            ..Default::default()
        }
    }

    fn remote(m: &[Mapping], proto: ServiceType, local: u16) -> Option<u16> {
        m.iter()
            .find(|x| x.proto == proto && x.local_port == local)
            .map(|x| x.remote_port)
    }

    #[test]
    fn test_registry() {
        let path =
            std::env::temp_dir().join(format!("rathole_registry_test_{}.toml", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let alice = |tcp: &[u16], udp: &[u16]| user("alice", 20000, 20002, tcp, udp);
        let n1 = [1u8; 32];
        let n2 = [2u8; 32];

        let mut r = Registry::load(path.clone()).unwrap();
        let m = r.register(&alice(&[8000, 3000], &[]), n1).unwrap();
        // A subscriber created after the first registration (no receiver was
        // alive during `publish`) must still see the mappings
        assert_eq!(r.subscribe().borrow().len(), 2);
        assert_eq!(remote(&m, ServiceType::Tcp, 3000), Some(20000));
        assert_eq!(remote(&m, ServiceType::Tcp, 8000), Some(20001));
        assert!(m.iter().all(|x| x.online));

        // Stable across re-registration and a smaller port set
        let m = r.register(&alice(&[8000], &[]), n2).unwrap();
        assert_eq!(remote(&m, ServiceType::Tcp, 8000), Some(20001));
        assert_eq!(m.len(), 1);
        let d = r.directory();
        assert_eq!(d.len(), 2);
        assert!(!d.iter().find(|x| x.local_port == 3000).unwrap().online);

        // Stale session cannot mark the user offline
        r.unregister("alice", n1);
        assert!(r.directory().iter().any(|x| x.online));
        r.unregister("alice", n2);
        assert!(r.directory().iter().all(|x| !x.online));

        // UDP shares the block; exhaustion is an error
        let m = r.register(&alice(&[3000, 8000], &[9000]), n1).unwrap();
        assert_eq!(remote(&m, ServiceType::Udp, 9000), Some(20002));
        assert!(r
            .register(&alice(&[3000, 8000, 8001], &[9000]), n1)
            .is_err());

        // Survives a reload
        let r2 = Registry::load(path.clone()).unwrap();
        let d = r2.directory();
        assert_eq!(remote(&d, ServiceType::Tcp, 3000), Some(20000));
        assert_eq!(remote(&d, ServiceType::Udp, 9000), Some(20002));
        assert!(d.iter().all(|x| !x.online));

        // Block change re-allocates stale ports
        let alice2 = user("alice", 30000, 30001, &[3000], &[]);
        let mut r3 = r2;
        let m = r3.register(&alice2, n1).unwrap();
        assert_eq!(remote(&m, ServiceType::Tcp, 3000), Some(30000));

        // A local port inside the block keeps its number, even if listed later
        let dev = user("dev", 5000, 5002, &[3000, 5001], &[]);
        let m = r3.register(&dev, n2).unwrap();
        assert_eq!(remote(&m, ServiceType::Tcp, 5001), Some(5001));
        assert_eq!(remote(&m, ServiceType::Tcp, 3000), Some(5000));

        // The client can turn a port off; a stale nonce can't
        r3.set_disabled("dev", n1, vec![5001]);
        assert!(m.iter().all(|x| x.online));
        r3.set_disabled("dev", n2, vec![5001]);
        let d = r3.directory();
        assert!(!d.iter().find(|x| x.local_port == 5001).unwrap().online);
        assert!(d.iter().find(|x| x.local_port == 3000).unwrap().online);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_persist_failure_rolls_back() {
        let dir = std::env::temp_dir().join(format!("rathole_missing_dir_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut r = Registry::load(dir.join("alloc.toml")).unwrap();
        let alice = user("alice", 20000, 20001, &[3000], &[]);
        assert!(r.register(&alice, [1u8; 32]).is_err());
        assert!(r.directory().is_empty());
        // Once the directory exists the retry persists
        std::fs::create_dir_all(&dir).unwrap();
        assert_eq!(r.register(&alice, [1u8; 32]).unwrap().len(), 1);
        assert!(dir.join("alloc.toml").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
