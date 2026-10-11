//! `SimSocket` — `DatagramSocket` over a [`SimNet`] mailbox.

use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bitdaemon_net::transport::{BoxFut, DatagramSocket};

use crate::net::{Mailbox, SimNet};

/// Simulated datagram socket — `send_to` schedules on the net,
/// `recv_from` drains the mailbox.
pub struct SimSocket {
    /// local address.
    pub local: SocketAddr,
    net: Arc<Mutex<SimNet>>,
    mb: Arc<Mutex<Mailbox>>,
}

impl Debug for SimSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimSocket")
            .field("local", &self.local)
            .finish()
    }
}

impl SimSocket {
    /// bind `local` on `net`.
    #[must_use]
    pub fn bind(local: SocketAddr, net: Arc<Mutex<SimNet>>) -> Self {
        let mb = net
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .register(local);
        Self { local, net, mb }
    }
}

impl DatagramSocket for SimSocket {
    fn send_to<'a>(&'a self, buf: &'a [u8], target: SocketAddr) -> BoxFut<'a, usize> {
        let data = buf.to_vec();
        let n = data.len();
        let local = self.local;
        self.net
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .send(local, target, data);
        Box::pin(async move { Ok(n) })
    }

    fn recv_from<'a>(&'a self, buf: &'a mut [u8]) -> BoxFut<'a, (usize, SocketAddr)> {
        let mb = Arc::clone(&self.mb);
        Box::pin(async move {
            loop {
                {
                    let mut g = mb.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some((from, data)) = g.queue.pop_front() {
                        let n = data.len().min(buf.len());
                        buf[..n].copy_from_slice(&data[..n]);
                        return Ok((n, from));
                    }
                }
                // park until the next step wakes us
                tokio::task::yield_now().await;
            }
        })
    }

    fn local_addr(&self) -> SocketAddr {
        self.local
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_recv() {
        let net = Arc::new(Mutex::new(SimNet::default()));
        let a = SimSocket::bind("10.0.0.1:1".parse().unwrap(), net.clone());
        let b = SimSocket::bind("10.0.0.2:2".parse().unwrap(), net.clone());
        a.send_to(b"ping", b.local_addr()).await.unwrap();
        {
            net.lock().unwrap().step();
        }
        let mut buf = [0u8; 16];
        let (n, from) = b.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"ping");
        assert_eq!(from, a.local_addr());
    }
}
