//! Small networking helpers shared by the protocol handlers.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use futures::stream::{FuturesUnordered, StreamExt as _};
use loadr_core::protocol::Timings;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

/// Delay before racing the next resolved address against an in-flight TCP
/// connection attempt. Failed attempts advance immediately.
const HAPPY_EYEBALLS_DELAY: Duration = Duration::from_millis(250);

/// Milliseconds elapsed since `start` as `f64`.
pub(crate) fn ms_since(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

/// Object-safe async byte stream (TCP or TLS) used behind `Box<dyn _>`.
pub(crate) trait IoStream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> IoStream for T {}

/// Resolve `host:port`, recording DNS time into `timings.dns_ms` when an
/// actual lookup happens (IP literals resolve instantly with `dns_ms = 0`).
pub(crate) async fn resolve(
    host: &url::Host<&str>,
    port: u16,
    timings: &mut Timings,
) -> Result<SocketAddr, String> {
    resolve_all(host, port, timings)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| format!("dns lookup for `{host}` returned no addresses"))
}

/// Resolve `host:port` to every available address, recording DNS time into
/// `timings.dns_ms` when an actual lookup happens.
pub(crate) async fn resolve_all(
    host: &url::Host<&str>,
    port: u16,
    timings: &mut Timings,
) -> Result<Vec<SocketAddr>, String> {
    match host {
        url::Host::Ipv4(ip) => Ok(vec![SocketAddr::new((*ip).into(), port)]),
        url::Host::Ipv6(ip) => Ok(vec![SocketAddr::new((*ip).into(), port)]),
        url::Host::Domain(domain) => {
            let start = Instant::now();
            let addrs: Vec<_> = tokio::net::lookup_host((domain.to_string(), port))
                .await
                .map_err(|e| format!("dns lookup for `{domain}` failed: {e}"))?
                .collect();
            timings.dns_ms = ms_since(start);
            if addrs.is_empty() {
                Err(format!("dns lookup for `{domain}` returned no addresses"))
            } else {
                Ok(addrs)
            }
        }
    }
}

/// Connect to a resolved address set with a small Happy Eyeballs race.
///
/// Resolver preference is retained, while IPv4 and IPv6 addresses are
/// interleaved so one unavailable family cannot block the other.
pub(crate) async fn connect_tcp(addrs: &[SocketAddr]) -> Result<TcpStream, String> {
    let mut remaining = interleave_address_families(addrs);
    let Some(first) = remaining.pop_front() else {
        return Err("cannot connect without a resolved address".to_string());
    };

    let mut attempts = FuturesUnordered::new();
    attempts.push(connect_attempt(first));
    let delay = tokio::time::sleep(HAPPY_EYEBALLS_DELAY);
    tokio::pin!(delay);
    let mut errors = Vec::new();

    while !attempts.is_empty() {
        tokio::select! {
            result = attempts.next() => {
                let Some((addr, result)) = result else {
                    break;
                };
                match result {
                    Ok(stream) => return Ok(stream),
                    Err(error) => errors.push(format!("{addr}: {error}")),
                }

                // An early failure should not make the next address wait for
                // the stagger timer.
                if let Some(addr) = remaining.pop_front() {
                    attempts.push(connect_attempt(addr));
                    delay.as_mut().reset(tokio::time::Instant::now() + HAPPY_EYEBALLS_DELAY);
                }
            }
            () = &mut delay, if !remaining.is_empty() => {
                if let Some(addr) = remaining.pop_front() {
                    attempts.push(connect_attempt(addr));
                    delay.as_mut().reset(tokio::time::Instant::now() + HAPPY_EYEBALLS_DELAY);
                }
            }
        }
    }

    Err(format!(
        "all connection attempts failed ({})",
        errors.join("; ")
    ))
}

async fn connect_attempt(addr: SocketAddr) -> (SocketAddr, std::io::Result<TcpStream>) {
    (addr, TcpStream::connect(addr).await)
}

fn interleave_address_families(addrs: &[SocketAddr]) -> VecDeque<SocketAddr> {
    let Some(first) = addrs.first() else {
        return VecDeque::new();
    };
    let prefer_ipv6 = first.is_ipv6();
    let (mut ipv6, mut ipv4): (VecDeque<_>, VecDeque<_>) =
        addrs.iter().copied().partition(SocketAddr::is_ipv6);
    let mut ordered = VecDeque::with_capacity(addrs.len());

    while !ipv4.is_empty() || !ipv6.is_empty() {
        let (preferred, alternate) = if prefer_ipv6 {
            (&mut ipv6, &mut ipv4)
        } else {
            (&mut ipv4, &mut ipv6)
        };
        if let Some(addr) = preferred.pop_front() {
            ordered.push_back(addr);
        }
        if let Some(addr) = alternate.pop_front() {
            ordered.push_back(addr);
        }
    }

    ordered
}

/// Extract `(host, port)` from a URL, failing when either is missing.
pub(crate) fn host_port(url: &url::Url) -> Result<(String, u16), String> {
    let host = url
        .host_str()
        .ok_or_else(|| format!("url `{url}` has no host"))?
        .to_string();
    let port = url
        .port_or_known_default()
        .ok_or_else(|| format!("url `{url}` has no port"))?;
    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connect_tcp_tries_later_addresses() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let working = listener.local_addr().expect("listener address");
        let unavailable = SocketAddr::from(([127, 0, 0, 1], 0));
        let addrs = [unavailable, working];

        let (connected, accepted) = tokio::join!(connect_tcp(&addrs), listener.accept());
        connected.expect("fall back to working address");
        accepted.expect("accept fallback connection");
    }

    #[test]
    fn interleaves_address_families_in_resolver_order() {
        let v6a = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], 1));
        let v6b = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 2], 2));
        let v4a = SocketAddr::from(([127, 0, 0, 1], 3));
        let v4b = SocketAddr::from(([127, 0, 0, 2], 4));

        assert_eq!(
            interleave_address_families(&[v6a, v6b, v4a, v4b]),
            VecDeque::from([v6a, v4a, v6b, v4b])
        );
    }
}
