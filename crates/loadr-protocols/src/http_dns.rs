//! Coalesce HTTP DNS lookups across VUs and bound retries before any HTTP send.

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use loadr_core::protocol::Timings;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::Instant;

const CACHE_TTL: Duration = Duration::from_secs(1);
const FAILURE_COOLDOWN: Duration = Duration::from_secs(1);
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_HOSTS: usize = 1024;
const LOOKUP_ATTEMPTS: u32 = 3;
type Addresses = Result<Vec<SocketAddr>, String>;
type Entry = Arc<AsyncMutex<Option<(Instant, Addresses)>>>;

#[derive(Default)]
pub(crate) struct HttpDns {
    entries: Mutex<HashMap<(String, u16), Entry>>,
}

impl HttpDns {
    #[cfg(test)]
    pub(super) async fn prime_for_test(&self, host: &str, port: u16, addresses: Vec<SocketAddr>) {
        self.resolve_with((host.to_owned(), port), || async { Ok(addresses.clone()) })
            .await
            .unwrap();
    }

    pub(crate) async fn resolve(
        &self,
        host: &url::Host<&str>,
        port: u16,
        timings: &mut Timings,
    ) -> Addresses {
        let domain = match host {
            url::Host::Ipv4(ip) => return Ok(vec![SocketAddr::new((*ip).into(), port)]),
            url::Host::Ipv6(ip) => return Ok(vec![SocketAddr::new((*ip).into(), port)]),
            url::Host::Domain(domain) => *domain,
        };
        let start = std::time::Instant::now();
        let result = self
            .resolve_with((domain.to_ascii_lowercase(), port), || async {
                match tokio::time::timeout(LOOKUP_TIMEOUT, tokio::net::lookup_host((domain, port)))
                    .await
                {
                    Ok(Ok(addrs)) => Ok(addrs.collect()),
                    Ok(Err(error)) => Err(format!("dns lookup for `{domain}` failed: {error}")),
                    Err(_) => Err(format!(
                        "dns lookup for `{domain}` exceeded {LOOKUP_TIMEOUT:?}"
                    )),
                }
            })
            .await;
        // Failed lookups and shared-lookup/backoff waits are DNS time too.
        timings.dns_ms += crate::net::ms_since(start);
        result
    }

    async fn resolve_with<F, Fut>(&self, key: (String, u16), lookup: F) -> Addresses
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Addresses>,
    {
        let entry = {
            let mut entries = self.entries.lock().expect("DNS entries lock");
            if entries.len() >= MAX_HOSTS {
                // Never evict an active lookup and start a duplicate for it.
                entries.retain(|_, entry| Arc::strong_count(entry) > 1);
            }
            if let Some(entry) = entries.get(&key) {
                Arc::clone(entry)
            } else {
                let entry = Arc::new(AsyncMutex::new(None));
                if entries.len() < MAX_HOSTS {
                    entries.insert(key.clone(), Arc::clone(&entry));
                }
                entry
            }
        };
        let mut cached = entry.lock().await;
        if let Some((until, result)) = cached.as_ref() {
            if *until > Instant::now() {
                let (until, result) = (*until, result.clone());
                drop(cached);
                if result.is_err() {
                    // Do not turn cached failures into a busy VU retry loop.
                    tokio::time::sleep_until(until).await;
                }
                return result;
            }
        }
        let mut result = Err(format!("dns lookup for `{}` returned no addresses", key.0));
        for attempt in 0..LOOKUP_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(100 << (attempt - 1))).await;
            }
            result = lookup().await.and_then(|addresses| {
                if addresses.is_empty() {
                    Err(format!("dns lookup for `{}` returned no addresses", key.0))
                } else {
                    Ok(addresses)
                }
            });
            if result.is_ok() {
                break;
            }
        }
        let ttl = if result.is_ok() {
            CACHE_TTL
        } else {
            FAILURE_COOLDOWN
        };
        *cached = Some((Instant::now() + ttl, result.clone()));
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    fn key() -> (String, u16) {
        ("test.invalid".into(), 443)
    }
    fn addresses() -> Vec<SocketAddr> {
        vec![SocketAddr::from(([127, 0, 0, 1], 443))]
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_vus_share_one_lookup() {
        let resolver = HttpDns::default();
        let calls = AtomicUsize::new(0);
        let requests = (0..128).map(|_| {
            resolver.resolve_with(key(), || async {
                calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok(addresses())
            })
        });
        for result in futures::future::join_all(requests).await {
            assert_eq!(result.unwrap(), addresses());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn transient_failure_recovers_with_bounded_retries() {
        let resolver = HttpDns::default();
        let calls = AtomicUsize::new(0);
        let result = resolver
            .resolve_with(key(), || async {
                if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                    Err("temporary DNS failure".into())
                } else {
                    Ok(addresses())
                }
            })
            .await;
        assert_eq!(result.unwrap(), addresses());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn failed_burst_is_paced_and_later_requests_can_recover() {
        let resolver = HttpDns::default();
        let calls = AtomicUsize::new(0);
        let requests = (0..128).map(|_| {
            resolver.resolve_with(key(), || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err("temporary DNS failure".into())
            })
        });
        let start = Instant::now();
        for result in futures::future::join_all(requests).await {
            assert!(result.is_err());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert!(start.elapsed() >= FAILURE_COOLDOWN);
        assert_eq!(
            resolver
                .resolve_with(key(), || async { Ok(addresses()) })
                .await
                .unwrap(),
            addresses()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn successful_results_expire_and_empty_results_are_retried() {
        let resolver = HttpDns::default();
        resolver
            .resolve_with(key(), || async { Ok(addresses()) })
            .await
            .unwrap();
        tokio::time::advance(CACHE_TTL).await;
        let calls = AtomicUsize::new(0);
        assert!(resolver
            .resolve_with(key(), || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(Vec::new())
            })
            .await
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_lookup_does_not_poison_other_vus() {
        let resolver = HttpDns::default();
        assert!(tokio::time::timeout(
            Duration::from_millis(5),
            resolver.resolve_with(key(), || async {
                std::future::pending::<Addresses>().await
            })
        )
        .await
        .is_err());
        assert_eq!(
            resolver
                .resolve_with(key(), || async { Ok(addresses()) })
                .await
                .unwrap(),
            addresses()
        );
    }
}
