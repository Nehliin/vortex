use std::{
    cell::RefCell,
    net::Ipv4Addr,
    rc::Rc,
    time::{Duration, Instant},
};

use ahash::AHashMap;
use bytes::Bytes;
use sha1::{Digest, Sha1};

const ZERO: [u8; 16] = [0; 16];
#[cfg(not(test))]
const SECRET_REFRESH_TIME: Duration = Duration::from_secs(60 * 5);
#[cfg(test)]
const SECRET_REFRESH_TIME: Duration = Duration::from_secs(3);

#[derive(Clone)]
pub(crate) struct TokenStore {
    // Random secret that changes every SECRET_REFRESH_TIME
    // used to generate tokens
    current_secret: Rc<RefCell<[u8; 16]>>,
    prev_secret: Rc<RefCell<[u8; 16]>>,
    received_tokens: Rc<RefCell<AHashMap<Ipv4Addr, (Instant, Bytes)>>>,
    shutdown: Rc<RefCell<bool>>,
}

impl TokenStore {
    pub fn new() -> TokenStore {
        let secret = rand::random::<[u8; 16]>();
        let current_secret = Rc::new(RefCell::new(secret));
        let prev_secret = Rc::new(RefCell::new(ZERO));
        let shutdown = Rc::new(RefCell::new(false));

        let shutdown_clone = shutdown.clone();
        let current_secret_clone = current_secret.clone();
        let prev_secret_clone = prev_secret.clone();
        tokio_uring::spawn(async move {
            while !*shutdown_clone.borrow() {
                tokio::time::sleep(SECRET_REFRESH_TIME).await;
                let mut current_secret = current_secret_clone.borrow_mut();
                let mut prev_secret = prev_secret_clone.borrow_mut();
                *prev_secret = *current_secret;
                *current_secret = rand::random::<[u8; 16]>();
            }
        });

        Self {
            current_secret,
            prev_secret,
            shutdown,
            received_tokens: Default::default(),
        }
    }

    pub fn validate(&self, ip: Ipv4Addr, token: Bytes) -> bool {
        let current_secret = self.current_secret.borrow();
        let prev_secret = self.prev_secret.borrow();
        let mut hasher = Sha1::new();
        hasher.update(ip.octets());
        hasher.update(*current_secret);
        let hash = hasher.finalize();
        if hash.as_slice() == token {
            true
        } else if *prev_secret != ZERO {
            // If there is a prev secret compare against that
            let mut hasher = Sha1::new();
            hasher.update(ip.octets());
            hasher.update(*prev_secret);
            let hash = hasher.finalize();
            hash.as_slice() == token
        } else {
            false
        }
    }

    pub fn generate(&self, ip: Ipv4Addr) -> Bytes {
        let current_secret = self.current_secret.borrow();
        let mut hasher = Sha1::new();
        hasher.update(ip.octets());
        hasher.update(*current_secret);
        let hash = hasher.finalize();
        Bytes::copy_from_slice(hash.as_slice())
    }

    #[inline]
    pub fn get_token(&self, ip: Ipv4Addr) -> Option<Bytes> {
        let mut received_tokens = self.received_tokens.borrow_mut();
        let (inserted_at, token) = received_tokens.remove(&ip)?;
        if Instant::now() - inserted_at <= (SECRET_REFRESH_TIME * 2) {
            received_tokens.insert(ip, (inserted_at, token.clone()));
            Some(token)
        } else {
            None
        }
    }

    #[inline]
    pub fn store_token(&self, ip: Ipv4Addr, token: Bytes) {
        let mut received_tokens = self.received_tokens.borrow_mut();
        received_tokens.insert(ip, (Instant::now(), token));
    }
}

impl Drop for TokenStore {
    fn drop(&mut self) {
        *self.shutdown.borrow_mut() = true;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic_token_validation() {
        tokio_uring::start(async {
            let token_store = TokenStore::new();
            let ip = Ipv4Addr::new(127, 3, 4, 5);
            let generated = token_store.generate(ip);
            assert!(token_store.validate(ip, generated));
        });
    }

    #[test]
    fn one_generation_older_token_is_valid() {
        tokio_uring::start(async {
            let token_store = TokenStore::new();
            let ip = Ipv4Addr::new(127, 3, 4, 5);
            let generated = token_store.generate(ip);
            tokio::time::sleep(SECRET_REFRESH_TIME + Duration::from_secs(1)).await;
            assert!(token_store.validate(ip, generated));
        });
    }

    #[test]
    fn two_generations_older_token_is_invalid() {
        tokio_uring::start(async {
            let token_store = TokenStore::new();
            let ip = Ipv4Addr::new(127, 3, 4, 5);
            let generated = token_store.generate(ip);
            tokio::time::sleep(SECRET_REFRESH_TIME * 2 + Duration::from_secs(1)).await;
            assert!(!token_store.validate(ip, generated));
        });
    }

    #[test]
    fn does_not_store_tokens_forever() {
        tokio_uring::start(async {
            let token_store = TokenStore::new();
            let ip = Ipv4Addr::new(127, 3, 4, 5);
            let generated = token_store.generate(ip);
            token_store.store_token(ip, generated);
            tokio::time::sleep(SECRET_REFRESH_TIME).await;
            assert!(token_store.get_token(ip).is_some());
            assert!(token_store.get_token(ip).is_some());
            tokio::time::sleep(SECRET_REFRESH_TIME + Duration::from_secs(1)).await;
            assert!(token_store.get_token(ip).is_none());
        });
    }
}
