//! How the service reaches Redis and retries at startup, from its settings (radix's and
//! upstream's rules; the binary itself is compared with upstream's in `differential.rs`).

use ratelimit::redis::{RedisAddress, RedisConnection, StartupBackoff, mask_credentials_in_url};
use ratelimit::settings::Settings;
use std::time::Duration;

fn connection(environment: &[(&str, &str)]) -> Result<RedisConnection, String> {
    let settings = Settings::from_lookup(&|key| {
        environment
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v.to_string())
    })
    .unwrap();
    RedisConnection::from_settings(&settings)
}

#[test]
fn the_default_is_the_nutcracker_socket() {
    let c = connection(&[]).unwrap();
    assert_eq!(
        c.address,
        RedisAddress::Unix("/var/run/nutcracker/ratelimit.sock".into())
    );
    assert_eq!((c.pool_size, c.timeout), (10, Duration::from_secs(10)));
    assert_eq!((c.username, c.password, c.database), (None, None, None));
}

/// `REDIS_AUTH` is `user:password` or a password; a `redis://` URL fills in what it lacks.
#[test]
fn credentials_come_from_redis_auth_then_the_url() {
    let tcp = [("REDIS_SOCKET_TYPE", "tcp")];
    let with = |more: &[(&'static str, &'static str)]| {
        let environment: Vec<_> = tcp.iter().chain(more).copied().collect();
        connection(&environment).unwrap()
    };
    let c = with(&[("REDIS_URL", "h:1"), ("REDIS_AUTH", "user:pa:ss")]);
    assert_eq!(
        (c.username.as_deref(), c.password.as_deref()),
        (Some("user"), Some("pa:ss"))
    );
    let c = with(&[("REDIS_URL", "h:1"), ("REDIS_AUTH", "secret")]);
    assert_eq!(
        (c.username.as_deref(), c.password.as_deref()),
        (None, Some("secret"))
    );
    let c = with(&[(
        "REDIS_URL",
        "redis://u%40x:p%2B@db.example:6380/3?password=q",
    )]);
    assert_eq!(
        c.address,
        RedisAddress::Tcp {
            host: "db.example".into(),
            port: 6380
        }
    );
    assert_eq!(
        (c.username.as_deref(), c.password.as_deref(), c.database),
        (Some("u@x"), Some("p+"), Some(3))
    );
    let c = with(&[
        ("REDIS_URL", "redis://h:1?username=a+b&password=c&db=2"),
        ("REDIS_AUTH", "pw"),
    ]);
    assert_eq!(
        (c.username.as_deref(), c.password.as_deref(), c.database),
        (Some("a b"), Some("pw"), Some(2))
    );
    assert_eq!(c.masked_url, "redis://h:1?username=a+b&password=c&db=2");
    let c = with(&[("REDIS_URL", "redis://u:p@h:1")]);
    assert_eq!(c.masked_url, "redis://*****@h:1");
}

#[test]
fn credentials_are_masked_in_each_url() {
    assert_eq!(
        mask_credentials_in_url("redis://a:b@h1:1,h2:2,redis://c@d@h3:3"),
        "redis://*****@h1:1,h2:2,redis://*****@h3:3"
    );
}

/// radix's pool size defaults to 4; upstream refuses CREATE and ERROR.
#[test]
fn pool_settings_like_upstream() {
    assert_eq!(
        connection(&[("REDIS_POOL_SIZE", "0")]).unwrap().pool_size,
        4
    );
    assert!(connection(&[("REDIS_POOL_SIZE", "-1")]).is_err());
    let error = connection(&[("REDIS_POOL_ON_EMPTY_BEHAVIOR", "Error")]).unwrap_err();
    assert!(
        error.starts_with("REDIS_POOL_ON_EMPTY_BEHAVIOR=ERROR is not supported"),
        "{error}"
    );
    assert!(connection(&[("REDIS_POOL_ON_EMPTY_BEHAVIOR", "anything")]).is_ok());
    assert_eq!(
        connection(&[("REDIS_TYPE", "Single")]).unwrap().address,
        connection(&[]).unwrap().address
    );
    assert_eq!(
        connection(&[("REDIS_TYPE", "x")]).unwrap_err(),
        "Unrecognized redis type x"
    );
}

/// jpillora/backoff: between `min` and `min × 2^attempt`, never beyond `[min, max]`.
#[test]
fn startup_retries_back_off_like_upstream() {
    let backoff = StartupBackoff {
        min: Duration::from_secs(1),
        max: Duration::from_secs(30),
        max_elapsed: Duration::ZERO,
    };
    for attempt in 0..10 {
        let d = backoff.duration(attempt);
        let ceiling = Duration::from_secs(1 << attempt).min(Duration::from_secs(30));
        assert!(
            d >= Duration::from_secs(1) && d <= ceiling,
            "{attempt}: {d:?}"
        );
    }
    let defaults = StartupBackoff {
        min: Duration::ZERO,
        max: Duration::ZERO,
        max_elapsed: Duration::ZERO,
    };
    assert!(defaults.duration(0) >= Duration::from_millis(100));
    let inverted = StartupBackoff {
        min: Duration::from_secs(5),
        max: Duration::from_secs(1),
        max_elapsed: Duration::ZERO,
    };
    assert_eq!(inverted.duration(3), Duration::from_secs(1));
}
