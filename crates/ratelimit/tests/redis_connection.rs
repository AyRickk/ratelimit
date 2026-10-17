//! How the service reaches Redis and retries at startup, from its settings (radix's and
//! upstream's rules; the binary itself is compared with upstream's in `differential.rs`).

use ratelimit::redis::{RedisConnection, StartupBackoff, mask_credentials_in_url};
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

fn credentials(c: &RedisConnection) -> (&str, &str, &str, &str) {
    (&c.address, &c.username, &c.password, &c.database)
}

#[test]
fn the_default_is_the_nutcracker_socket() {
    let c = connection(&[]).unwrap();
    assert_eq!(
        (c.network.as_str(), c.address.as_str()),
        ("unix", "/var/run/nutcracker/ratelimit.sock")
    );
    assert_eq!((c.pool_size, c.timeout), (10, Duration::from_secs(10)));
    assert_eq!(
        credentials(&c),
        ("/var/run/nutcracker/ratelimit.sock", "", "", "")
    );
}

/// `REDIS_AUTH` is `user:password` or a password; radix fills in what is still empty from a
/// `redis://` URL (user info, path, then the query's first values).
#[test]
fn credentials_come_from_redis_auth_then_the_url() {
    let with = |url: &str, auth: &str| {
        let c = connection(&[("REDIS_URL", url), ("REDIS_AUTH", auth)]).unwrap();
        let (a, u, p, d) = credentials(&c);
        (a.to_string(), u.to_string(), p.to_string(), d.to_string())
    };
    let owned = |a: &str, u: &str, p: &str, d: &str| (a.into(), u.into(), p.into(), d.into());
    assert_eq!(with("h:1", "user:pa:ss"), owned("h:1", "user", "pa:ss", ""));
    assert_eq!(with("h:1", "secret"), owned("h:1", "", "secret", ""));
    assert_eq!(with("h:1", "user:"), owned("h:1", "user", "", ""));
    assert_eq!(
        with("redis://bob@h:1", ":pw"),
        owned("h:1", "bob", "pw", "")
    );
    assert_eq!(
        with("redis://u%40x:p%2B@db.example:6380/3?password=q", ""),
        owned("db.example:6380", "u@x", "p+", "3")
    );
    assert_eq!(
        with("redis://h:1?username=a+b&password=c&db=2&db=9", "pw"),
        owned("h:1", "a b", "pw", "2")
    );
    // A key without a value is empty; a pair with `;` is dropped; a fragment is not the host.
    assert_eq!(
        with("redis://h:1?username&username=x;y&db=05#frag", ""),
        owned("h:1", "", "", "05")
    );
    assert_eq!(with("redis://h:1/abc/", ""), owned("h:1", "", "", "abc/"));
    // What Go cannot parse is dialled as it is.
    assert_eq!(
        with("redis://u:%zz@h:1", ""),
        owned("redis://u:%zz@h:1", "", "", "")
    );
}

/// Upstream's masking, and the query's credentials too (`docs/parity.md` §7).
#[test]
fn credentials_are_masked_in_each_url() {
    assert_eq!(
        mask_credentials_in_url("redis://a:b@h1:1,h2:2,redis://c@d@h3:3"),
        "redis://*****@h1:1,h2:2,redis://*****@h3:3"
    );
    assert_eq!(
        mask_credentials_in_url("redis://h:1?username=u&password=p&db=2"),
        "redis://h:1?username=*****&password=*****&db=2"
    );
    assert_eq!(
        mask_credentials_in_url("redis://h:1?pass%77ord=p&db=2"),
        "redis://h:1?pass%77ord=*****&db=2"
    );
}

/// radix's pool size defaults to 4; upstream refuses CREATE and ERROR, and unknown types.
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
    assert!(connection(&[("REDIS_TYPE", "Single")]).is_ok());
    assert_eq!(
        connection(&[("REDIS_TYPE", "FOO")]).unwrap_err(),
        "Unrecognized redis type FOO"
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
