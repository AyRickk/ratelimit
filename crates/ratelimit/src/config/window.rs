//! The `fixed` window of a `rate_limits:` entry (docs/design.md D8, ADR 0002): any duration,
//! its windows starting at the epoch, at a constant offset, or at a phase hashed from the
//! descriptor.

use ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response::rate_limit::Unit;

/// A `fixed` window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedWindow {
    /// Its length, in seconds.
    pub duration: i64,
    /// Where its windows start.
    pub origin: Origin,
}

/// Where a fixed window's windows start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// At multiples of the duration since the epoch.
    Epoch,
    /// `offset: <duration>`: that many seconds after them, `0 < offset < duration`.
    Offset(i64),
    /// `offset: hashed`: at a phase of the descriptor's, a multiple of `rounding`.
    Hashed { rounding: i64 },
}

impl FixedWindow {
    /// The phase of the windows for a descriptor whose entries are `entries` (`key`, `value`,
    /// after the `share_threshold` substitution).
    pub fn phase<'a>(&self, entries: impl IntoIterator<Item = (&'a str, &'a str)>) -> i64 {
        match self.origin {
            Origin::Epoch => 0,
            Origin::Offset(offset) => offset,
            Origin::Hashed { rounding } => {
                let steps = (self.duration / rounding) as u64;
                (hash(entries) % steps) as i64 * rounding
            }
        }
    }

    /// The start of the window containing `now`.
    pub fn start(&self, now: i64, phase: i64) -> i64 {
        now - (now - phase).rem_euclid(self.duration)
    }

    /// The seconds until the window containing `now` ends (D12).
    pub fn reset(&self, now: i64, phase: i64) -> i64 {
        self.duration - (now - phase).rem_euclid(self.duration)
    }

    /// The window's part of the storage key (ADR 0001): `fx<duration>`, then `o<offset>` or
    /// `oh<rounding>`.
    pub fn shape(&self) -> String {
        match self.origin {
            Origin::Epoch => format!("fx{}", self.duration),
            Origin::Offset(offset) => format!("fx{}o{offset}", self.duration),
            Origin::Hashed { rounding } => format!("fx{}oh{rounding}", self.duration),
        }
    }

    /// The window as `/rlconfig` shows it: `fixed <duration>s`, then its offset.
    pub fn describe(&self) -> String {
        match self.origin {
            Origin::Epoch => format!("fixed {}s", self.duration),
            Origin::Offset(offset) => format!("fixed {}s offset={offset}s", self.duration),
            Origin::Hashed { rounding } => {
                format!("fixed {}s offset=hashed/{rounding}s", self.duration)
            }
        }
    }

    /// The unit the window is reported in, the largest whose length divides the duration,
    /// and how many of them it lasts (D13).
    pub fn unit(&self) -> (Unit, u32) {
        let units = [
            (Unit::Year, 365 * 86_400),
            (Unit::Month, 30 * 86_400),
            (Unit::Week, 7 * 86_400),
            (Unit::Day, 86_400),
            (Unit::Hour, 3_600),
            (Unit::Minute, 60),
            (Unit::Second, 1),
        ];
        let (unit, length) = units
            .into_iter()
            .find(|(_, length)| self.duration % length == 0)
            .expect("a whole number of seconds");
        (unit, (self.duration / length) as u32)
    }
}

/// 64-bit FNV-1a over the entries as `key=value`, joined by NUL (ADR 0002).
fn hash<'a>(entries: impl IntoIterator<Item = (&'a str, &'a str)>) -> u64 {
    let mut input = Vec::new();
    for (i, (key, value)) in entries.into_iter().enumerate() {
        if i > 0 {
            input.push(0);
        }
        input.extend_from_slice(key.as_bytes());
        input.push(b'=');
        input.extend_from_slice(value.as_bytes());
    }
    fnv1a(&input)
}

/// 64-bit FNV-1a.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, b| {
        (hash ^ u64::from(*b)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// The longest duration a window may have: the reported multiplier is a `uint32`.
const MAX_DURATION: i64 = u32::MAX as i64;

/// A duration as written in a window: whole numbers of `w`, `d`, `h`, `m` then `s`, each unit
/// at most once and in that order (`1h30m`), in seconds.
pub(super) fn parse_duration(text: &str) -> Option<i64> {
    if text.is_empty() {
        return None;
    }
    let mut total: i64 = 0;
    let mut rest = text;
    // The longest unit the next number may have.
    let mut longest = i64::MAX;
    while !rest.is_empty() {
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return None;
        }
        let count: i64 = rest[..digits].parse().ok()?;
        let unit = match rest.as_bytes().get(digits) {
            Some(b'w') => 7 * 86_400,
            Some(b'd') => 86_400,
            Some(b'h') => 3_600,
            Some(b'm') => 60,
            Some(b's') => 1,
            _ => return None,
        };
        if unit >= longest {
            return None;
        }
        longest = unit;
        total = total.checked_add(count.checked_mul(unit)?)?;
        rest = &rest[digits + 1..];
    }
    (total <= MAX_DURATION).then_some(total)
}

/// `offset_rounding`'s default: the largest of an hour, a minute and a second that divides
/// the duration (ADR 0002).
pub(super) fn default_rounding(duration: i64) -> i64 {
    [3_600, 60]
        .into_iter()
        .find(|r| duration % r == 0)
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FNV-1a 64's published test vectors.
    #[test]
    fn the_hash_is_fnv_1a_64() {
        assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a(b"foobar"), 0x8594_4171_f739_67e8);
        assert_eq!(
            hash([("user", "alice"), ("org", "x")]),
            fnv1a(b"user=alice\0org=x")
        );
    }
}
