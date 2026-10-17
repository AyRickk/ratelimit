//! Ports of the Go standard library functions whose exact behaviour upstream exposes: parsing
//! (`strconv`, `time.ParseDuration`) and quoting, with Go's error texts.

use std::fmt::Write;

/// `strconv.Quote`, for error messages: ASCII control characters escaped, other characters
/// as they are (Go also escapes non-printable non-ASCII characters).
pub fn quote(s: &str) -> String {
    let mut out = String::from('"');
    for c in s.chars() {
        match c {
            '"' => out += "\\\"",
            '\\' => out += "\\\\",
            '\x07' => out += "\\a",
            '\x08' => out += "\\b",
            '\x0c' => out += "\\f",
            '\n' => out += "\\n",
            '\r' => out += "\\r",
            '\t' => out += "\\t",
            '\x0b' => out += "\\v",
            c if (c as u32) < 0x20 || c == '\x7f' => {
                let _ = write!(out, "\\x{:02x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// `strconv.FormatFloat(v, 'g', -1, 64)`: the shortest digits, in exponent form below 1e-4
/// and from 1e6 (`1e+06`); `+Inf`, `-Inf`, `NaN`.
pub fn format_float(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 { "+Inf" } else { "-Inf" }.to_string();
    }
    if v == 0.0 {
        return if v.is_sign_negative() { "-0" } else { "0" }.to_string();
    }
    // Shortest round-trip digits and decimal exponent, from Rust's `{:e}` formatting.
    let sci = format!("{v:e}");
    let (mantissa, exp) = sci.split_once('e').unwrap();
    let exp: i32 = exp.parse().unwrap();
    // With the shortest representation, Go switches to %e at exponent 6.
    if !(-4..6).contains(&exp) {
        let exp_sign = if exp < 0 { '-' } else { '+' };
        return format!("{mantissa}e{exp_sign}{:02}", exp.abs());
    }
    format!("{v}")
}

/// Why a `strconv` parser failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumError {
    Syntax,
    Range,
}

/// A `*strconv.NumError`'s text.
fn num_error(function: &str, text: &str, error: NumError) -> String {
    let reason = match error {
        NumError::Syntax => "invalid syntax",
        NumError::Range => "value out of range",
    };
    format!("strconv.{function}: parsing {}: {reason}", quote(text))
}

/// `strconv.ParseBool`.
pub fn parse_bool(text: &str) -> Result<bool, String> {
    match text {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(num_error("ParseBool", text, NumError::Syntax)),
    }
}

/// `strconv.Atoi`: a decimal integer with an optional sign, without underscores.
pub fn atoi(text: &str) -> Result<i64, String> {
    let error = |e| num_error("Atoi", text, e);
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(error(NumError::Syntax));
    }
    text.parse::<i64>().map_err(|_| error(NumError::Range))
}

/// Whether the underscores in a number are where Go allows them: only between digits, or
/// between a base prefix and a digit (`underscoreOK`).
fn underscores_ok(s: &str) -> bool {
    let s = s.strip_prefix(['+', '-']).unwrap_or(s);
    let bytes = s.as_bytes();
    let mut saw = b'^';
    let mut i = 0;
    let mut hex = false;
    if bytes.len() >= 2 && bytes[0] == b'0' && matches!(bytes[1] | 0x20, b'b' | b'o' | b'x') {
        i = 2;
        saw = b'0';
        hex = bytes[1] | 0x20 == b'x';
    }
    for &c in &bytes[i..] {
        if c.is_ascii_digit() || (hex && (c | 0x20).is_ascii_hexdigit()) {
            saw = b'0';
        } else if c == b'_' {
            if saw != b'0' {
                return false;
            }
            saw = b'_';
        } else {
            if saw == b'_' {
                return false;
            }
            saw = b'!';
        }
    }
    saw != b'_'
}

/// `strconv.ParseInt(text, 0, bits)`: a sign, then a `0x`, `0o`, `0b` or `0` (octal) prefix,
/// with `_` between digits.
pub fn parse_int(text: &str, bits: u32) -> Result<i64, String> {
    let error = |e| num_error("ParseInt", text, e);
    let (negative, unsigned) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let magnitude = parse_uint(unsigned).map_err(error)?;
    let cutoff = 1u64 << (bits - 1);
    match negative {
        false if magnitude < cutoff => Ok(magnitude as i64),
        true if magnitude <= cutoff => Ok((magnitude as i64).wrapping_neg()),
        _ => Err(error(NumError::Range)),
    }
}

/// `strconv.ParseUint(text, 0, 64)`.
fn parse_uint(text: &str) -> Result<u64, NumError> {
    if text.is_empty() {
        return Err(NumError::Syntax);
    }
    let bytes = text.as_bytes();
    let (base, digits) = match bytes {
        [b'0', p, _, ..] if *p | 0x20 == b'b' => (2, &text[2..]),
        [b'0', p, _, ..] if *p | 0x20 == b'o' => (8, &text[2..]),
        [b'0', p, _, ..] if *p | 0x20 == b'x' => (16, &text[2..]),
        [b'0', ..] => (8, &text[1..]),
        _ => (10, text),
    };
    let mut value: u64 = 0;
    let mut underscores = false;
    for c in digits.chars() {
        if c == '_' {
            underscores = true;
            continue;
        }
        let digit = c.to_digit(base).ok_or(NumError::Syntax)?;
        // Go stops at the first overflow, before looking at the rest.
        value = value
            .checked_mul(u64::from(base))
            .and_then(|v| v.checked_add(u64::from(digit)))
            .ok_or(NumError::Range)?;
    }
    if underscores && !underscores_ok(text) {
        return Err(NumError::Syntax);
    }
    Ok(value)
}

/// `strconv.ParseFloat(text, bits)`: decimal or hexadecimal (`0x1.8p3`) floats with `_`
/// between digits, `inf`/`infinity` (signed) and `nan`; a finite value too large for the size
/// is out of range.
pub fn parse_float(text: &str, bits: u32) -> Result<f64, String> {
    let error = |e| num_error("ParseFloat", text, e);
    let (sign, unsigned) = match text.as_bytes().first() {
        Some(b'-') => (-1.0, &text[1..]),
        Some(b'+') => (1.0, &text[1..]),
        _ => (1.0, text),
    };
    let lower = unsigned.to_ascii_lowercase();
    if lower == "inf" || lower == "infinity" {
        return Ok(sign * f64::INFINITY);
    }
    if text.eq_ignore_ascii_case("nan") {
        return Ok(f64::NAN);
    }
    if unsigned.contains('_') && !underscores_ok(text) {
        return Err(error(NumError::Syntax));
    }
    let cleaned = unsigned.replace('_', "");
    let magnitude = if lower.starts_with("0x") {
        parse_hex_float(&cleaned[2..], bits).ok_or(error(NumError::Syntax))?
    } else {
        let valid = {
            let (mantissa, exponent) = match cleaned.find(['e', 'E']) {
                Some(at) => (&cleaned[..at], Some(&cleaned[at + 1..])),
                None => (cleaned.as_str(), None),
            };
            let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
            let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
            digits(whole)
                && digits(fraction)
                && !(whole.is_empty() && fraction.is_empty())
                && exponent.is_none_or(|e| {
                    let e = e.strip_prefix(['+', '-']).unwrap_or(e);
                    !e.is_empty() && digits(e)
                })
        };
        if !valid {
            return Err(error(NumError::Syntax));
        }
        if bits == 32 {
            f64::from(
                cleaned
                    .parse::<f32>()
                    .map_err(|_| error(NumError::Syntax))?,
            )
        } else {
            cleaned
                .parse::<f64>()
                .map_err(|_| error(NumError::Syntax))?
        }
    };
    if magnitude.is_infinite() {
        return Err(error(NumError::Range));
    }
    Ok(sign * magnitude)
}

/// The magnitude of a hexadecimal float after `0x`: hex digits with an optional point, then a
/// required `p` exponent; rounded to the size.
fn parse_hex_float(s: &str, bits: u32) -> Option<f64> {
    let (mantissa, exponent) = s.split_once(['p', 'P'])?;
    let exponent: i32 = exponent.parse().ok()?;
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    let mut value: f64 = 0.0;
    for c in whole.chars().chain(fraction.chars()) {
        value = value * 16.0 + f64::from(c.to_digit(16)?);
    }
    let shift = exponent - 4 * fraction.len() as i32;
    let value = value * 2f64.powi(shift);
    Some(if bits == 32 {
        f64::from(value as f32)
    } else {
        value
    })
}

/// The `time` package's own `quote`: non-ASCII and control characters as `\x` bytes.
fn time_quote(s: &str) -> String {
    let mut out = String::from('"');
    for c in s.chars() {
        if !c.is_ascii() || c < ' ' {
            let mut bytes = [0; 4];
            for b in c.encode_utf8(&mut bytes).bytes() {
                let _ = write!(out, "\\x{b:02x}");
            }
        } else {
            if c == '"' || c == '\\' {
                out.push('\\');
            }
            out.push(c);
        }
    }
    out.push('"');
    out
}

/// `time.ParseDuration`, in nanoseconds, with Go's error texts.
pub fn parse_duration(text: &str) -> Result<i64, String> {
    let invalid = || format!("time: invalid duration {}", time_quote(text));
    const OVERFLOW: u64 = 1 << 63;
    let (negative, mut s) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    if s == "0" {
        return Ok(0);
    }
    if s.is_empty() {
        return Err(invalid());
    }
    let mut total: u64 = 0;
    while !s.is_empty() {
        if !(s.starts_with('.') || s.starts_with(|c: char| c.is_ascii_digit())) {
            return Err(invalid());
        }
        let digits = s.bytes().take_while(u8::is_ascii_digit).count();
        let mut whole: u64 = 0;
        for b in s[..digits].bytes() {
            if whole > OVERFLOW / 10 {
                return Err(invalid());
            }
            whole = whole * 10 + u64::from(b - b'0');
            if whole > OVERFLOW {
                return Err(invalid());
            }
        }
        s = &s[digits..];
        let mut fraction: u64 = 0;
        let mut scale = 1.0;
        let mut fraction_digits = 0;
        if let Some(after) = s.strip_prefix('.') {
            fraction_digits = after.bytes().take_while(u8::is_ascii_digit).count();
            let mut overflow = false;
            for b in after[..fraction_digits].bytes() {
                if overflow {
                    continue;
                }
                if fraction > (OVERFLOW - 1) / 10 {
                    overflow = true;
                    continue;
                }
                let next = fraction * 10 + u64::from(b - b'0');
                if next > OVERFLOW {
                    overflow = true;
                    continue;
                }
                fraction = next;
                scale *= 10.0;
            }
            s = &after[fraction_digits..];
        }
        if digits == 0 && fraction_digits == 0 {
            return Err(invalid());
        }
        let unit_len = s
            .bytes()
            .take_while(|b| *b != b'.' && !b.is_ascii_digit())
            .count();
        if unit_len == 0 {
            return Err(format!(
                "time: missing unit in duration {}",
                time_quote(text)
            ));
        }
        let unit: u64 = match &s[..unit_len] {
            "ns" => 1,
            "us" | "\u{b5}s" | "\u{3bc}s" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            other => {
                return Err(format!(
                    "time: unknown unit {} in duration {}",
                    time_quote(other),
                    time_quote(text)
                ));
            }
        };
        s = &s[unit_len..];
        if whole > OVERFLOW / unit {
            return Err(invalid());
        }
        let mut value = whole * unit;
        if fraction > 0 {
            value += (fraction as f64 * (unit as f64 / scale)) as u64;
            if value > OVERFLOW {
                return Err(invalid());
            }
        }
        total += value;
        if total > OVERFLOW {
            return Err(invalid());
        }
    }
    if negative {
        return Ok((total as i64).wrapping_neg());
    }
    i64::try_from(total).map_err(|_| invalid())
}
