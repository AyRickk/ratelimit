//! A YAML document model that reproduces how Go's `gopkg.in/yaml.v2` resolves scalars,
//! which is what envoyproxy/ratelimit uses to read its config files. It differs from YAML 1.2
//! parsers in ways that change behaviour: `yes`/`on` are booleans, `010` is octal, a scalar
//! decoded into a string field keeps its source text, and so on.

use yaml_rust2::parser::{Event, MarkedEventReceiver, Parser, Tag};
use yaml_rust2::scanner::{Marker, TScalarStyle};

#[derive(Debug, Clone)]
pub(crate) enum Node {
    Scalar(Scalar),
    Seq {
        items: Vec<Node>,
        line: usize,
    },
    Map {
        entries: Vec<(Node, Node)>,
        line: usize,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct Scalar {
    pub text: String,
    plain: bool,
    tag: Option<String>,
    pub line: usize,
}

/// A scalar after yaml.v2 resolution.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Resolved {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    Str(String),
}

impl Node {
    pub fn line(&self) -> usize {
        match self {
            Node::Scalar(s) => s.line,
            Node::Seq { line, .. } | Node::Map { line, .. } => *line,
        }
    }

    /// The yaml.v2 short tag used in type errors (`!!str`, `!!map`, ...).
    pub fn short_tag(&self) -> &'static str {
        match self {
            Node::Seq { .. } => "!!seq",
            Node::Map { .. } => "!!map",
            Node::Scalar(s) => match s.resolve() {
                Resolved::Null => "!!null",
                Resolved::Bool(_) => "!!bool",
                Resolved::Int(_) | Resolved::Uint(_) => "!!int",
                Resolved::Float(_) => "!!float",
                Resolved::Str(_) => "!!str",
            },
        }
    }
}

impl Scalar {
    pub fn resolve(&self) -> Resolved {
        match self.tag.as_deref() {
            Some("tag:yaml.org,2002:str") => return Resolved::Str(self.text.clone()),
            Some(_) => {}
            None if !self.plain => return Resolved::Str(self.text.clone()),
            None => {}
        }
        resolve_plain(&self.text)
    }
}

fn resolve_plain(s: &str) -> Resolved {
    match s {
        "" | "~" | "null" | "Null" | "NULL" => return Resolved::Null,
        "y" | "Y" | "yes" | "Yes" | "YES" | "true" | "True" | "TRUE" | "on" | "On" | "ON" => {
            return Resolved::Bool(true);
        }
        "n" | "N" | "no" | "No" | "NO" | "false" | "False" | "FALSE" | "off" | "Off" | "OFF" => {
            return Resolved::Bool(false);
        }
        ".inf" | ".Inf" | ".INF" | "+.inf" | "+.Inf" | "+.INF" => {
            return Resolved::Float(f64::INFINITY);
        }
        "-.inf" | "-.Inf" | "-.INF" => return Resolved::Float(f64::NEG_INFINITY),
        ".nan" | ".NaN" | ".NAN" => return Resolved::Float(f64::NAN),
        _ => {}
    }
    if !s.starts_with(|c: char| c.is_ascii_digit() || matches!(c, '+' | '-' | '.')) {
        return Resolved::Str(s.to_string());
    }
    let plain = s.replace('_', "");
    if let Some(v) = go_parse_int(&plain) {
        return Resolved::Int(v);
    }
    if let Some(v) = go_parse_uint(&plain) {
        return Resolved::Uint(v);
    }
    if is_yaml_style_float(&plain) {
        if let Ok(v) = plain.parse::<f64>() {
            return Resolved::Float(v);
        }
    }
    if let Some(rest) = plain.strip_prefix("0b") {
        if let Ok(v) = i64::from_str_radix(rest, 2) {
            return Resolved::Int(v);
        }
        if let Ok(v) = u64::from_str_radix(rest, 2) {
            return Resolved::Uint(v);
        }
    } else if let Some(rest) = plain.strip_prefix("-0b") {
        if let Ok(v) = i64::from_str_radix(&format!("-{rest}"), 2) {
            return Resolved::Int(v);
        }
    }
    Resolved::Str(s.to_string())
}

/// `strconv.ParseInt(s, 0, 64)`: optional sign, then `0x`, `0o`, `0b` or a leading `0` (octal).
fn go_parse_int(s: &str) -> Option<i64> {
    let (negative, digits) = match s.as_bytes().first()? {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    let magnitude = go_parse_unsigned_digits(digits)?;
    if negative {
        if magnitude <= i64::MAX as u64 + 1 {
            Some((magnitude as i128).wrapping_neg() as i64)
        } else {
            None
        }
    } else {
        i64::try_from(magnitude).ok()
    }
}

/// `strconv.ParseUint(s, 0, 64)`: no sign allowed.
fn go_parse_uint(s: &str) -> Option<u64> {
    go_parse_unsigned_digits(s)
}

fn go_parse_unsigned_digits(s: &str) -> Option<u64> {
    let (radix, digits) = if let Some(rest) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X"))
    {
        (16, rest)
    } else if let Some(rest) = s.strip_prefix("0o").or_else(|| s.strip_prefix("0O")) {
        (8, rest)
    } else if let Some(rest) = s.strip_prefix("0b").or_else(|| s.strip_prefix("0B")) {
        (2, rest)
    } else if s.len() > 1 && s.starts_with('0') {
        (8, &s[1..])
    } else {
        (10, s)
    };
    if digits.is_empty() || !digits.chars().all(|c| c.is_digit(radix)) {
        return None;
    }
    u64::from_str_radix(digits, radix).ok()
}

/// `^[-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)?$`
fn is_yaml_style_float(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    if matches!(b.first(), Some(b'-' | b'+')) {
        i += 1;
    }
    let digits = |i: &mut usize| {
        let start = *i;
        while *i < b.len() && b[*i].is_ascii_digit() {
            *i += 1;
        }
        *i - start
    };
    if b.get(i) == Some(&b'.') {
        i += 1;
        if digits(&mut i) == 0 {
            return false;
        }
    } else {
        if digits(&mut i) == 0 {
            return false;
        }
        if b.get(i) == Some(&b'.') {
            i += 1;
            digits(&mut i);
        }
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(b.get(i), Some(b'-' | b'+')) {
            i += 1;
        }
        if digits(&mut i) == 0 {
            return false;
        }
    }
    i == b.len()
}

/// Formats a resolved scalar like Go's `fmt.Sprintf("%v", v)`.
pub(crate) fn go_format(value: &Resolved) -> String {
    match value {
        Resolved::Null => "<nil>".to_string(),
        Resolved::Bool(b) => b.to_string(),
        Resolved::Int(v) => v.to_string(),
        Resolved::Uint(v) => v.to_string(),
        Resolved::Float(v) => go_format_float(*v),
        Resolved::Str(s) => s.clone(),
    }
}

/// `strconv.FormatFloat(v, 'g', -1, 64)` as used by `%v`.
fn go_format_float(v: f64) -> String {
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
    if exp < -4 || exp >= 6 {
        let exp_sign = if exp < 0 { '-' } else { '+' };
        return format!("{mantissa}e{exp_sign}{:02}", exp.abs());
    }
    format!("{v}")
}

struct Builder {
    stack: Vec<Frame>,
    anchors: Vec<Option<Node>>,
    root: Option<Node>,
    documents: usize,
}

enum Frame {
    Seq {
        items: Vec<Node>,
        line: usize,
        anchor: usize,
    },
    Map {
        entries: Vec<(Node, Node)>,
        pending_key: Option<Node>,
        line: usize,
        anchor: usize,
    },
}

impl Builder {
    fn push(&mut self, node: Node, anchor: usize) {
        if anchor > 0 {
            if self.anchors.len() <= anchor {
                self.anchors.resize(anchor + 1, None);
            }
            self.anchors[anchor] = Some(node.clone());
        }
        match self.stack.last_mut() {
            None => {
                if self.documents == 1 {
                    self.root = Some(node);
                }
            }
            Some(Frame::Seq { items, .. }) => items.push(node),
            Some(Frame::Map {
                entries,
                pending_key,
                ..
            }) => match pending_key.take() {
                None => *pending_key = Some(node),
                Some(key) => entries.push((key, node)),
            },
        }
    }
}

impl MarkedEventReceiver for Builder {
    fn on_event(&mut self, ev: Event, mark: Marker) {
        let line = mark.line();
        match ev {
            Event::DocumentStart => self.documents += 1,
            Event::Scalar(text, style, anchor, tag) => {
                let plain = matches!(style, TScalarStyle::Plain);
                let tag = tag.map(|Tag { handle, suffix }| {
                    if handle == "!!" || handle == "tag:yaml.org,2002:" {
                        format!("tag:yaml.org,2002:{suffix}")
                    } else {
                        format!("{handle}{suffix}")
                    }
                });
                self.push(
                    Node::Scalar(Scalar {
                        text,
                        plain,
                        tag,
                        line,
                    }),
                    anchor,
                );
            }
            Event::SequenceStart(anchor, _) => self.stack.push(Frame::Seq {
                items: Vec::new(),
                line,
                anchor,
            }),
            Event::MappingStart(anchor, _) => self.stack.push(Frame::Map {
                entries: Vec::new(),
                pending_key: None,
                line,
                anchor,
            }),
            Event::SequenceEnd => {
                if let Some(Frame::Seq {
                    items,
                    line,
                    anchor,
                }) = self.stack.pop()
                {
                    self.push(Node::Seq { items, line }, anchor);
                }
            }
            Event::MappingEnd => {
                if let Some(Frame::Map {
                    entries,
                    line,
                    anchor,
                    ..
                }) = self.stack.pop()
                {
                    self.push(Node::Map { entries, line }, anchor);
                }
            }
            Event::Alias(id) => {
                let node = self.anchors.get(id).cloned().flatten();
                if let Some(node) = node {
                    self.push(node, 0);
                }
            }
            Event::Nothing | Event::StreamStart | Event::StreamEnd | Event::DocumentEnd => {}
        }
    }
}

/// Parses the first document of `content`. `Ok(None)` is an empty document.
pub(crate) fn parse(content: &str) -> Result<Option<Node>, String> {
    let mut builder = Builder {
        stack: Vec::new(),
        anchors: Vec::new(),
        root: None,
        documents: 0,
    };
    Parser::new_from_str(content)
        .load(&mut builder, true)
        .map_err(|e| format!("yaml: line {}: {}", e.marker().line(), e.info()))?;
    Ok(builder.root)
}
