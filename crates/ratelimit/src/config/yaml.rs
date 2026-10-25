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
                Resolved::Str(_) if s.is_timestamp() => "!!timestamp",
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

    /// A null that leaves a pointer field nil: yaml.v2 only treats `null`, `~`, an empty
    /// plain scalar or an explicit `!!null` this way (other null spellings allocate it).
    pub fn is_nil(&self) -> bool {
        self.tag.as_deref() == Some("tag:yaml.org,2002:null")
            || (self.plain && self.tag.is_none() && matches!(self.text.as_str(), "null" | "~" | ""))
    }

    /// Whether yaml.v2 resolves the scalar as a timestamp (it still decodes as a string).
    fn is_timestamp(&self) -> bool {
        match self.tag.as_deref() {
            Some("tag:yaml.org,2002:timestamp") => true,
            None if self.plain => is_go_timestamp(&self.text),
            _ => false,
        }
    }
}

/// yaml.v2 `parseTimestamp`: `2006-1-2`, `2006-1-2 15:4:5.999999999` or
/// `2006-1-2T15:4:5.999999999Z07:00` (`t` also accepted), with Go's range checks.
fn is_go_timestamp(s: &str) -> bool {
    fn number(s: &str, min_digits: usize, max_digits: usize) -> Option<(u32, &str)> {
        let len = s
            .bytes()
            .take(max_digits)
            .take_while(u8::is_ascii_digit)
            .count();
        (len >= min_digits).then(|| (s[..len].parse().unwrap(), &s[len..]))
    }
    let Some((year, rest)) = number(s, 4, 4) else {
        return false;
    };
    let Some(rest) = rest.strip_prefix('-') else {
        return false;
    };
    let Some((month, rest)) = number(rest, 1, 2) else {
        return false;
    };
    let Some(rest) = rest.strip_prefix('-') else {
        return false;
    };
    let Some((day, rest)) = number(rest, 1, 2) else {
        return false;
    };
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if !(1..=12).contains(&month) || day < 1 || day > days[month as usize - 1] {
        return false;
    }
    if rest.is_empty() {
        return true;
    }
    let (zoned, rest) = match rest.as_bytes()[0] {
        b'T' | b't' => (true, &rest[1..]),
        b' ' => (false, &rest[1..]),
        _ => return false,
    };
    let Some((hour, rest)) = number(rest, 1, 2) else {
        return false;
    };
    let Some(rest) = rest.strip_prefix(':') else {
        return false;
    };
    let Some((minute, rest)) = number(rest, 1, 2) else {
        return false;
    };
    let Some(rest) = rest.strip_prefix(':') else {
        return false;
    };
    let Some((second, mut rest)) = number(rest, 1, 2) else {
        return false;
    };
    if hour > 23 || minute > 59 || second > 59 {
        return false;
    }
    if let Some(fraction) = rest.strip_prefix('.') {
        let digits = fraction.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return false;
        }
        rest = &fraction[digits..];
    }
    if !zoned {
        return rest.is_empty();
    }
    if rest == "Z" {
        return true;
    }
    let Some(offset) = rest.strip_prefix(['+', '-']) else {
        return false;
    };
    let b = offset.as_bytes();
    b.len() == 5 && b[2] == b':' && [0, 1, 3, 4].iter().all(|&i| b[i].is_ascii_digit())
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
    if is_yaml_style_float(&plain)
        && let Ok(v) = plain.parse::<f64>()
    {
        return Resolved::Float(v);
    }
    if let Some(rest) = plain.strip_prefix("0b") {
        if let Ok(v) = i64::from_str_radix(rest, 2) {
            return Resolved::Int(v);
        }
        if let Ok(v) = u64::from_str_radix(rest, 2) {
            return Resolved::Uint(v);
        }
    } else if let Some(rest) = plain.strip_prefix("-0b")
        && let Ok(v) = i64::from_str_radix(&format!("-{rest}"), 2)
    {
        return Resolved::Int(v);
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
    let magnitude = go_parse_uint(digits)?;
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
        Resolved::Float(v) => crate::go::format_float(*v),
        Resolved::Str(s) => s.clone(),
    }
}

struct Builder {
    stack: Vec<Frame>,
    anchors: Vec<Option<Node>>,
    root: Option<Node>,
    documents: usize,
    error: Option<String>,
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
            Event::Scalar(mut text, style, anchor, tag) => {
                let plain = matches!(style, TScalarStyle::Plain);
                let mut tag = tag.map(|Tag { handle, suffix }| {
                    if handle == "!!" || handle == "tag:yaml.org,2002:" {
                        format!("tag:yaml.org,2002:{suffix}")
                    } else {
                        format!("{handle}{suffix}")
                    }
                });
                if tag.as_deref() == Some("tag:yaml.org,2002:binary") {
                    use base64::Engine;
                    let compact: String = text.split_whitespace().collect();
                    match base64::engine::general_purpose::STANDARD.decode(compact) {
                        Ok(bytes) => {
                            text = String::from_utf8_lossy(&bytes).into_owned();
                            tag = Some("tag:yaml.org,2002:str".to_string());
                        }
                        Err(_) => {
                            self.error.get_or_insert_with(|| {
                                "yaml: !!binary value contains invalid base64 data".to_string()
                            });
                        }
                    }
                }
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
                    match expand_merge_keys(entries) {
                        Ok(entries) => self.push(Node::Map { entries, line }, anchor),
                        Err(e) => {
                            self.error.get_or_insert(e);
                        }
                    }
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

/// A config file's bytes as yaml.v2's reader decodes them: UTF-8 (a byte order mark skipped),
/// or UTF-16 after its byte order mark; the first invalid sequence or control character is
/// its error. (yaml.v2 decodes 512 bytes at a time as it parses: a syntax error before a bad
/// byte further on is reported first there.)
pub(crate) fn decode(bytes: &[u8]) -> Result<String, String> {
    match bytes {
        [0xFF, 0xFE, rest @ ..] => decode_utf16(rest, u16::from_le_bytes),
        [0xFE, 0xFF, rest @ ..] => decode_utf16(rest, u16::from_be_bytes),
        _ => decode_utf8(bytes.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(bytes)),
    }
}

/// UTF-8, with yaml.v2's errors.
fn decode_utf8(bytes: &[u8]) -> Result<String, String> {
    let fail = |problem: &str| Err(format!("yaml: {problem}"));
    let mut i = 0;
    while i < bytes.len() {
        let octet = bytes[i];
        let width = match octet {
            _ if octet & 0x80 == 0 => 1,
            _ if octet & 0xE0 == 0xC0 => 2,
            _ if octet & 0xF0 == 0xE0 => 3,
            _ if octet & 0xF8 == 0xF0 => 4,
            _ => return fail("invalid leading UTF-8 octet"),
        };
        if i + width > bytes.len() {
            return fail("incomplete UTF-8 octet sequence");
        }
        let mut value = u32::from(match width {
            1 => octet & 0x7F,
            2 => octet & 0x1F,
            3 => octet & 0x0F,
            _ => octet & 0x07,
        });
        for &trailing in &bytes[i + 1..i + width] {
            if trailing & 0xC0 != 0x80 {
                return fail("invalid trailing UTF-8 octet");
            }
            value = (value << 6) + u32::from(trailing & 0x3F);
        }
        let minimum = [0, 0, 0x80, 0x800, 0x10000][width];
        if value < minimum {
            return fail("invalid length of a UTF-8 sequence");
        }
        if (0xD800..=0xDFFF).contains(&value) || value > 0x10FFFF {
            return fail("invalid Unicode character");
        }
        check_character(value)?;
        i += width;
    }
    // Every sequence was checked.
    Ok(String::from_utf8(bytes.to_vec()).unwrap())
}

/// UTF-16 after its byte order mark, with yaml.v2's errors.
fn decode_utf16(bytes: &[u8], read: fn([u8; 2]) -> u16) -> Result<String, String> {
    let fail = |problem: &str| Err(format!("yaml: {problem}"));
    let mut text = String::new();
    let mut i = 0;
    while i < bytes.len() {
        if i + 2 > bytes.len() {
            return fail("incomplete UTF-16 character");
        }
        let unit = read([bytes[i], bytes[i + 1]]);
        i += 2;
        let value = match unit {
            0xDC00..=0xDFFF => return fail("unexpected low surrogate area"),
            0xD800..=0xDBFF => {
                if i + 2 > bytes.len() {
                    return fail("incomplete UTF-16 surrogate pair");
                }
                let low = read([bytes[i], bytes[i + 1]]);
                if !(0xDC00..=0xDFFF).contains(&low) {
                    return fail("expected low surrogate area");
                }
                i += 2;
                0x10000 + ((u32::from(unit) & 0x3FF) << 10) + (u32::from(low) & 0x3FF)
            }
            unit => u32::from(unit),
        };
        check_character(value)?;
        text.push(char::from_u32(value).unwrap());
    }
    Ok(text)
}

/// The characters yaml.v2 accepts: tab, line breaks, printable ASCII, `U+0085`, and the rest
/// of Unicode but surrogates and `U+FFFE`/`U+FFFF`.
fn check_character(value: u32) -> Result<(), String> {
    match value {
        0x09 | 0x0A | 0x0D | 0x20..=0x7E | 0x85 | 0xA0..=0xD7FF | 0xE000..=0xFFFD => Ok(()),
        0x10000..=0x10FFFF => Ok(()),
        _ => Err("yaml: control characters are not allowed".into()),
    }
}

/// Parses the first document of `content`. `Ok(None)` is an empty document.
pub(crate) fn parse(content: &str) -> Result<Option<Node>, String> {
    let content = content.strip_prefix('\u{FEFF}').unwrap_or(content);
    for c in content.chars() {
        check_character(c as u32)?;
    }
    // libyaml reads NEL as a line break, `\n` (the parser here is YAML 1.2).
    // A CR alone breaks lines too; values are the same with `\n`.
    let content = content
        .replace('\u{85}', "\n")
        .replace("\r\n", "\n")
        .replace('\r', "\n");
    let mut content = content;
    // One more parse per quoted scalar to indent: config files have few.
    loop {
        let mut builder = Builder {
            stack: Vec::new(),
            anchors: Vec::new(),
            root: None,
            documents: 0,
            error: None,
        };
        match Parser::new_from_str(&content).load(&mut builder, true) {
            Ok(()) => {
                return match builder.error {
                    Some(e) => Err(e),
                    None => Ok(builder.root),
                };
            }
            Err(e) => {
                let at = |accept: &[char]| {
                    char_at(&content, e.marker().line(), e.marker().col(), accept)
                };
                let indented = match e.info() {
                    "invalid indentation in quoted scalar" => at(&['"', '\''])
                        .and_then(|start| indent_quoted_continuations(&content, start)),
                    // A tab in a continuation line's leading blanks, which are folded away.
                    "tab cannot be used as indentation" => at(&['\t']).map(|tab| {
                        content
                            .chars()
                            .enumerate()
                            .map(|(i, c)| if i == tab { ' ' } else { c })
                            .collect()
                    }),
                    _ => None,
                };
                match indented {
                    Some(indented) => content = indented,
                    None => {
                        return Err(format!("yaml: line {}: {}", e.marker().line(), e.info()));
                    }
                }
            }
        }
    }
}

/// yaml.v2 merge keys: a plain `<<` entry is replaced, in place, by the entries of the map it
/// refers to, or of each map of a sequence with earlier maps taking precedence.
fn expand_merge_keys(entries: Vec<(Node, Node)>) -> Result<Vec<(Node, Node)>, String> {
    let is_merge =
        |k: &Node| matches!(k, Node::Scalar(s) if s.plain && s.tag.is_none() && s.text == "<<");
    if !entries.iter().any(|(k, _)| is_merge(k)) {
        return Ok(entries);
    }
    let want_map = || "yaml: map merge requires map or sequence of maps as the value".to_string();
    let mut out = Vec::with_capacity(entries.len());
    for (k, v) in entries {
        if !is_merge(&k) {
            out.push((k, v));
            continue;
        }
        match v {
            Node::Map { entries, .. } => out.extend(entries),
            Node::Seq { items, .. } => {
                // Later entries win, so the first map of the sequence goes last.
                for item in items.into_iter().rev() {
                    match item {
                        Node::Map { entries, .. } => out.extend(entries),
                        _ => return Err(want_map()),
                    }
                }
            }
            _ => return Err(want_map()),
        }
    }
    Ok(out)
}

/// The char index of the char among `accept` that yaml-rust2 reports at `line` (from 1) and
/// `col` (in chars).
fn char_at(content: &str, line: usize, col: usize, accept: &[char]) -> Option<usize> {
    let line_start: usize = content
        .split_inclusive('\n')
        .take(line.checked_sub(1)?)
        .map(|l| l.chars().count())
        .sum();
    let c = content
        .split_inclusive('\n')
        .nth(line - 1)?
        .chars()
        .nth(col)?;
    accept.contains(&c).then_some(line_start + col)
}

/// `---` or `...` alone or before a blank.
fn starts_with_marker(rest: &[char]) -> bool {
    (rest.starts_with(&['-', '-', '-']) || rest.starts_with(&['.', '.', '.']))
        && rest
            .get(3)
            .is_none_or(|c| matches!(c, ' ' | '\t' | '\r' | '\n'))
}

/// libyaml does not check a quoted scalar's indentation, where yaml-rust2 wants each
/// continuation line more indented than the block holding the scalar. The continuation lines
/// of the scalar opening at char `start` are indented further: their leading whitespace is
/// folded away, so the value and line numbers stay the same. `None` if it has none.
fn indent_quoted_continuations(content: &str, start: usize) -> Option<String> {
    let chars: Vec<char> = content.chars().collect();
    let quote = *chars.get(start).filter(|c| matches!(c, '"' | '\''))?;
    let mut out: String = chars[..=start].iter().collect();
    let mut changed = false;
    let mut i = start + 1;
    while i < chars.len() {
        let c = chars[i];
        out.push(c);
        match c {
            // An escape, except of a line break, which continues the scalar too.
            '\\' if quote == '"' && chars.get(i + 1).is_some_and(|c| *c != '\n') => {
                out.push(chars[i + 1]);
                i += 1;
            }
            '\'' if quote == '\'' && chars.get(i + 1) == Some(&'\'') => {
                out.push('\'');
                i += 1;
            }
            c if c == quote => break,
            // A document marker at a line's start is left there, where libyaml refuses it.
            '\n' if !starts_with_marker(&chars[i + 1..]) => {
                out.push_str(&" ".repeat(256));
                changed = true;
            }
            _ => {}
        }
        i += 1;
    }
    out.extend(chars.get(i + 1..).unwrap_or_default());
    changed.then_some(out)
}

#[cfg(test)]
mod continuation_tests {
    use super::{Node, parse};

    /// Go's `%#v` of what yaml.v2 decodes.
    fn render(node: &Node) -> String {
        match node {
            Node::Scalar(s) => format!("{:?}", s.text),
            Node::Seq { items, .. } => format!(
                "[]interface {{}}{{{}}}",
                items.iter().map(render).collect::<Vec<_>>().join(", ")
            ),
            Node::Map { entries, .. } => {
                let mut entries: Vec<String> = entries
                    .iter()
                    .map(|(k, v)| format!("{}:{}", render(k), render(v)))
                    .collect();
                entries.sort();
                format!(
                    "map[interface {{}}]interface {{}}{{{}}}",
                    entries.join(", ")
                )
            }
        }
    }

    /// Quoted scalars continued on less indented lines, next to plain scalars holding quotes,
    /// read as yaml.v2 v2.4.0 reads them (its `%#v`; `None` where it refuses the input).
    #[test]
    fn quoted_scalars_continue_as_yaml_v2_reads_them() {
        for (input, expected) in [
            (
                "a: &x \"p\nq\"\n",
                Some("map[interface {}]interface {}{\"a\":\"p q\"}"),
            ),
            (
                "a: !!str \"p\nq\"\n",
                Some("map[interface {}]interface {}{\"a\":\"p q\"}"),
            ),
            (
                "a: x\n  \"y\nb: c\n",
                Some("map[interface {}]interface {}{\"a\":\"x \\\"y\", \"b\":\"c\"}"),
            ),
            (
                "a: p - \"q\nb: c\n",
                Some("map[interface {}]interface {}{\"a\":\"p - \\\"q\", \"b\":\"c\"}"),
            ),
            (
                "a: p, \"q\nb: c\n",
                Some("map[interface {}]interface {}{\"a\":\"p, \\\"q\", \"b\":\"c\"}"),
            ),
            ("\"k\nx\": v\n", None),
            ("m:\n  \"k\nx\": v\n", None),
            (
                "m:\n  k: \"a\nb\"\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"k\":\"a b\"}}",
                ),
            ),
            (
                "- - \"x\ny\"\n",
                Some("[]interface {}{[]interface {}{\"x y\"}}"),
            ),
            (
                "? \"x\ny\"\n: v\n",
                Some("map[interface {}]interface {}{\"x y\":\"v\"}"),
            ),
            (
                "m:\n  k: | # c\n    \"z\n  j: \"a\nb\"\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"j\":\"a b\", \"k\":\"\\\"z\\n\"}}",
                ),
            ),
            (
                "m:\n  k: [ \"a\nb\", c]\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"k\":[]interface {}{\"a b\", \"c\"}}}",
                ),
            ),
            (
                "m:\n  k: \"a\nb\"\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"k\":\"a b\"}}",
                ),
            ),
            (
                "m:\n  k: \"a\n\tb\"\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"k\":\"a b\"}}",
                ),
            ),
            (
                "m:\n  k: \"a # b\nc\"\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"k\":\"a # b c\"}}",
                ),
            ),
            ("k: \"a\n---\nb\"\n", None),
            (
                "m:\n  k: 'it''s\nb'\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"k\":\"it's b\"}}",
                ),
            ),
            (
                "m:\n  k: a |\n    \"x\n  j: c\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"j\":\"c\", \"k\":\"a | \\\"x\"}}",
                ),
            ),
            (
                "m:\n  k: \"a\\\nb\"\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"k\":\"ab\"}}",
                ),
            ),
            (
                "m: {a: \"x\ny\"}\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"a\":\"x y\"}}",
                ),
            ),
            ("- a\n  'b\n- c\n", Some("[]interface {}{\"a 'b\", \"c\"}")),
            (
                "%YAML 1.1\n---\nk: \"a\nb\"\n",
                Some("map[interface {}]interface {}{\"k\":\"a b\"}"),
            ),
            (
                "m:\n  k: >\n    \"x\n  j: \"a\nb\"\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"j\":\"a b\", \"k\":\"\\\"x\\n\"}}",
                ),
            ),
            (
                "k: [a, # c \"\n \"b\nc\"]\n",
                Some("map[interface {}]interface {}{\"k\":[]interface {}{\"a\", \"b c\"}}"),
            ),
            (
                "m:\n  k: {\"a\":\"b\nc\"}\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"k\":map[interface {}]interface {}{\"a\":\"b c\"}}}",
                ),
            ),
            ("k: \"a\n...\nb\"\n", None),
            (
                "k: a:\"b\nj: c\n",
                Some("map[interface {}]interface {}{\"j\":\"c\", \"k\":\"a:\\\"b\"}"),
            ),
            (
                "- k: \"a\nb\"\n  j: c\n",
                Some("[]interface {}{map[interface {}]interface {}{\"j\":\"c\", \"k\":\"a b\"}}"),
            ),
            (
                "m:\n  k: \"a\n\nb\"\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"k\":\"a\\nb\"}}",
                ),
            ),
            (
                "m:\n  k: &x \"a\nb\"\n  j: *x\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"j\":\"a b\", \"k\":\"a b\"}}",
                ),
            ),
            (
                "m:\n  \"k\": \"a\nb\"\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"k\":\"a b\"}}",
                ),
            ),
            (
                "m:\n  k: \"a\n\t\tb\"\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"k\":\"a b\"}}",
                ),
            ),
            (
                "- O'Reilly\n- x\n",
                Some("[]interface {}{\"O'Reilly\", \"x\"}"),
            ),
            ("- k: |\n   x\n  \"y\n", None),
            ("- !!str \"a\nb\"\n", Some("[]interface {}{\"a b\"}")),
            (
                "m:\n  k: a\n    \"b c\n  j: d\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"j\":\"d\", \"k\":\"a \\\"b c\"}}",
                ),
            ),
            (
                "m:\n  k: a -\n    \"b\n  j: \"x\ny\"\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"j\":\"x y\", \"k\":\"a - \\\"b\"}}",
                ),
            ),
            (
                "m:\n  k: x |\n   \"y\n  j: \"a\nb\"\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"j\":\"a b\", \"k\":\"x | \\\"y\"}}",
                ),
            ),
            (
                "k: v\n---\nj: a, \"b\n",
                Some("map[interface {}]interface {}{\"k\":\"v\"}"),
            ),
            (
                "m:\n  k: |+\n    x\n\n  j: \"a\nb\"\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"j\":\"a b\", \"k\":\"x\\n\\n\"}}",
                ),
            ),
            (
                "m:\n  k: \"a |\nb\"\n  j: \"c\nd\"\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"j\":\"c d\", \"k\":\"a | b\"}}",
                ),
            ),
            (
                "m:\n  k: \"a\nb\" # \"x\n  j: c\n",
                Some(
                    "map[interface {}]interface {}{\"m\":map[interface {}]interface {}{\"j\":\"c\", \"k\":\"a b\"}}",
                ),
            ),
            (
                "a:\n  b:\n    c: \"x\ny\"\n  d: e\n",
                Some(
                    "map[interface {}]interface {}{\"a\":map[interface {}]interface {}{\"b\":map[interface {}]interface {}{\"c\":\"x y\"}, \"d\":\"e\"}}",
                ),
            ),
            // Columns after a non-ASCII plain scalar, and before a non-ASCII quoted one.
            (
                "m:\n  é: \"a\nb\"\n",
                Some(
                    r#"map[interface {}]interface {}{"m":map[interface {}]interface {}{"é":"a b"}}"#,
                ),
            ),
            (
                "m:\n  k: \"é\n\tb\"\n  j: é \"c\n",
                Some(
                    r#"map[interface {}]interface {}{"m":map[interface {}]interface {}{"j":"é \"c", "k":"é b"}}"#,
                ),
            ),
            (
                "m:\n  \"é\": x\n  k: \"a\nb\"\n",
                Some(
                    r#"map[interface {}]interface {}{"m":map[interface {}]interface {}{"k":"a b", "é":"x"}}"#,
                ),
            ),
            (
                "m:\n  \"é\": \"a\nb\"\n",
                Some(
                    r#"map[interface {}]interface {}{"m":map[interface {}]interface {}{"é":"a b"}}"#,
                ),
            ),
            (
                "m:\n  k: [é, \"a\nb\"]\n",
                Some(
                    r#"map[interface {}]interface {}{"m":map[interface {}]interface {}{"k":[]interface {}{"é", "a b"}}}"#,
                ),
            ),
            // A document marker at a line's start, which libyaml refuses inside a scalar,
            // also after lines that are indented further.
            ("m:\n  k: \"a\nb\n---\nc\"\n", None),
            ("m:\n  k: 'a\nb\n...\nc'\n", None),
            ("m:\n  k: \"a\n\tb\n---\nc\"\n", None),
            // A CR alone as the line break.
            (
                "m:\r  k: \"a\rb\"\r",
                Some(
                    r#"map[interface {}]interface {}{"m":map[interface {}]interface {}{"k":"a b"}}"#,
                ),
            ),
            ("m:\n  k: \"a\n--- b\"\n", None),
            ("m:\n  k: 'a\n... b'\n", None),
        ] {
            let got = parse(input).ok().flatten().map(|n| render(&n));
            assert_eq!(got.as_deref(), expected, "{input:?}");
        }
    }
}
