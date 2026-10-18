//! The YAML config schema of envoyproxy/ratelimit (`YamlRoot` and friends in
//! `src/config/config_impl.go`), decoded with yaml.v2's rules and error messages.

use super::yaml::{Node, Resolved, go_format};
use prost_types::{ListValue, Struct, Value, value::Kind};
use std::collections::BTreeMap;

const VALID_KEYS: &[&str] = &[
    "domain",
    "key",
    "value",
    "descriptors",
    "rate_limit",
    "unit",
    "requests_per_unit",
    "unlimited",
    "shadow_mode",
    "quota_mode",
    "name",
    "replaces",
    "detailed_metric",
    "value_to_metric",
    "share_threshold",
    "metadata",
];

#[derive(Debug, Default)]
pub(crate) struct YamlRoot {
    pub domain: String,
    pub descriptors: Vec<YamlDescriptor>,
}

#[derive(Debug, Default)]
pub(crate) struct YamlDescriptor {
    pub key: String,
    pub value: String,
    pub rate_limit: Option<YamlRateLimit>,
    pub descriptors: Vec<YamlDescriptor>,
    pub shadow_mode: bool,
    pub quota_mode: bool,
    pub detailed_metric: bool,
    pub value_to_metric: bool,
    pub share_threshold: bool,
    pub metadata: Option<Struct>,
}

#[derive(Debug, Default)]
pub(crate) struct YamlRateLimit {
    pub requests_per_unit: u32,
    pub unit: String,
    pub unlimited: bool,
    pub name: String,
    pub replaces: Vec<String>,
}

/// Mapping entries with duplicate keys collapsed, the last one winning, like a Go map.
fn dedup_entries(entries: &[(Node, Node)]) -> Vec<(&Node, &Node)> {
    let mut out: Vec<(&Node, &Node)> = Vec::new();
    for (k, v) in entries {
        let resolved = match k {
            Node::Scalar(s) => Some(s.resolve()),
            _ => None,
        };
        if let Some(r) = &resolved
            && let Some(pos) = out.iter().position(|(ok, _)| match ok {
                Node::Scalar(s) => &s.resolve() == r,
                _ => false,
            })
        {
            out.remove(pos);
        }
        out.push((k, v));
    }
    out
}

/// `%v` of a value decoded into `interface{}`.
fn go_format_node(node: &Node) -> String {
    match node {
        Node::Scalar(s) => go_format(&s.resolve()),
        Node::Seq { items, .. } => {
            let items: Vec<_> = items.iter().map(go_format_node).collect();
            format!("[{}]", items.join(" "))
        }
        Node::Map { entries, .. } => {
            let mut items: Vec<_> = dedup_entries(entries)
                .into_iter()
                .map(|(k, v)| format!("{}:{}", go_format_node(k), go_format_node(v)))
                .collect();
            items.sort();
            format!("map[{}]", items.join(" "))
        }
    }
}

/// `validateYamlKeys`: every key anywhere outside `metadata` must be a known string key,
/// lists may only contain maps, and leaves may only be strings, ints, bools or null.
fn validate_keys(root: Option<&Node>) -> Result<(), String> {
    match root {
        Some(Node::Map { entries, .. }) => validate_map(entries),
        _ => Ok(()),
    }
}

fn validate_map(entries: &[(Node, Node)]) -> Result<(), String> {
    for (k, v) in dedup_entries(entries) {
        let key = match k {
            Node::Scalar(s) => s.resolve(),
            _ => unreachable!("collection keys are rejected by check_map_keys"),
        };
        let Resolved::Str(key) = key else {
            return Err(format!(
                "config error, key is not of type string: {}",
                go_format(&key)
            ));
        };
        if !VALID_KEYS.contains(&key.as_str()) {
            return Err(format!("config error, unknown key '{key}'"));
        }
        if key == "metadata" {
            continue;
        }
        match v {
            Node::Seq { items, .. } => {
                for item in items {
                    match item {
                        Node::Map { entries, .. } => validate_map(entries)?,
                        other => {
                            return Err(format!(
                                "config error, yaml file contains list of type other than map: {}",
                                go_format_node(other)
                            ));
                        }
                    }
                }
            }
            Node::Map { entries, .. } => validate_map(entries)?,
            Node::Scalar(s) => match s.resolve() {
                Resolved::Str(_) | Resolved::Int(_) | Resolved::Bool(_) | Resolved::Null => {}
                Resolved::Uint(_) | Resolved::Float(_) => {
                    return Err("error checking config".to_string());
                }
            },
        }
    }
    Ok(())
}

/// `%#v` of a value decoded into `interface{}`.
fn go_syntax(node: &Node) -> String {
    match node {
        Node::Scalar(s) => match s.resolve() {
            Resolved::Null => "interface {}(nil)".to_string(),
            Resolved::Str(s) => format!("{s:?}"),
            other => go_format(&other),
        },
        Node::Seq { items, .. } => {
            let items: Vec<_> = items.iter().map(go_syntax).collect();
            format!("[]interface {{}}{{{}}}", items.join(", "))
        }
        Node::Map { entries, .. } => {
            let mut items: Vec<_> = dedup_entries(entries)
                .into_iter()
                .map(|(k, v)| format!("{}:{}", go_syntax(k), go_syntax(v)))
                .collect();
            items.sort();
            format!("map[interface {{}}]interface {{}}{{{}}}", items.join(", "))
        }
    }
}

/// yaml.v2 rejects collection keys while decoding into `map[interface{}]interface{}`.
fn check_map_keys(node: &Node) -> Result<(), String> {
    match node {
        Node::Scalar(_) => Ok(()),
        Node::Seq { items, .. } => items.iter().try_for_each(check_map_keys),
        Node::Map { entries, .. } => entries.iter().try_for_each(|(k, v)| match k {
            Node::Scalar(_) => check_map_keys(v),
            collection => Err(format!("yaml: invalid map key: {}", go_syntax(collection))),
        }),
    }
}

/// A yaml.v2 type error, e.g. "line 3: cannot unmarshal !!str `abc` into uint32".
fn type_error(node: &Node, go_type: &str) -> String {
    let value = match node {
        Node::Scalar(s) => {
            let text = s.text.as_bytes();
            if text.len() > 10 {
                format!(" `{}...`", String::from_utf8_lossy(&text[..7]))
            } else {
                format!(" `{}`", s.text)
            }
        }
        _ => String::new(),
    };
    format!(
        "line {}: cannot unmarshal {}{} into {}",
        node.line(),
        node.short_tag(),
        value,
        go_type
    )
}

fn unmarshal_errors(errors: &[String]) -> String {
    format!("yaml: unmarshal errors:\n  {}", errors.join("\n  "))
}

fn is_null(node: &Node) -> bool {
    matches!(node, Node::Scalar(s) if s.resolve() == Resolved::Null)
}

/// Null for a pointer field, which yaml.v2 leaves nil only for some null spellings.
fn is_nil_pointer(node: &Node) -> bool {
    matches!(node, Node::Scalar(s) if s.is_nil())
}

struct Decoder {
    errors: Vec<String>,
}

impl Decoder {
    fn string(&mut self, node: &Node) -> String {
        match node {
            Node::Scalar(s) if s.resolve() == Resolved::Null => String::new(),
            Node::Scalar(s) => s.text.clone(),
            other => {
                self.errors.push(type_error(other, "string"));
                String::new()
            }
        }
    }

    fn uint32(&mut self, node: &Node) -> u32 {
        let value = match node {
            Node::Scalar(s) => match s.resolve() {
                Resolved::Null => return 0,
                Resolved::Int(v) => u32::try_from(v).ok(),
                Resolved::Uint(v) => u32::try_from(v).ok(),
                Resolved::Float(v) if v >= 0.0 && v <= u32::MAX as f64 => Some(v as u32),
                _ => None,
            },
            _ => None,
        };
        value.unwrap_or_else(|| {
            self.errors.push(type_error(node, "uint32"));
            0
        })
    }

    fn boolean(&mut self, node: &Node) -> bool {
        match node {
            Node::Scalar(s) => match s.resolve() {
                Resolved::Null => false,
                Resolved::Bool(b) => b,
                _ => {
                    self.errors.push(type_error(node, "bool"));
                    false
                }
            },
            _ => {
                self.errors.push(type_error(node, "bool"));
                false
            }
        }
    }

    /// The entries of a mapping decoded into a struct, keyed by field name.
    fn fields<'a>(&mut self, node: &'a Node, go_type: &str) -> Option<Vec<(String, &'a Node)>> {
        match node {
            Node::Map { entries, .. } => Some(
                entries
                    .iter()
                    .map(|(k, v)| {
                        let name = match k {
                            Node::Scalar(s) if s.resolve() == Resolved::Null => String::new(),
                            Node::Scalar(s) => s.text.clone(),
                            _ => String::new(),
                        };
                        (name, v)
                    })
                    .collect(),
            ),
            n if is_null(n) => None,
            other => {
                self.errors.push(type_error(other, go_type));
                None
            }
        }
    }

    fn seq<'a>(&mut self, node: &'a Node, go_type: &str) -> &'a [Node] {
        match node {
            Node::Seq { items, .. } => items,
            n if is_null(n) => &[],
            other => {
                self.errors.push(type_error(other, go_type));
                &[]
            }
        }
    }

    fn root(&mut self, node: &Node) -> YamlRoot {
        let mut root = YamlRoot::default();
        for (name, v) in self.fields(node, "config.YamlRoot").unwrap_or_default() {
            match name.as_str() {
                "domain" => root.domain = self.string(v),
                "descriptors" => root.descriptors = self.descriptors(v),
                _ => {}
            }
        }
        root
    }

    fn descriptors(&mut self, node: &Node) -> Vec<YamlDescriptor> {
        self.seq(node, "[]config.YamlDescriptor")
            .iter()
            .map(|item| self.descriptor(item))
            .collect()
    }

    fn descriptor(&mut self, node: &Node) -> YamlDescriptor {
        let mut d = YamlDescriptor::default();
        for (name, v) in self
            .fields(node, "config.YamlDescriptor")
            .unwrap_or_default()
        {
            match name.as_str() {
                "key" => d.key = self.string(v),
                "value" => d.value = self.string(v),
                "rate_limit" => self.rate_limit(v, &mut d.rate_limit),
                "descriptors" => d.descriptors = self.descriptors(v),
                "shadow_mode" => d.shadow_mode = self.boolean(v),
                "quota_mode" => d.quota_mode = self.boolean(v),
                "detailed_metric" => d.detailed_metric = self.boolean(v),
                "value_to_metric" => d.value_to_metric = self.boolean(v),
                "share_threshold" => d.share_threshold = self.boolean(v),
                "metadata" => self.metadata(v, &mut d.metadata),
                _ => {}
            }
        }
        d
    }

    /// A `*YamlRateLimit` field: decoding into an existing value overlays it, like yaml.v2.
    fn rate_limit(&mut self, node: &Node, out: &mut Option<YamlRateLimit>) {
        if is_nil_pointer(node) {
            *out = None;
            return;
        }
        let rl = out.get_or_insert_with(Default::default);
        let Some(fields) = self.fields(node, "config.YamlRateLimit") else {
            // Any other null spelling allocates the struct and zeroes it.
            *rl = YamlRateLimit::default();
            return;
        };
        for (name, v) in fields {
            match name.as_str() {
                "requests_per_unit" => rl.requests_per_unit = self.uint32(v),
                "unit" => rl.unit = self.string(v),
                "unlimited" => rl.unlimited = self.boolean(v),
                "name" => rl.name = self.string(v),
                "replaces" => {
                    rl.replaces = self
                        .seq(v, "[]config.yamlReplaces")
                        .iter()
                        .map(|item| {
                            let mut replaced = String::new();
                            for (name, v) in
                                self.fields(item, "config.yamlReplaces").unwrap_or_default()
                            {
                                if name == "name" {
                                    replaced = self.string(v);
                                }
                            }
                            replaced
                        })
                        .collect()
                }
                _ => {}
            }
        }
    }

    /// A `map[string]interface{}` field as a protobuf `Struct`, nil when empty. Decoding into
    /// an existing map adds to it, like yaml.v2.
    fn metadata(&mut self, node: &Node, out: &mut Option<Struct>) {
        match node {
            Node::Map { entries, .. } => {
                let mut fields: BTreeMap<_, _> = out.take().map(|s| s.fields).unwrap_or_default();
                for (k, v) in entries {
                    let key = self.string(k);
                    fields.insert(key, to_value(v));
                }
                *out = (!fields.is_empty()).then_some(Struct { fields });
            }
            n if is_null(n) => *out = None,
            other => self
                .errors
                .push(type_error(other, "map[string]interface {}")),
        }
    }
}

/// `structpb.NewValue` of a value decoded into `interface{}`.
fn to_value(node: &Node) -> Value {
    let kind = match node {
        Node::Scalar(s) => match s.resolve() {
            Resolved::Null => Kind::NullValue(0),
            Resolved::Bool(b) => Kind::BoolValue(b),
            Resolved::Int(v) => Kind::NumberValue(v as f64),
            Resolved::Uint(v) => Kind::NumberValue(v as f64),
            Resolved::Float(v) => Kind::NumberValue(v),
            Resolved::Str(s) => Kind::StringValue(s),
        },
        Node::Seq { items, .. } => Kind::ListValue(ListValue {
            values: items.iter().map(to_value).collect(),
        }),
        Node::Map { entries, .. } => Kind::StructValue(Struct {
            fields: dedup_entries(entries)
                .into_iter()
                .map(|(k, v)| (go_format_node(k), to_value(v)))
                .collect(),
        }),
    };
    Value { kind: Some(kind) }
}

/// `ConfigFileContentToYaml` without the file name prefix: parse, validate keys, decode.
pub(crate) fn parse_config(content: &str) -> Result<YamlRoot, String> {
    // Upstream writes each error at debug level before it panics with it.
    parsed_config(content).inspect_err(|e| crate::debug!("{e}"))
}

fn parsed_config(content: &str) -> Result<YamlRoot, String> {
    let loading = |e: String| format!("error loading config file: {e}");
    let root = super::yaml::parse(content).map_err(loading)?;
    match root.as_ref() {
        Some(map @ Node::Map { .. }) => check_map_keys(map).map_err(loading)?,
        Some(n) if !is_null(n) => {
            let error = type_error(n, "map[interface {}]interface {}");
            return Err(loading(unmarshal_errors(&[error])));
        }
        _ => {}
    }
    validate_keys(root.as_ref())?;
    let Some(root) = root else {
        return Ok(YamlRoot::default());
    };
    let mut decoder = Decoder { errors: Vec::new() };
    let decoded = decoder.root(&root);
    if decoder.errors.is_empty() {
        Ok(decoded)
    } else {
        Err(loading(unmarshal_errors(&decoder.errors)))
    }
}
