//! Rate limit configuration: loading YAML config files and looking up the limit that applies
//! to a descriptor. A port of envoyproxy/ratelimit `src/config/config_impl.go`.

mod schema;
pub(crate) mod yaml;

use rate_limit_response::rate_limit::Unit;
use ratelimit_proto::envoy::extensions::common::ratelimit::v3::RateLimitDescriptor;
use ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response;
use schema::YamlDescriptor;
use std::collections::HashMap;

/// A config file to load: its name (used in error messages) and its YAML content.
#[derive(Debug, Clone)]
pub struct ConfigFile {
    pub name: String,
    pub content: String,
}

#[derive(Debug, Clone, Default)]
pub struct LoadOptions {
    /// `MERGE_DOMAIN_CONFIG`: several files may contribute descriptors to the same domain.
    pub merge_domain_configs: bool,
    /// `SANITIZE_DESCRIPTOR_METRIC_DOTS`: `.` in descriptor keys and values becomes `_` in
    /// stats keys (never in matching).
    pub sanitize_descriptor_metric_dots: bool,
}

/// A config error, formatted like envoyproxy/ratelimit's `RateLimitConfigError`: `<file>: <message>`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(String);

impl ConfigError {
    fn new(file: &str, message: impl std::fmt::Display) -> Self {
        Self(format!("{file}: {message}"))
    }
}

/// A configured limit, as found for a descriptor.
#[derive(Debug, Clone, PartialEq)]
pub struct RateLimit {
    /// The descriptor path the limit's stats are named after, e.g. `domain.key_value.subkey`.
    pub full_key: String,
    pub limit: rate_limit_response::RateLimit,
    pub name: String,
    pub unlimited: bool,
    /// `detailed_metric`: stats are named after the request's values, not the config's.
    pub detailed_metric: bool,
    pub shadow_mode: bool,
    pub quota_mode: bool,
    /// Names of limits this one replaces in the same request.
    pub replaces: Vec<String>,
    /// For each descriptor entry, the wildcard value (`files/*`) it matched with
    /// `share_threshold`, or empty; `None` when no entry matched that way.
    pub share_threshold_key_pattern: Option<Vec<String>>,
    pub metadata: Option<prost_types::Struct>,
}

impl RateLimit {
    fn new(full_key: String, requests_per_unit: u32, unit: i32, name: String) -> Self {
        Self {
            full_key,
            limit: rate_limit_response::RateLimit {
                name: name.clone(),
                requests_per_unit,
                unit,
                unit_multiplier: None,
            },
            name,
            unlimited: false,
            detailed_metric: false,
            shadow_mode: false,
            quota_mode: false,
            replaces: Vec::new(),
            share_threshold_key_pattern: None,
            metadata: None,
        }
    }
}

/// The result of looking up a descriptor.
#[derive(Debug, Clone, PartialEq)]
pub enum LimitLookup {
    /// The request's domain is not configured.
    UnknownDomain,
    /// The domain is configured but no limit applies to the descriptor.
    NoLimit,
    Limit(RateLimit),
}

#[derive(Debug, Default)]
struct Descriptor {
    descriptors: HashMap<String, Descriptor>,
    limit: Option<RateLimit>,
    /// The `key_value` patterns containing `*` among `descriptors`, in config order,
    /// pre-split on `*`.
    wildcards: Vec<(String, Vec<String>)>,
    value_to_metric: bool,
    /// With `share_threshold`, the descriptor's own `key_value` wildcard pattern.
    share_threshold_pattern: Option<String>,
}

/// A loaded set of config files.
#[derive(Debug)]
pub struct RateLimitConfig {
    domains: HashMap<String, Descriptor>,
    sanitize_dots: bool,
}

/// Loads and validates config files, failing on the first invalid one.
pub fn load(files: &[ConfigFile], options: &LoadOptions) -> Result<RateLimitConfig, ConfigError> {
    // Like upstream, every file is parsed and key-validated before any domain is loaded.
    let roots = files
        .iter()
        .map(|file| {
            schema::parse_config(&file.content).map_err(|e| ConfigError::new(&file.name, e))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let names: Vec<&str> = files.iter().map(|f| f.name.as_str()).collect();
    load_roots(&names, roots, options)
}

/// [`load`] for files read from disk (name, bytes): each is decoded as yaml.v2 reads its
/// input (UTF-8, or UTF-16 with a byte order mark), then parsed, in order; the first failure
/// is the error.
pub fn load_bytes(
    files: &[(String, Vec<u8>)],
    options: &LoadOptions,
) -> Result<RateLimitConfig, ConfigError> {
    let roots = files
        .iter()
        .map(|(name, bytes)| {
            let loading = |e: String| ConfigError::new(name, e);
            let content = yaml::decode(bytes).map_err(|e| {
                let e = format!("error loading config file: {e}");
                crate::debug!("{e}");
                loading(e)
            })?;
            schema::parse_config(&content).map_err(loading)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let names: Vec<&str> = files.iter().map(|(name, _)| name.as_str()).collect();
    load_roots(&names, roots, options)
}

/// The domains of parsed files, in order.
fn load_roots(
    names: &[&str],
    roots: Vec<schema::YamlRoot>,
    options: &LoadOptions,
) -> Result<RateLimitConfig, ConfigError> {
    let mut config = RateLimitConfig {
        domains: HashMap::new(),
        sanitize_dots: options.sanitize_descriptor_metric_dots,
    };
    for (name, root) in names.iter().zip(roots) {
        if root.domain.is_empty() {
            return Err(ConfigError::new(
                name,
                "config file cannot have empty domain",
            ));
        }
        if config.domains.contains_key(&root.domain) && !options.merge_domain_configs {
            return Err(ConfigError::new(
                name,
                format!("duplicate domain '{}' in config file", root.domain),
            ));
        }
        match config.domains.contains_key(&root.domain) {
            true => crate::debug!("patching domain: {}", root.domain),
            false => crate::debug!("loading domain: {}", root.domain),
        }
        let domain = config.domains.entry(root.domain.clone()).or_default();
        domain.load_descriptors(name, &format!("{}.", root.domain), &root.descriptors)?;
    }
    Ok(config)
}

/// `RateLimitResponse_RateLimit_Unit.String()`: the name, or the number when it has none.
pub fn unit_name(unit: i32) -> String {
    crate::protojson::RESPONSE_UNITS
        .iter()
        .find(|(_, n)| *n == unit)
        .map_or_else(|| unit.to_string(), |(name, _)| name.to_string())
}

/// A unit name as accepted by upstream: any case, `UNKNOWN` excluded.
fn parse_unit(unit: &str) -> Option<Unit> {
    Unit::from_str_name(&unit.to_uppercase()).filter(|u| *u != Unit::Unknown)
}

fn load_limit(
    file: &str,
    full_key: &str,
    config: &YamlDescriptor,
) -> Result<Option<RateLimit>, ConfigError> {
    let Some(rl) = &config.rate_limit else {
        return Ok(None);
    };
    let unit = parse_unit(&rl.unit);
    if rl.unlimited {
        if unit.is_some() {
            return Err(ConfigError::new(
                file,
                "should not specify rate limit unit when unlimited",
            ));
        }
    } else if unit.is_none() {
        return Err(ConfigError::new(
            file,
            format!("invalid rate limit unit '{}'", rl.unit),
        ));
    }
    // `NewRateLimit`'s `NewStats`.
    crate::debug!("Creating stats for key: '{full_key}'");
    for replaced in &rl.replaces {
        if replaced.is_empty() {
            return Err(ConfigError::new(
                file,
                "should not have an empty replaces entry",
            ));
        }
        if *replaced == rl.name {
            return Err(ConfigError::new(
                file,
                "replaces should not contain name of same descriptor",
            ));
        }
    }
    Ok(Some(RateLimit {
        unlimited: rl.unlimited,
        detailed_metric: config.detailed_metric,
        shadow_mode: config.shadow_mode,
        quota_mode: config.quota_mode,
        replaces: rl.replaces.clone(),
        metadata: config.metadata.clone(),
        ..RateLimit::new(
            full_key.to_string(),
            rl.requests_per_unit,
            unit.unwrap_or(Unit::Unknown) as i32,
            rl.name.clone(),
        )
    }))
}

impl Descriptor {
    fn load_descriptors(
        &mut self,
        file: &str,
        parent_key: &str,
        descriptors: &[YamlDescriptor],
    ) -> Result<(), ConfigError> {
        for config in descriptors {
            if config.key.is_empty() {
                return Err(ConfigError::new(file, "descriptor has empty key"));
            }
            let mut final_key = config.key.clone();
            if !config.value.is_empty() {
                final_key += "_";
                final_key += &config.value;
            }
            let new_parent_key = format!("{parent_key}{final_key}");
            if self.descriptors.contains_key(&final_key) {
                return Err(ConfigError::new(
                    file,
                    format!("duplicate descriptor composite key '{new_parent_key}'"),
                ));
            }
            let limit = load_limit(file, &new_parent_key, config)?;
            if config.share_threshold && !final_key.contains('*') {
                return Err(ConfigError::new(
                    file,
                    format!(
                        "share_threshold can only be used with wildcard values (containing '*'), but found key '{final_key}'"
                    ),
                ));
            }
            if final_key.contains('*') {
                let parts = final_key.split('*').map(str::to_string).collect();
                self.wildcards.push((final_key.clone(), parts));
            }
            crate::debug!(
                "loading descriptor: key={new_parent_key}{}",
                limit.as_ref().map_or(String::new(), |l| format!(
                    " ratelimit={{requests_per_unit={}, unit={}, unlimited={}, shadow_mode={}, quota_mode={}}}",
                    l.limit.requests_per_unit,
                    unit_name(l.limit.unit),
                    l.unlimited,
                    l.shadow_mode,
                    l.quota_mode
                ))
            );
            let mut descriptor = Descriptor {
                limit,
                value_to_metric: config.value_to_metric,
                share_threshold_pattern: config.share_threshold.then(|| final_key.clone()),
                ..Default::default()
            };
            descriptor.load_descriptors(
                file,
                &format!("{new_parent_key}."),
                &config.descriptors,
            )?;
            self.descriptors.insert(final_key, descriptor);
        }
        Ok(())
    }
}

impl RateLimitConfig {
    /// `IsEmptyDomains`: whether the config has no domain.
    pub fn is_empty_domains(&self) -> bool {
        self.domains.is_empty()
    }

    /// `Dump`: a line per limit, `<full key>: unit=<UNIT> requests_per_unit=<n>, shadow_mode:
    /// <bool>, quota_mode: <bool>`, each descriptor before those under it. Upstream walks Go
    /// maps, in a random order; domains and descriptors are sorted here.
    pub fn dump(&self) -> String {
        fn dump(descriptor: &Descriptor, out: &mut String) {
            if let Some(limit) = &descriptor.limit {
                let unit = unit_name(limit.limit.unit);
                out.push_str(&format!(
                    "{}: unit={unit} requests_per_unit={}, shadow_mode: {}, quota_mode: {}\n",
                    limit.full_key,
                    limit.limit.requests_per_unit,
                    limit.shadow_mode,
                    limit.quota_mode
                ));
            }
            let mut children: Vec<_> = descriptor.descriptors.iter().collect();
            children.sort_by(|a, b| a.0.cmp(b.0));
            for (_, child) in children {
                dump(child, out);
            }
        }
        let mut domains: Vec<_> = self.domains.iter().collect();
        domains.sort_by(|a, b| a.0.cmp(b.0));
        let mut out = String::new();
        for (_, domain) in domains {
            dump(domain, &mut out);
        }
        out
    }

    /// `GetLimit`: the limit that applies to `descriptor` in `domain`.
    pub fn get_limit<'a>(&'a self, domain: &str, descriptor: &RateLimitDescriptor) -> LimitLookup {
        crate::debug!("starting get limit lookup");
        let Some(mut current) = self.domains.get(domain) else {
            crate::debug!("unknown domain '{domain}'");
            return LimitLookup::UnknownDomain;
        };
        if let Some(limit) = &descriptor.limit {
            // The override replaces the config entirely; shadow mode is deliberately off.
            // envoy.type.v3.RateLimitUnit shares its numbering with the response unit.
            let key = self.descriptor_key(domain, descriptor);
            crate::debug!("Creating stats for key: '{key}'");
            return LimitLookup::Limit(RateLimit::new(
                key,
                limit.requests_per_unit,
                limit.unit,
                String::new(),
            ));
        }
        let mut found: Option<RateLimit> = None;
        let mut detailed_key = domain.to_string();
        let mut value_to_metric_key = domain.to_string();
        let mut share_patterns: HashMap<usize, String> = HashMap::new();
        let mut stopped_at_leaf = false;
        for (i, entry) in descriptor.entries.iter().enumerate() {
            detailed_key += &format!(
                ".{}_{}",
                self.sanitize(&entry.key),
                self.sanitize(&entry.value)
            );
            let key_value = format!("{}_{}", entry.key, entry.value);
            crate::debug!("looking up key: {key_value}");
            let mut matched_wildcard = None;
            let mut next = current.descriptors.get(&key_value);
            if next.is_none()
                && let Some((pattern, _)) = current
                    .wildcards
                    .iter()
                    .find(|(_, parts)| wildcard_match(parts, &key_value))
            {
                next = current.descriptors.get(pattern);
                matched_wildcard = Some(pattern.as_str());
            }
            let matched_using_value = next.is_some();
            if next.is_none() {
                crate::debug!("looking up key: {}", entry.key);
                next = current.descriptors.get(&entry.key);
            }

            // The value part of a `key_value` wildcard pattern, e.g. `files*` in `key_files*`.
            let pattern_value = |pattern: &'a str| {
                pattern
                    .strip_prefix(&format!("{}_", entry.key))
                    .unwrap_or(pattern)
            };
            if let (Some(_), Some(pattern)) = (
                matched_wildcard,
                next.and_then(|n| n.share_threshold_pattern.as_deref()),
            ) {
                let pattern = pattern_value(pattern);
                crate::debug!(
                    "tracking share_threshold for entry index {i} (key {}), wildcard pattern {pattern}",
                    entry.key
                );
                share_patterns.insert(i, pattern.to_string());
            }

            value_to_metric_key += ".";
            value_to_metric_key += &self.sanitize(&entry.key);
            if let Some(next) = next {
                let value = match matched_wildcard {
                    Some(_) if share_patterns.contains_key(&i) => share_patterns[&i].as_str(),
                    Some(_) if next.value_to_metric => entry.value.as_str(),
                    Some(pattern) => pattern_value(pattern),
                    None if matched_using_value || next.value_to_metric => entry.value.as_str(),
                    None => "",
                };
                if !value.is_empty() {
                    value_to_metric_key += "_";
                    value_to_metric_key += &self.sanitize(value);
                }
            }

            let Some(next) = next else {
                stopped_at_leaf = true;
                break;
            };
            if let Some(limit) = &next.limit {
                let final_key = match matched_using_value {
                    true => key_value.as_str(),
                    false => entry.key.as_str(),
                };
                crate::debug!("found rate limit: {final_key}");
                if i == descriptor.entries.len() - 1 {
                    found = Some(limit.clone());
                    // Upstream walks a Go map, in a random order (docs/parity.md §7).
                    if crate::log::enabled(crate::log::Level::Debug) {
                        let mut patterns: Vec<_> = share_patterns.iter().collect();
                        patterns.sort();
                        for (index, pattern) in patterns {
                            crate::debug!(
                                "share_threshold enabled for entry index {index}, using wildcard pattern {pattern}"
                            );
                        }
                    }
                } else {
                    crate::debug!(
                        "request depth does not match config depth, there are more entries in the request's descriptor"
                    );
                }
            }
            if next.descriptors.is_empty() {
                stopped_at_leaf = true;
                // A detailed limit is rebuilt with its stats.
                if let Some(limit) = found.as_ref().filter(|l| l.detailed_metric) {
                    crate::debug!("Creating stats for key: '{}'", limit.full_key);
                }
                break;
            }
            crate::debug!("iterating to next level");
            current = next;
        }
        if let Some(limit) = &mut found {
            if !share_patterns.is_empty() {
                limit.share_threshold_key_pattern = Some(
                    (0..descriptor.entries.len())
                        .map(|i| share_patterns.get(&i).cloned().unwrap_or_default())
                        .collect(),
                );
            }
            // Upstream rebuilds the limit with `NewRateLimit`, which drops its metadata, when a
            // detailed limit is found at a leaf or when value_to_metric changes the stats key.
            let rebuilt = if limit.detailed_metric {
                stopped_at_leaf
            } else {
                value_to_metric_key != limit.full_key
            };
            if rebuilt {
                limit.metadata = None;
            }
            let full_key = match &limit.share_threshold_key_pattern {
                Some(patterns) if limit.detailed_metric => {
                    self.share_threshold_key(domain, descriptor, patterns)
                }
                _ if limit.detailed_metric => detailed_key,
                _ => value_to_metric_key,
            };
            if crate::log::enabled(crate::log::Level::Debug)
                && (limit.detailed_metric || full_key != limit.full_key)
            {
                crate::debug!("Creating stats for key: '{full_key}'");
            }
            limit.full_key = full_key;
        }
        found.map_or(LimitLookup::NoLimit, LimitLookup::Limit)
    }
}

/// Whether `value` matches a pattern pre-split on `*`, each `*` matching zero or more characters.
fn wildcard_match(parts: &[String], value: &str) -> bool {
    let [first, middle @ .., last] = parts else {
        return parts.first().is_some_and(|p| p == value);
    };
    if !value.starts_with(first.as_str()) || !value.ends_with(last.as_str()) {
        return false;
    }
    if value.len() < parts.iter().map(String::len).sum() {
        return false;
    }
    let mut remaining = &value[first.len()..value.len() - last.len()];
    for part in middle {
        let Some(idx) = remaining.find(part.as_str()) else {
            return false;
        };
        remaining = &remaining[idx + part.len()..];
    }
    true
}

impl RateLimitConfig {
    /// A descriptor key or value as written into a stats key.
    fn sanitize<'a>(&self, s: &'a str) -> std::borrow::Cow<'a, str> {
        if self.sanitize_dots {
            s.replace('.', "_").into()
        } else {
            s.into()
        }
    }

    /// `domain.key1_value1.key2`: the stats key of a request's limit override.
    fn descriptor_key(&self, domain: &str, descriptor: &RateLimitDescriptor) -> String {
        let entries: Vec<String> = descriptor
            .entries
            .iter()
            .map(|e| {
                if e.value.is_empty() {
                    self.sanitize(&e.key).into_owned()
                } else {
                    format!("{}_{}", self.sanitize(&e.key), self.sanitize(&e.value))
                }
            })
            .collect();
        format!("{domain}.{}", entries.join("."))
    }

    /// The detailed stats key when `share_threshold` applies: shared entries use their pattern.
    fn share_threshold_key(
        &self,
        domain: &str,
        descriptor: &RateLimitDescriptor,
        patterns: &[String],
    ) -> String {
        let mut key = domain.to_string();
        for (entry, pattern) in descriptor.entries.iter().zip(patterns) {
            key += ".";
            key += &self.sanitize(&entry.key);
            let value = if pattern.is_empty() {
                &entry.value
            } else {
                pattern
            };
            if !value.is_empty() {
                key += "_";
                key += &self.sanitize(value);
            }
        }
        key
    }
}
