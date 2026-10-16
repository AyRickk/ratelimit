//! datadog-go v5.5.0's origin detection (`statsd/container_linux.go`): the container ID sent
//! with each metric, so the Datadog agent can tag it. Ported as it is, quirks included: outside
//! the host cgroup namespace, a container ID found in `mountinfo` is replaced by the cgroup's
//! inode (`in-<inode>`), and nothing is sent when `mountinfo` names no container.

use regex::Regex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

/// The inode of the host's cgroup namespace (Linux `proc_ns.h`), wrong under Docker-in-Docker.
const HOST_CGROUP_NAMESPACE_INODE: u64 = 0xEFFF_FFFB;

/// A line of `/proc/self/cgroup`, capturing its path.
static CGROUP_LINE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[0-9]+:[^:]*:(.+)$").unwrap());

/// A container ID at the end of a cgroup path: a UUID, a 64-hex ID or an ECS task ID.
static CONTAINER_ID: LazyLock<Regex> = LazyLock::new(|| {
    let uuid = "[0-9a-f]{8}[-_][0-9a-f]{4}[-_][0-9a-f]{4}[-_][0-9a-f]{4}[-_][0-9a-f]{12}";
    let container = "[0-9a-f]{64}";
    let task = "[0-9a-f]{32}-[0-9]+";
    Regex::new(&format!("({uuid}|{container}|{task})(?:.scope)?$")).unwrap()
});

/// A container's `hostname` mount in `/proc/self/mountinfo` (Go's `\s` and `\S` are ASCII).
static MOUNTINFO_ID: LazyLock<Regex> = LazyLock::new(|| {
    let id = "([0-9a-f]{64})|([0-9a-f]{32}-[0-9]{10})|([0-9a-f]{8}(-[0-9a-f]{4}){4}$)";
    Regex::new(&format!(
        r".*/([^\t\n\x0C\r /]+)/({id})/[^\t\n\x0C\r ]*hostname"
    ))
    .unwrap()
});

/// Where origin detection reads from.
#[derive(Debug, Clone)]
pub struct Sources {
    /// `/proc/self/cgroup`.
    pub cgroup: PathBuf,
    /// `/proc/self/mountinfo`.
    pub mountinfo: PathBuf,
    /// `/sys/fs/cgroup`.
    pub cgroup_mount: PathBuf,
}

impl Default for Sources {
    fn default() -> Self {
        Self {
            cgroup: "/proc/self/cgroup".into(),
            mountinfo: "/proc/self/mountinfo".into(),
            cgroup_mount: "/sys/fs/cgroup".into(),
        }
    }
}

/// This process's container ID, as datadog-go finds it. Only on Linux.
pub fn container_id() -> Option<String> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    detect(host_cgroup_namespace(), &Sources::default())
}

/// The container ID from `sources`: from the cgroup file in the host cgroup namespace, else the
/// cgroup's inode when `mountinfo` names a container.
pub fn detect(host_cgroup_namespace: bool, sources: &Sources) -> Option<String> {
    let id = if host_cgroup_namespace {
        parse_container_id(&std::fs::read_to_string(&sources.cgroup).ok()?)
    } else {
        let found = parse_mountinfo(&std::fs::read_to_string(&sources.mountinfo).ok()?);
        if found.is_empty() {
            String::new()
        } else {
            cgroup_inode(&sources.cgroup_mount, &sources.cgroup)
        }
    };
    (!id.is_empty()).then_some(id)
}

#[cfg(unix)]
fn host_cgroup_namespace() -> bool {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata("/proc/self/ns/cgroup").is_ok_and(|m| m.ino() == HOST_CGROUP_NAMESPACE_INODE)
}

#[cfg(not(unix))]
fn host_cgroup_namespace() -> bool {
    false
}

/// `parseContainerID`: the container ID of the first cgroup line ending with one.
fn parse_container_id(cgroup: &str) -> String {
    cgroup
        .lines()
        .filter_map(|line| CGROUP_LINE.captures(line))
        .find_map(|path| CONTAINER_ID.captures(&path[1]).map(|id| id[1].to_string()))
        .unwrap_or_default()
}

/// `parseMountinfo`: from the first line naming a container, the rightmost match, unless it is
/// a containerd sandbox.
fn parse_mountinfo(mountinfo: &str) -> String {
    for line in mountinfo.lines() {
        let Some(last) = MOUNTINFO_ID.captures_iter(line).last() else {
            continue;
        };
        if &last[1] != "sandboxes" {
            return last[2].to_string();
        }
    }
    String::new()
}

/// `getCgroupInode`: `in-<inode>` of the cgroup v1 memory controller's node, else of the
/// cgroup v2 node, under `mount`.
fn cgroup_inode(mount: &Path, cgroup: &Path) -> String {
    let Ok(cgroup) = std::fs::read_to_string(cgroup) else {
        return String::new();
    };
    let mut paths = HashMap::new();
    for line in cgroup.lines() {
        let tokens: Vec<&str> = line.split(':').collect();
        if let [_, controller @ ("memory" | ""), path] = tokens[..] {
            paths.insert(controller, path);
        }
    }
    for controller in ["memory", ""] {
        let Some(node) = paths.get(controller) else {
            continue;
        };
        let path = go_path_join(&[&mount.to_string_lossy(), controller, node]);
        if let Some(inode) = inode(Path::new(&path)) {
            return format!("in-{inode}");
        }
    }
    String::new()
}

#[cfg(unix)]
fn inode(path: &Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).ok().map(|m| m.ino())
}

#[cfg(not(unix))]
fn inode(_: &Path) -> Option<u64> {
    None
}

/// Go's `path.Join`: the non-empty elements joined with `/`, then cleaned.
fn go_path_join(elements: &[&str]) -> String {
    let joined = elements
        .iter()
        .filter(|e| !e.is_empty())
        .copied()
        .collect::<Vec<_>>()
        .join("/");
    if joined.is_empty() {
        return String::new();
    }
    let rooted = joined.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in joined.split('/') {
        match part {
            "" | "." => {}
            ".." if parts.last().is_some_and(|p| *p != "..") => {
                parts.pop();
            }
            ".." if rooted => {}
            part => parts.push(part),
        }
    }
    let cleaned = parts.join("/");
    match (rooted, cleaned.is_empty()) {
        (true, _) => format!("/{cleaned}"),
        (false, true) => ".".into(),
        (false, false) => cleaned,
    }
}
