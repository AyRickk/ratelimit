//! Listening as upstream's Go servers do.

use std::net::SocketAddr;

/// Go's `net.Listen("tcp", addr)` (and go-reuseport's, with `reuse_port`): a wildcard host
/// (empty, `0.0.0.0` or `::`) listens on both IPv4 and IPv6 (IPv4 only when IPv6 is
/// unavailable); a host name on its first IPv4 address, else its first address.
pub async fn listen(addr: &str, reuse_port: bool) -> std::io::Result<tokio::net::TcpListener> {
    let invalid =
        |message: &str| std::io::Error::new(std::io::ErrorKind::InvalidInput, message.to_string());
    let (host, port) = addr
        .rsplit_once(':')
        .ok_or_else(|| invalid("missing port in address"))?;
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    let port: u16 = match port {
        "" => 0,
        port => port.parse().map_err(|_| invalid("invalid port"))?,
    };
    let bind = |address: SocketAddr| -> std::io::Result<tokio::net::TcpListener> {
        let domain = socket2::Domain::for_address(address);
        let socket = socket2::Socket::new(domain, socket2::Type::STREAM, None)?;
        if address.is_ipv6() && address.ip().is_unspecified() {
            socket.set_only_v6(false)?;
        }
        // Like Go on Unix.
        socket.set_reuse_address(true)?;
        if reuse_port {
            socket.set_reuse_port(true)?;
        }
        socket.bind(&address.into())?;
        socket.listen(1024)?;
        socket.set_nonblocking(true)?;
        tokio::net::TcpListener::from_std(socket.into())
    };
    if matches!(host, "" | "0.0.0.0" | "::") {
        let any6 = SocketAddr::from((std::net::Ipv6Addr::UNSPECIFIED, port));
        let any4 = SocketAddr::from((std::net::Ipv4Addr::UNSPECIFIED, port));
        // Go falls back to IPv4 only when the system has no IPv6.
        return match bind(any6) {
            Err(e) if e.raw_os_error() == Some(libc_eafnosupport()) => bind(any4),
            result => result,
        };
    }
    let addresses: Vec<SocketAddr> = tokio::net::lookup_host((host, port)).await?.collect();
    let address = addresses
        .iter()
        .find(|a| a.is_ipv4())
        .or(addresses.first())
        .ok_or_else(|| invalid("no address"))?;
    bind(*address)
}

/// `net.JoinHostPort`: `host:port`, with brackets around a host containing `:`.
pub fn join_host_port(host: &str, port: i64) -> String {
    match host.contains(':') {
        true => format!("[{host}]:{port}"),
        false => format!("{host}:{port}"),
    }
}

/// `EAFNOSUPPORT`: the address family (here IPv6) is not supported.
fn libc_eafnosupport() -> i32 {
    if cfg!(target_os = "linux") { 97 } else { 47 }
}

/// Go's `*net.OpError` for a listener that could not bind: `listen <network> <address>: bind:
/// <errno>`; other errors (an address that does not parse) keep their text.
pub fn listen_error(network: &str, address: &str, error: &std::io::Error) -> String {
    match error.raw_os_error() {
        Some(_) => format!(
            "listen {network} {address}: bind: {}",
            crate::go::errno_text(error)
        ),
        None => error.to_string(),
    }
}
