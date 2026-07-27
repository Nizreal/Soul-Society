pub mod client;

use std::net::SocketAddr;

/// Helper sederhana untuk mengubah IP (String) dan Port (u16) menjadi SocketAddr
pub fn to_socket_addr(ip: &str, port: u16) -> SocketAddr {
    format!("{}:{}", ip, port)
        .parse()
        .unwrap_or_else(|_| panic!("Invalid IP/Port format: {}:{}", ip, port))
}
