//! UDP server for ILP (InfluxDB Line Protocol) ingestion.
//!
//! Unlike the TCP server, the UDP server is connectionless and
//! fire-and-forget. Each datagram may contain one or more newline-
//! delimited ILP lines. This is useful for high-throughput metrics
//! where occasional packet loss is acceptable.

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::net::UdpSocket;

use exchange_core::table::WriteMode;

use super::parser::{parse_ilp_line, IlpLine};
use super::server::flush_batch_public;

/// Default port for the ILP UDP server.
pub const DEFAULT_ILP_UDP_PORT: u16 = 9010;

/// Default batch size: flush after this many lines.
const DEFAULT_UDP_BATCH_SIZE: usize = 500;

/// Configuration for the ILP UDP server.
#[derive(Debug, Clone)]
pub struct IlpUdpServerConfig {
    /// Address to listen on.
    pub addr: SocketAddr,
    /// Database root directory.
    pub db_root: PathBuf,
    /// Number of ILP lines to batch before flushing.
    pub batch_size: usize,
    /// Controls whether writes go through WAL for durability.
    pub write_mode: WriteMode,
}

impl IlpUdpServerConfig {
    pub fn new(addr: SocketAddr, db_root: PathBuf) -> Self {
        Self {
            addr,
            db_root,
            batch_size: DEFAULT_UDP_BATCH_SIZE,
            write_mode: WriteMode::default(),
        }
    }
}

/// Start the ILP UDP server.
///
/// Receives datagrams containing ILP lines, parses them, batches
/// writes, and flushes to disk. Runs forever (or until the runtime
/// is shut down).
pub async fn start_ilp_udp_server(addr: SocketAddr, db_root: PathBuf) -> io::Result<()> {
    let config = IlpUdpServerConfig::new(addr, db_root);
    start_ilp_udp_server_with_config(config).await
}

/// Start the ILP UDP server with full configuration.
pub async fn start_ilp_udp_server_with_config(config: IlpUdpServerConfig) -> io::Result<()> {
    let socket = UdpSocket::bind(config.addr).await?;
    let config = Arc::new(config);

    tracing::info!(addr = %config.addr, "ILP UDP server listening");

    let mut buf = vec![0u8; 65536];
    let mut batch: Vec<IlpLine> = Vec::with_capacity(config.batch_size);

    loop {
        let (len, src) = socket.recv_from(&mut buf).await?;
        let data = &buf[..len];

        // Parse the datagram as one or more newline-delimited ILP lines.
        let text = match std::str::from_utf8(data) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(peer = %src, error = %e, "ILP UDP: invalid UTF-8, skipping datagram");
                continue;
            }
        };

        for line in text.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }

            match parse_ilp_line(trimmed) {
                Ok(parsed) => {
                    batch.push(parsed);
                    if batch.len() >= config.batch_size {
                        let to_flush = std::mem::replace(
                            &mut batch,
                            Vec::with_capacity(config.batch_size),
                        );
                        let db_root = config.db_root.clone();
                        let wm = config.write_mode;
                        // Flush in a blocking task to avoid blocking the UDP recv loop.
                        tokio::task::spawn_blocking(move || flush_batch_public(&db_root, to_flush, wm));
                    }
                }
                Err(e) => {
                    tracing::warn!(peer = %src, error = %e, line = %trimmed, "ILP UDP parse error, skipping line");
                }
            }
        }

        // If the batch has accumulated some lines but not enough for a flush,
        // and we've been idle, flush proactively to reduce latency.
        // For simplicity, we flush at the end of each datagram if non-empty.
        if !batch.is_empty() {
            let to_flush = std::mem::replace(
                &mut batch,
                Vec::with_capacity(config.batch_size),
            );
            let db_root = config.db_root.clone();
            let wm = config.write_mode;
            tokio::task::spawn_blocking(move || flush_batch_public(&db_root, to_flush, wm));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_udp_config_defaults() {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
        let dir = TempDir::new().unwrap();
        let config = IlpUdpServerConfig::new(addr, dir.path().to_path_buf());
        assert_eq!(config.batch_size, DEFAULT_UDP_BATCH_SIZE);
    }

    #[tokio::test]
    async fn test_udp_server_receives_datagram() {
        let dir = TempDir::new().unwrap();
        let db_root = dir.path().to_path_buf();

        // Bind to random port.
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = socket.local_addr().unwrap();
        drop(socket);

        let config = IlpUdpServerConfig {
            addr: server_addr,
            db_root: db_root.clone(),
            batch_size: 1,
            write_mode: WriteMode::Direct,
        };

        // Start server in background.
        let handle = tokio::spawn(start_ilp_udp_server_with_config(config));

        // Give server time to bind.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Send a datagram with ILP data.
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let line = "trades,symbol=BTC/USD price=60000.0,volume=1.5 1710460800000000000\n";
        client.send_to(line.as_bytes(), server_addr).await.unwrap();

        // Wait for the batch to be flushed.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Verify the table was auto-created and data written.
        let table_dir = db_root.join("trades");
        assert!(table_dir.exists(), "trades table should be auto-created");

        // Abort the server.
        handle.abort();
    }

    #[tokio::test]
    async fn test_udp_multiple_lines_in_datagram() {
        let dir = TempDir::new().unwrap();
        let db_root = dir.path().to_path_buf();

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = socket.local_addr().unwrap();
        drop(socket);

        let config = IlpUdpServerConfig {
            addr: server_addr,
            db_root: db_root.clone(),
            batch_size: 10,
            write_mode: WriteMode::Direct,
        };

        let handle = tokio::spawn(start_ilp_udp_server_with_config(config));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Send multiple lines in a single datagram.
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let lines = "metrics,host=server1 cpu=0.85 1710460800000000000\n\
                     metrics,host=server2 cpu=0.42 1710460801000000000\n\
                     metrics,host=server1 cpu=0.90 1710460802000000000\n";
        client.send_to(lines.as_bytes(), server_addr).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let table_dir = db_root.join("metrics");
        assert!(table_dir.exists(), "metrics table should be auto-created");

        handle.abort();
    }

    #[tokio::test]
    async fn test_udp_ignores_invalid_utf8() {
        let dir = TempDir::new().unwrap();
        let db_root = dir.path().to_path_buf();

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = socket.local_addr().unwrap();
        drop(socket);

        let config = IlpUdpServerConfig {
            addr: server_addr,
            db_root: db_root.clone(),
            batch_size: 1,
            write_mode: WriteMode::Direct,
        };

        let handle = tokio::spawn(start_ilp_udp_server_with_config(config));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Send invalid UTF-8 bytes.
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.send_to(&[0xff, 0xfe, 0xfd], server_addr).await.unwrap();

        // Server should not crash; just skip the datagram.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Now send valid data.
        let line = "test_table,tag=val field=1.0 1710460800000000000\n";
        client.send_to(line.as_bytes(), server_addr).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert!(db_root.join("test_table").exists());

        handle.abort();
    }
}
