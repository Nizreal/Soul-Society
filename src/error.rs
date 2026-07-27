use serde::{Deserialize, Serialize};
use std::io;
use std::net::AddrParseError;
use tarpc::client::RpcError;

#[derive(Debug, Serialize, Deserialize, thiserror::Error)]
pub enum NodeError {
    #[error("Not the leader. Leader is at {leader_addr:?}")]
    NotLeader {
        leader_addr: Option<String>, // Alamat IP:Port Leader
    },
    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<io::Error> for NodeError {
    fn from(err: io::Error) -> Self {
        NodeError::Internal(format!("IO error: {}", err))
    }
}

impl From<RpcError> for NodeError {
    fn from(err: RpcError) -> Self {
        NodeError::Internal(format!("RPC error: {}", err))
    }
}

impl From<AddrParseError> for NodeError {
    fn from(err: AddrParseError) -> Self {
        NodeError::Internal(format!("Address parse error: {}", err))
    }
}