use crate::domain::{Command, LogEntry};
use crate::error::NodeError;
use std::collections::HashMap;

// Definisi Service Tarpc
#[tarpc::service]
pub trait RaftService {
    // --- Client RPCs ---
    // Client mengirim command ke sini. Redirect jika bukan Leader.
    async fn execute(cmd: Command) -> Result<String, NodeError>;

    // Debugging: Ambil log (Spec No. 6b)
    async fn request_log() -> Vec<LogEntry>;

    // --- Membership RPC ---
    async fn apply_membership(node_id: u64, node_addr: String) -> Result<ApplyMembershipResponse, NodeError>;
    async fn remove_membership(node_id: u64) -> Result<(), NodeError>;

    // --- Raft Internal RPCs ---
    
    // Vote Request (Election)
    async fn request_vote(
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64
    ) -> RequestVoteReply;

    // Heartbeat & Replication
    async fn append_entries(
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64
    ) -> AppendEntriesReply;

    async fn install_snapshot(
        term: u64,
        leader_id: u64,
        last_included_index : u64,
        last_included_term: u64,
        data: Vec<u8>,
        done: bool
    ) -> InstallSnapshotReply;
}

// Struct Reply
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct RequestVoteReply {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct AppendEntriesReply {
    pub term: u64,
    pub success: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ApplyMembershipResponse {
    pub peers: HashMap<u64, String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct InstallSnapshotReply {
    pub term: u64,
    pub success: bool
}