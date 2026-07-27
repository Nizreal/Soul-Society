use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Enum Perintah sesuai Spec
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    // KV Operations
    Ping,
    Set { key: String, value: String },
    Get { key: String },
    Strlen { key: String },
    Append { key: String, value: String }, // Append string, create if not exist
    Del { key: String },

    // Membership Change (Manual)
    AddNode { id: u64, address: String },
    RemoveNode { id: u64 },

    // [BONUS] Transaction: Eksekusi banyak perintah sekaligus
    Transaction { commands: Vec<Command> },
}

// Entri Log Raft
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Command,
}

// Role Node
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Serialize, Deserialize)]
pub struct Snapshot{
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: HashMap<String, String>
}