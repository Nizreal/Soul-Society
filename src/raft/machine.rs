use std::collections::HashMap;
use tracing::info;
use serde::{Serialize, Deserialize};

use crate::domain::Command;

#[derive(Clone, Serialize, Deserialize)]
pub struct StateMachine {
    pub data: HashMap<String, String>,
}

impl StateMachine {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Menerapkan command yang sudah committed ke dalam state
    pub fn apply(&mut self, cmd: &Command) -> String {
        match cmd {
            Command::Ping => "PONG".to_string(),

            Command::Set { key, value } => {
                self.data.insert(key.clone(), value.clone());
                "OK".to_string()
            }

            Command::Get { key } => {
                self.data.get(key).cloned().unwrap_or_else(|| "".to_string())
            }

            Command::Strlen { key } => {
                match self.data.get(key) {
                    Some(val) => val.len().to_string(),
                    None => "0".to_string(),
                }
            }

            Command::Append { key, value } => {
                self.data
                    .entry(key.clone())
                    .and_modify(|v| v.push_str(value))
                    .or_insert(value.clone());
                "OK".to_string()
            }

            Command::Del { key } => {
                match self.data.remove(key) {
                    Some(val) => val,
                    None => "".to_string(),
                }
            }

            // Membership changes ditangani di layer Raft, bukan di KV Store,
            // tapi kita bisa return OK saja.
            Command::AddNode { id, address } => {
                info!("Membership change applied: Added node {} at {}", id, address);
                "OK".to_string()
            }
            Command::RemoveNode { id } => {
                info!("Membership change applied: Removed node {}", id);
                "OK".to_string()
            }

            // [BONUS] Handle Transaction
            Command::Transaction { commands } => {
                let mut results = Vec::new();
                // Eksekusi setiap perintah dalam batch
                for sub_cmd in commands {
                    // Panggil diri sendiri secara rekursif
                    let res = self.apply(sub_cmd);
                    results.push(res);
                }
                // Gabungkan hasil (misal dipisah koma atau format array JSON)
                format!("[{}]", results.join(", "))
            }
        }
    }
}