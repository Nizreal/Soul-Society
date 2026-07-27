use serde::{Deserialize, Serialize};
use serde_json::{from_str, to_string_pretty};
use tracing::{debug, info};

use crate::{domain::{LogEntry, Role, Command}, error::NodeError}; 
use std::{collections::HashMap, fs::{read_to_string, rename, write}, path::Path};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HardState {
    current_term: u64,
    voted_for: Option<u64>,
    log: Vec<LogEntry>,
    last_included_index: u64,
    last_included_term: u64,
}

pub struct RaftState {
    // Persistent
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub log: Vec<LogEntry>,

    // Volatile
    pub commit_index: u64,
    pub last_applied: u64,
    pub role: Role,
    pub current_leader: Option<u64>,

    // Leader Only
    pub next_index: HashMap<u64, u64>,
    pub match_index: HashMap<u64, u64>,
    
    // Membership
    pub my_addr: String,
    pub peers: HashMap<u64, String>, // ID -> Address
    pub my_id: u64,

    // Snapshot
    pub last_included_index: u64,
    pub last_included_term: u64,
}

impl RaftState {
    pub fn new(id: u64, peers: HashMap<u64, String>) -> Self {
        let storage_path = format!("node_{}.json", id);
        if let Ok(hs) = Self::load_hs(&storage_path) {
            info!("Loaded persistent state from {}", storage_path);
            return Self::restore(id, peers, hs);
        }

        // dummy entry at index 0 (matches last_included_index=0)
        let dummy_entry = LogEntry {
            term: 0,
            index: 0,
            command: Command::Ping,
        };

        info!("Initializing new state for Node {}", id);
        Self {
            current_term: 0,
            voted_for: None,
            log: vec![dummy_entry],
            commit_index: 0,
            last_applied: 0,
            current_leader: None,
            role: Role::Follower,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            peers,
            my_id: id,
            my_addr: format!("127.0.0.1:{}", 8000 + id),
            last_included_index: 0,
            last_included_term: 0,
        }
    }
    
    // --- Log Access Helpers (Virtual Indexing) ---

    pub fn last_log_index(&self) -> u64 {
        self.last_included_index + self.log.len() as u64 - 1
    }

    pub fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(self.last_included_term) 
    }

    /// Safely gets the term of a log entry at a specific logical index.
    /// Returns 0 if index is unknown or compacted (unless it matches last_included_index).
    pub fn get_log_term(&self, index: u64) -> u64 {
        if index < self.last_included_index {
            return 0; // Too old (compacted)
        }
        if index == self.last_included_index {
            return self.last_included_term;
        }
        let physical_idx = (index - self.last_included_index) as usize;
        if physical_idx < self.log.len() {
            self.log[physical_idx].term
        } else {
            0
        }
    }

    // --- Core Raft Logic ---

    pub fn append_entries(
        &mut self,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>
    ) -> bool {
        // 1. Safety Check: If request is for an index we've already compacted, we can't verify it.
        // The Leader should have sent InstallSnapshot instead.
        if prev_log_index < self.last_included_index {
            return false;
        }

        // 2. Consistency Check: Ensure we have an entry at prev_log_index
        if prev_log_index > self.last_log_index() {
            return false;
        }

        // 3. Term Check: Ensure the term matches
        if self.get_log_term(prev_log_index) != prev_log_term {
            return false;
        }

        // 4. Append / Conflict Resolution
        let mut index = prev_log_index + 1;
        for entry in entries {
            if index <= self.last_included_index {
                // Skip entries that are part of our snapshot (redundant)
                index += 1;
                continue;
            }

            let physical_idx = (index - self.last_included_index) as usize;

            if physical_idx < self.log.len() {
                if self.log[physical_idx].term != entry.term {
                    // Conflict: Delete existing entry and all that follow
                    self.log.truncate(physical_idx);
                    self.log.push(entry);
                }
            } else {
                // No conflict: Append new entry
                self.log.push(entry);
            }
            index += 1;
        }
        
        true
    }

    pub fn is_log_up_to_date(
        &self,
        last_log_index: u64,
        last_log_term: u64
    ) -> bool {
        let my_last_term = self.last_log_term();
        let my_last_index = self.last_log_index();

        if last_log_term != my_last_term {
            return last_log_term > my_last_term;
        }
        last_log_index >= my_last_index
    }

    // --- Role & Term Management ---

    pub fn increment_term(&mut self) {
        self.current_term += 1;
        self.voted_for = Some(self.my_id);
    }

    pub fn update_term(&mut self, new_term: u64) {
        if new_term > self.current_term {
            self.current_term = new_term;
            self.voted_for = None;
            self.role = Role::Follower;
        }
    }

    pub fn become_follower(&mut self, term: u64) {
        self.update_term(term);
        self.role = Role::Follower;
    }

    pub fn become_candidate(&mut self) {
        self.role = Role::Candidate;
        self.increment_term();
    }

    pub fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.reset_leader_state();
    }

    pub fn reset_leader_state(&mut self) {
        let last_idx = self.last_log_index();
        for peer_id in self.peers.keys() {
            self.next_index.insert(*peer_id, last_idx + 1);
            self.match_index.insert(*peer_id, 0);
        }
    }

    pub fn update_next_index(&mut self, peer_id: u64, index: u64) {
        self.next_index.insert(peer_id, index);
    }

    pub fn update_match_index(&mut self, peer_id: u64, index: u64) {
        self.match_index.insert(peer_id, index);
    }

    pub fn advance_commit_index(&mut self) -> Option<u64> {
        if self.role != Role::Leader {
            return None;
        }

        let mut match_indexes: Vec<u64> = self.match_index.values().cloned().collect();
        match_indexes.push(self.last_log_index());
        match_indexes.sort_unstable_by(|a, b| b.cmp(a));

        let majority_idx = match_indexes.len() / 2;
        let n = match_indexes[majority_idx];

        // New condition: n must be > last_included_index to be in the current log vec
        if n > self.commit_index && n > self.last_included_index && n <= self.last_log_index() {
            let physical_idx = (n - self.last_included_index) as usize;
            // Check term of entry at n
            if self.log[physical_idx].term == self.current_term {
                self.commit_index = n;
                return Some(n);
            }
        }

        None
    }

    // --- Persistence ---

    pub fn save_hs_to_disk(hs: HardState, id: u64) -> Result<(), NodeError> {
        let path = format!("node_{}.json", id);
        let tmp_path = format!("{}.tmp", path);

        let json = to_string_pretty(&hs)
            .map_err(|e| NodeError::Internal(format!("Serialize error: {}", e)))?;

        write(&tmp_path, json)
            .map_err(|e| NodeError::Internal(format!("Write error: {}", e)))?;
        rename(&tmp_path, &path)
            .map_err(|e| NodeError::Internal(format!("Rename error: {}", e)))?;
        
        debug!("Persisted hard state to disk (term={}, last_idx={})", hs.current_term, hs.last_included_index);
        Ok(())
    }

    fn load_hs(path: &str) -> Result<HardState, NodeError> {
        if !Path::new(path).exists() {
            return Err(NodeError::Internal("File not found".into()));
        }
        let content = read_to_string(path)
            .map_err(|e| NodeError::Internal(format!("Read error: {}", e)))?;
        from_str(&content)
            .map_err(|e| NodeError::Internal(format!("Deserialize error: {}", e)))
    }

    fn restore(
        id: u64,
        peers: HashMap<u64, String>,
        hs: HardState
    ) -> Self {
        // If we have a snapshot, log[0] is the snapshot placeholder
        let dummy_entry = LogEntry {
            term: hs.last_included_term,
            index: hs.last_included_index,
            command: Command::Ping, 
        };

        // If log on disk is empty (which technically shouldn't happen if properly persisted with dummy),
        // we recreate the dummy.
        let log = if hs.log.is_empty() { 
            vec![dummy_entry]
        } else {
            hs.log
        };

        Self {
            current_term: hs.current_term,
            voted_for: hs.voted_for,
            log,
            commit_index: hs.last_included_index, // Commit index starts at least at snapshot
            last_applied: hs.last_included_index, // Last applied also starts at snapshot
            current_leader: None,
            role: Role::Follower,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            peers,
            my_id: id,
            my_addr: format!("127.0.0.1:{}", 8000 + id),
            last_included_index: hs.last_included_index,
            last_included_term: hs.last_included_term,
        }
    }

    pub fn get_hs(&self) -> HardState {
        HardState { 
            current_term: self.current_term,
            voted_for: self.voted_for, 
            log: self.log.clone(),
            last_included_index: self.last_included_index,
            last_included_term: self.last_included_term,
        }
    }
}
