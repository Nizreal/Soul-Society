use rand::{rng, Rng};
use tarpc::context;
use tokio::sync::{mpsc, oneshot};
use tokio::task::spawn_blocking;
use tokio::time::{interval, Instant, MissedTickBehavior, Sleep};
use tracing::{debug, error, info, warn};

use crate::domain::{Command, LogEntry, Role, Snapshot};
use crate::error::NodeError;
use crate::rpc::{
    AppendEntriesReply, ApplyMembershipResponse, InstallSnapshotReply, RaftServiceClient,
    RequestVoteReply,
};
use crate::utils::client::execute_with_redirect;

use super::state::RaftState;
use crate::raft::machine::StateMachine;
use std::cmp::min;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

// consts for time (in ms)
const ELECTION_TIMEOUT_MIN: u64 = 1500;
const ELECTION_TIMEOUT_MAX: u64 = 3000;
const HEARTBEAT_INTERVAL: u64 = 500;
const RAFT_LOG_SIZE_LIMIT: u64 = 10;

// Pesan yang bisa dikirim ke Actor
pub enum ActorMsg {
    ClientRequest {
        cmd: Command,
        reply_to: oneshot::Sender<Result<String, NodeError>>,
    },
    RequestLog {
        reply_to: oneshot::Sender<Vec<LogEntry>>,
    },
    // Membership Change Message
    ApplyMembership {
        node_id: u64,
        node_addr: String,
        reply_to: oneshot::Sender<Result<ApplyMembershipResponse, NodeError>>,
    },
    RemoveMembership {
        node_id: u64,
        reply_to: oneshot::Sender<Result<(), NodeError>>,
    },
    UpdatePeerClient {
        node_id: u64,
        client: RaftServiceClient,
    },
    PeerDisconnected {
        peer_id: u64,
    },
    // Pesan Internal Raft (RPC masuk di-convert jadi pesan ini)
    RequestVote {
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
        reply_to: oneshot::Sender<RequestVoteReply>,
    },
    AppendEntries {
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
        reply_to: oneshot::Sender<AppendEntriesReply>,
    },
    AppendEntriesResult {
        peer_id: u64,
        term: u64,
        success: bool,
        last_log_index: u64,
    },
    VoteResult {
        peer_id: u64,
        election_term: u64,
        reply: RequestVoteReply,
    },
    InstallSnapshot {
        term: u64,
        leader_id: u64,
        last_included_index: u64,
        last_included_term: u64,
        data: Vec<u8>,
        done: bool,
        reply_to: oneshot::Sender<InstallSnapshotReply>,
    },
    InstallSnapshotReply {
        term: u64,
        success: bool,
    },
    TriggerSnapshot,
}

pub struct RaftActor {
    state: RaftState,
    inbox: mpsc::Receiver<ActorMsg>,
    msg_sender: mpsc::Sender<ActorMsg>,
    peers: HashMap<u64, RaftServiceClient>,
    state_machine: StateMachine,
    // Mapping: Log Index -> Channel untuk balas ke Client
    pending_requests: BTreeMap<u64, oneshot::Sender<Result<String, NodeError>>>,
    pending_membership: BTreeMap<u64, oneshot::Sender<Result<ApplyMembershipResponse, NodeError>>>,
    pending_removals: BTreeMap<u64, oneshot::Sender<Result<(), NodeError>>>,
    election_votes: u64,
    election_term: Option<u64>,
}

impl RaftActor {
    pub fn new(
        mut state: RaftState,
        inbox: mpsc::Receiver<ActorMsg>,
        msg_sender: mpsc::Sender<ActorMsg>,
        peers: HashMap<u64, RaftServiceClient>,
    ) -> Self {
        let mut state_machine = StateMachine::new();
        let snapshot_path = format!("snapshot_{}.json", state.my_id);

        if let Ok(content) = std::fs::read_to_string(&snapshot_path) {
            if let Ok(snapshot) = serde_json::from_str::<Snapshot>(&content) {
                info!(
                    "Loaded snapshot from {} (last_included_index={})",
                    snapshot_path, snapshot.last_included_index
                );
                state_machine.data = snapshot.data;
            } else {
                warn!("Failed to deserialize snapshot from {}", snapshot_path);
            }
        } else {
            info!(
                "No snapshot found at {}, starting with empty state machine.",
                snapshot_path
            );
        }

        // Rebuild state exactly through the persisted commit point.  Snapshot
        // data is the base and committed entries after it must be replayed.
        let replay_from = state.last_included_index + 1;
        for index in replay_from..=state.commit_index {
            let physical = (index - state.last_included_index) as usize;
            if let Some(entry) = state.log.get(physical) {
                match &entry.command {
                    Command::AddNode { id, address } if *id != state.my_id => {
                        state.peers.insert(*id, address.clone());
                    }
                    Command::RemoveNode { id } => {
                        state.peers.remove(id);
                    }
                    command => {
                        state_machine.apply(command);
                    }
                }
            }
        }
        state.last_applied = state.commit_index;
        Self {
            state,
            inbox,
            msg_sender,
            peers,
            state_machine,
            pending_requests: BTreeMap::new(),
            pending_membership: BTreeMap::new(),
            pending_removals: BTreeMap::new(),
            election_votes: 0,
            election_term: None,
        }
    }

    fn random_election_timeout() -> Duration {
        let ms = rng().random_range(ELECTION_TIMEOUT_MIN..=ELECTION_TIMEOUT_MAX);
        Duration::from_millis(ms)
    }

    async fn persist_state(&self) -> Result<(), NodeError> {
        let hs = self.state.get_hs();
        let id = self.state.my_id;

        spawn_blocking(move || RaftState::save_hs_to_disk(hs, id))
            .await
            .map_err(|e| NodeError::Internal(format!("Join error: {}", e)))??;

        Ok(())
    }

    fn update_readiness(&self) {
        if self.state.role == Role::Leader || self.state.current_leader.is_some() {
            if let Err(error) = std::fs::write("raft.ready", b"ready\n") {
                warn!("cannot update readiness: {error}");
            }
        } else {
            let _ = std::fs::remove_file("raft.ready");
        }
    }

    pub async fn run(mut self) {
        let election_timer = tokio::time::sleep(Self::random_election_timeout());
        tokio::pin!(election_timer);
        let mut heartbeat_timer = interval(Duration::from_millis(HEARTBEAT_INTERVAL));
        heartbeat_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        info!("RaftNode {} started as Follower", self.state.my_id);

        loop {
            tokio::select! {
                // 1. Handle Pesan Masuk
                Some(msg) = self.inbox.recv() => {
                    self.handle_message(msg, &mut election_timer).await;
                }

                // 2. Election Timeout (Jika Follower/Candidate)
                _ = &mut election_timer => {
                    if self.state.role != Role::Leader {
                        warn!("Election timeout reached! Starting election...");
                        self.start_election().await;

                        election_timer.as_mut().reset(Instant::now() + Self::random_election_timeout());
                    }
                }

                // 3. Heartbeat (Jika Leader)
                _ = heartbeat_timer.tick() => {
                    if self.state.role == Role::Leader {
                        self.send_heartbeats().await;
                    }
                }
            }
        }
    }

    /* Message Handlers */
    async fn handle_message(
        &mut self,
        msg: ActorMsg,
        election_timer: &mut std::pin::Pin<&mut Sleep>,
    ) {
        match msg {
            ActorMsg::ClientRequest { cmd, reply_to } => {
                self.handle_client_request(cmd, reply_to).await;
            }
            ActorMsg::RequestLog { reply_to } => {
                let _ = reply_to.send(self.state.log.clone());
            }
            ActorMsg::ApplyMembership {
                node_id,
                node_addr,
                reply_to,
            } => {
                self.handle_apply_membership(node_id, node_addr, reply_to)
                    .await;
            }
            ActorMsg::RemoveMembership { node_id, reply_to } => {
                self.handle_remove_membership(node_id, reply_to).await;
            }
            ActorMsg::UpdatePeerClient { node_id, client } => {
                self.peers.insert(node_id, client);
                info!("Successfully connected to peer {}", node_id);
            }
            ActorMsg::PeerDisconnected { peer_id } => {
                if self.peers.remove(&peer_id).is_some() {
                    warn!("Removed dead client for peer {}", peer_id);
                }
            }
            ActorMsg::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
                reply_to,
            } => {
                self.handle_request_vote(
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                    reply_to,
                    election_timer,
                )
                .await;
            }
            ActorMsg::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                reply_to,
            } => {
                self.handle_append_entries(
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                    reply_to,
                    election_timer,
                )
                .await;
            }
            ActorMsg::AppendEntriesResult {
                peer_id,
                term,
                success,
                last_log_index,
            } => {
                // Result belongs to a former leader/candidate period.
                if self.state.role != Role::Leader || term != self.state.current_term {
                    return;
                }
                if term > self.state.current_term {
                    self.state.become_follower(term);
                    return;
                }

                if success {
                    self.state.update_match_index(peer_id, last_log_index);
                    self.state.update_next_index(peer_id, last_log_index + 1);
                    if let Some(new_commit) = self.state.advance_commit_index() {
                        self.apply_committed_entries().await;
                        info!("Commit index advanced to {}", new_commit);
                    }
                } else {
                    let current_next = *self.state.next_index.get(&peer_id).unwrap_or(&1);
                    let new_next = if current_next > 1 {
                        current_next - 1
                    } else {
                        1
                    };
                    self.state.update_next_index(peer_id, new_next);
                }
            }
            ActorMsg::VoteResult {
                peer_id,
                election_term,
                reply,
            } => {
                self.handle_vote_result(peer_id, election_term, reply).await;
            }
            ActorMsg::InstallSnapshot {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                data,
                done,
                reply_to,
            } => {
                self.handle_install_snapshot(
                    term,
                    leader_id,
                    last_included_index,
                    last_included_term,
                    data,
                    done,
                    reply_to,
                    election_timer,
                )
                .await;
            }
            ActorMsg::InstallSnapshotReply { .. } => {
                // Currently handled via direct async call in send_heartbeats logic or could be expanded.
            }
            ActorMsg::TriggerSnapshot => {
                self.handle_trigger_snapshot().await;
            }
        }
    }

    async fn handle_trigger_snapshot(&mut self) {
        let last_applied = self.state.last_applied;

        // Cannot snapshot if applied index hasn't moved past existing snapshot
        if last_applied <= self.state.last_included_index {
            return;
        }

        info!("Triggering snapshot at index {}", last_applied);

        // 1. Capture State Machine
        let snapshot_data = self.state_machine.data.clone();

        // 2. Get last included term
        let last_term = self.state.get_log_term(last_applied);

        let snapshot = Snapshot {
            last_included_index: last_applied,
            last_included_term: last_term,
            data: snapshot_data,
        };

        // 3. Save to disk
        let node_id = self.state.my_id;
        let save_result = spawn_blocking(move || {
            let path = format!("snapshot_{}.json", node_id);
            let tmp_path = format!("{}.tmp", path);
            let json = serde_json::to_string_pretty(&snapshot).map_err(|e| e.to_string())?;
            std::fs::write(&tmp_path, json).map_err(|e| e.to_string())?;
            std::fs::rename(&tmp_path, &path).map_err(|e| e.to_string())?;
            Ok::<(), String>(())
        })
        .await;

        if let Ok(Ok(())) = save_result {
            info!("Snapshot saved successfully.");

            // 4. Truncate Log in Memory
            // We need to keep the entry at last_applied as the new dummy entry (index 0)
            // Calculate how many to remove from front.
            // Current log: [last_included_index, ..., last_applied, ...]
            // New log: [last_applied, ...]

            let remove_count = (last_applied - self.state.last_included_index) as usize;

            // Safety check
            if remove_count < self.state.log.len() {
                self.state.log.drain(0..remove_count);
                // The new first element is now at `last_applied`
                self.state.log[0].term = last_term;
                self.state.log[0].index = last_applied;
                self.state.log[0].command = Command::Ping; // Dummy command

                self.state.last_included_index = last_applied;
                self.state.last_included_term = last_term;

                if let Err(error) = self.persist_state().await {
                    error!("snapshot hard-state persistence failed: {error}");
                } else {
                    info!(
                        "Log truncated. New start index: {}",
                        self.state.last_included_index
                    );
                }
            } else {
                error!(
                    "Log truncation error: remove_count {} >= log.len {}",
                    remove_count,
                    self.state.log.len()
                );
            }
        } else {
            error!("Failed to save snapshot to disk.");
        }
    }

    async fn handle_install_snapshot(
        &mut self,
        term: u64,
        leader_id: u64,
        last_included_index: u64,
        last_included_term: u64,
        data: Vec<u8>,
        _done: bool, // Assuming full snapshot for now
        reply_to: oneshot::Sender<InstallSnapshotReply>,
        election_timer: &mut std::pin::Pin<&mut Sleep>,
    ) {
        if term < self.state.current_term {
            let _ = reply_to.send(InstallSnapshotReply {
                term: self.state.current_term,
                success: false,
            });
            return;
        }

        if term > self.state.current_term || self.state.role != Role::Leader {
            self.state.become_follower(term);
        }

        self.state.current_leader = Some(leader_id);
        self.update_readiness();
        election_timer
            .as_mut()
            .reset(Instant::now() + Self::random_election_timeout());

        // Decode Snapshot
        if let Ok(snapshot) = serde_json::from_slice::<Snapshot>(&data) {
            info!("Installing snapshot up to index {}", last_included_index);
            let snapshot_path = format!("snapshot_{}.json", self.state.my_id);
            let temp_path = format!("{}.tmp", snapshot_path);
            if let Err(error) = std::fs::write(&temp_path, &data)
                .and_then(|_| std::fs::rename(&temp_path, &snapshot_path))
            {
                error!("refusing installed snapshot because it cannot be saved: {error}");
                let _ = reply_to.send(InstallSnapshotReply {
                    term: self.state.current_term,
                    success: false,
                });
                return;
            }

            // Update State Machine
            self.state_machine.data = snapshot.data;

            // Update Log (Truncate)
            // Discard entire log and reset from snapshot

            self.state.last_included_index = last_included_index;
            self.state.last_included_term = last_included_term;
            self.state.commit_index = last_included_index;
            self.state.last_applied = last_included_index;

            // Create a new dummy entry at the snapshot index
            let dummy_entry = LogEntry {
                term: last_included_term,
                index: last_included_index,
                command: Command::Ping, // Placeholder
            };
            self.state.log = vec![dummy_entry];

            match self.persist_state().await {
                Ok(()) => {
                    let _ = reply_to.send(InstallSnapshotReply {
                        term: self.state.current_term,
                        success: true,
                    });
                }
                Err(error) => {
                    error!("refusing installed snapshot success: {error}");
                    let _ = reply_to.send(InstallSnapshotReply {
                        term: self.state.current_term,
                        success: false,
                    });
                }
            }
        } else {
            warn!("Failed to deserialize snapshot data");
            let _ = reply_to.send(InstallSnapshotReply {
                term: self.state.current_term,
                success: false,
            });
        }
    }
    async fn handle_request_vote(
        &mut self,
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
        reply_to: oneshot::Sender<RequestVoteReply>,
        election_timer: &mut std::pin::Pin<&mut Sleep>,
    ) {
        if term > self.state.current_term {
            info!(
                "Received RequestVote with higher term ({}), updating term.",
                term
            );
            self.state.become_follower(term);
        }

        let mut vote_granted = false;

        if term >= self.state.current_term {
            let can_vote =
                self.state.voted_for.is_none() || self.state.voted_for == Some(candidate_id);
            let is_log_ok = self.state.is_log_up_to_date(last_log_index, last_log_term);

            if can_vote && is_log_ok {
                vote_granted = true;
                self.state.voted_for = Some(candidate_id);
                election_timer
                    .as_mut()
                    .reset(Instant::now() + Self::random_election_timeout());
                info!(
                    "Vote GRANTED for candidate {} at term {}",
                    candidate_id, term
                );
            } else {
                debug!(
                    "Vote DENIED for candidate {}. Reason: voted_for={:?}, log_ok={}",
                    candidate_id, self.state.voted_for, is_log_ok
                );
            }
        } else {
            debug!(
                "Vote DENIED for candidate {}. Reason: Term too old ({} < {})",
                candidate_id, term, self.state.current_term
            );
        }

        if let Err(error) = self.persist_state().await {
            error!("refusing vote because persistence failed: {error}");
            vote_granted = false;
        }
        let _ = reply_to.send(RequestVoteReply {
            term: self.state.current_term,
            vote_granted,
        });
    }

    async fn handle_append_entries(
        &mut self,
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
        reply_to: oneshot::Sender<AppendEntriesReply>,
        election_timer: &mut std::pin::Pin<&mut Sleep>,
    ) {
        if term < self.state.current_term {
            debug!(
                "Rejecting AppendEntries from {} (Term {} < {})",
                leader_id, term, self.state.current_term
            );
            let _ = reply_to.send(AppendEntriesReply {
                term: self.state.current_term,
                success: false,
            });
            return;
        }

        if term > self.state.current_term || self.state.role != Role::Follower {
            info!(
                "Recognized valid Leader {} at term {}. Becoming Follower.",
                leader_id, term
            );
            self.state.become_follower(term);
        }

        self.state.current_leader = Some(leader_id);
        self.update_readiness();
        election_timer
            .as_mut()
            .reset(Instant::now() + Self::random_election_timeout());

        let success = self
            .state
            .append_entries(prev_log_index, prev_log_term, entries);
        if success {
            if let Err(error) = self.persist_state().await {
                error!("refusing AppendEntries success because persistence failed: {error}");
                let _ = reply_to.send(AppendEntriesReply {
                    term: self.state.current_term,
                    success: false,
                });
                return;
            }
        }

        if success {
            if leader_commit > self.state.commit_index {
                let last_new_entry_index = self.state.last_log_index();
                self.state.commit_index = min(leader_commit, last_new_entry_index);
                self.apply_committed_entries().await;
                debug!("Commit index updated to {}", self.state.commit_index);
            }
        }
        let _ = reply_to.send(AppendEntriesReply {
            term: self.state.current_term,
            success,
        });
    }

    async fn apply_committed_entries(&mut self) {
        while self.state.last_applied < self.state.commit_index {
            self.state.last_applied += 1;
            let idx = self.state.last_applied;

            // Adjust index for log access
            let log_len = self.state.log.len();
            // Virtual index -> Physical index
            if idx < self.state.last_included_index {
                // Already snapshot/compacted? Should not happen if last_applied is correct
                continue;
            }
            let physical_idx = (idx - self.state.last_included_index) as usize;

            if physical_idx < log_len {
                let entry = &self.state.log[physical_idx];
                let result = match &entry.command {
                    Command::AddNode { id, address } => {
                        if *id != self.state.my_id {
                            self.state.peers.insert(*id, address.clone());
                        }
                        if let Some(reply) = self.pending_membership.remove(&idx) {
                            let mut peers = self.state.peers.clone();
                            peers.insert(self.state.my_id, self.state.my_addr.clone());
                            let _ = reply.send(Ok(ApplyMembershipResponse { peers }));
                        }
                        "OK".into()
                    }
                    Command::RemoveNode { id } => {
                        self.state.peers.remove(id);
                        self.peers.remove(id);
                        self.state.next_index.remove(id);
                        self.state.match_index.remove(id);
                        if let Some(reply) = self.pending_removals.remove(&idx) {
                            let _ = reply.send(Ok(()));
                        }
                        "OK".into()
                    }
                    command => self.state_machine.apply(command),
                };
                info!(
                    "Applied log index {}: {:?} -> {}",
                    idx, entry.command, result
                );

                if let Some(sender) = self.pending_requests.remove(&idx) {
                    let _ = sender.send(Ok(result));
                }
            } else {
                warn!("Log missing at index {}", idx);
            }
        }

        // Trigger snapshot if log is too big
        if self.state.log.len() as u64 > RAFT_LOG_SIZE_LIMIT {
            let _ = self.msg_sender.send(ActorMsg::TriggerSnapshot).await;
        }
    }

    /* Internal Logic */
    async fn start_election(&mut self) {
        self.state.become_candidate();
        self.state.current_leader = None;
        self.update_readiness();
        self.fail_pending(NodeError::NotLeader { leader_addr: None });
        if let Err(error) = self.persist_state().await {
            error!("cannot start election without persisting term: {error}");
            self.state.role = Role::Follower;
            return;
        }

        if let Err(error) = self.persist_state().await {
            error!("failed to persist committed index: {error}");
        }

        let term = self.state.current_term;
        let my_id = self.state.my_id;
        let last_log_index = self.state.last_log_index();
        let last_log_term = self.state.last_log_term();
        info!("Election started for Term {}", term);

        for (peer_id, client) in &self.peers {
            let client = client.clone();
            let sender = self.msg_sender.clone();
            let peer_id = *peer_id;

            tokio::spawn(async move {
                let mut context = tarpc::context::current();
                context.deadline = std::time::Instant::now() + Duration::from_millis(1000);
                let reply = client
                    .request_vote(context, term, my_id, last_log_index, last_log_term)
                    .await;

                if let Ok(response) = reply {
                    let _ = sender
                        .send(ActorMsg::VoteResult {
                            peer_id,
                            election_term: term,
                            reply: response,
                        })
                        .await;
                } else {
                    warn!("Peer {} failed to vote", peer_id);
                }
            });
        }
        self.election_term = Some(term);
        self.election_votes = 1;
        if self.election_votes >= self.majority() {
            self.become_leader().await;
        }
    }

    fn majority(&self) -> u64 {
        (self.state.peers.len() as u64 + 1) / 2 + 1
    }

    async fn handle_vote_result(
        &mut self,
        peer_id: u64,
        election_term: u64,
        reply: RequestVoteReply,
    ) {
        if self.state.role != Role::Candidate || self.election_term != Some(election_term) {
            return;
        }
        if reply.term > self.state.current_term {
            self.state.become_follower(reply.term);
            self.state.current_leader = None;
            self.update_readiness();
            self.election_term = None;
            if let Err(error) = self.persist_state().await {
                error!("failed to persist newer term: {error}");
            }
        } else if reply.vote_granted {
            self.election_votes += 1;
            info!(
                "Vote received from {}. Total: {}",
                peer_id, self.election_votes
            );
            if self.election_votes >= self.majority() {
                self.become_leader().await;
            }
        }
    }

    async fn become_leader(&mut self) {
        self.state.become_leader();
        self.update_readiness();
        self.election_term = None;
        info!("Became leader for term {}", self.state.current_term);
        self.send_heartbeats().await;
    }

    fn fail_pending(&mut self, error: NodeError) {
        for (_, sender) in std::mem::take(&mut self.pending_requests) {
            let _ = sender.send(Err(error.clone()));
        }
        for (_, sender) in std::mem::take(&mut self.pending_membership) {
            let _ = sender.send(Err(error.clone()));
        }
        for (_, sender) in std::mem::take(&mut self.pending_removals) {
            let _ = sender.send(Err(error.clone()));
        }
    }

    async fn send_heartbeats(&mut self) {
        debug!("Sending heartbeats...");
        let current_peers = self.state.peers.clone();

        for (peer_id, peer_addr_str) in current_peers {
            if peer_id == self.state.my_id {
                continue;
            }
            let existing_client = self.peers.get(&peer_id).cloned();
            let term = self.state.current_term;
            let my_id = self.state.my_id;
            let leader_commit = self.state.commit_index;
            let next_index = *self.state.next_index.get(&peer_id).unwrap_or(&1);

            // Check if we need to send a snapshot
            if next_index <= self.state.last_included_index {
                // Send Snapshot
                let last_included_index = self.state.last_included_index;
                let last_included_term = self.state.last_included_term;
                let sender = self.msg_sender.clone();
                let snapshot_path = format!("snapshot_{}.json", my_id);

                tokio::spawn(async move {
                    let client = match existing_client {
                        Some(c) => c,
                        None => match tokio::time::timeout(
                            Duration::from_millis(HEARTBEAT_INTERVAL),
                            crate::utils::client::connect(&peer_addr_str),
                        )
                        .await
                        {
                            Ok(Ok(new_client)) => {
                                let _ = sender
                                    .send(ActorMsg::UpdatePeerClient {
                                        node_id: peer_id,
                                        client: new_client.clone(),
                                    })
                                    .await;
                                new_client
                            }
                            _ => return,
                        },
                    };

                    // Read snapshot from disk
                    let data = match tokio::fs::read(&snapshot_path).await {
                        Ok(d) => d,
                        Err(e) => {
                            error!("Failed to read snapshot file {}: {}", snapshot_path, e);
                            return;
                        }
                    };

                    let mut context = tarpc::context::current();
                    context.deadline = std::time::Instant::now() + Duration::from_millis(2000); // Longer timeout for snapshot

                    match client
                        .install_snapshot(
                            context,
                            term,
                            my_id,
                            last_included_index,
                            last_included_term,
                            data,
                            true,
                        )
                        .await
                    {
                        Ok(reply) => {
                            // Treat success like AppendEntries success: update indices
                            // We re-use AppendEntriesResult for simplicity to update state
                            let _ = sender
                                .send(ActorMsg::AppendEntriesResult {
                                    peer_id,
                                    term: reply.term,
                                    success: reply.success,
                                    last_log_index: last_included_index,
                                })
                                .await;
                        }
                        Err(e) => {
                            warn!("InstallSnapshot RPC failed for peer {}: {}", peer_id, e);
                            let _ = sender.send(ActorMsg::PeerDisconnected { peer_id }).await;
                        }
                    }
                });
            } else {
                // Send AppendEntries (Existing Logic)
                let prev_log_index = next_index - 1;
                // Use virtual indexing helper
                let prev_log_term = self.state.get_log_term(prev_log_index);

                // Get entries: adjust for virtual indexing
                let entries = if next_index <= self.state.last_log_index() {
                    // Calculate start index in the physical log vector
                    let start_physical_idx = (next_index - self.state.last_included_index) as usize;
                    if start_physical_idx < self.state.log.len() {
                        self.state.log[start_physical_idx..].to_vec()
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                };

                let sender = self.msg_sender.clone();
                let last_idx_sent = prev_log_index + entries.len() as u64;

                tokio::spawn(async move {
                    let client = match existing_client {
                        Some(c) => c,
                        None => {
                            info!("[Heartbeat] Peer {} disconnected, attempting to reconnect to {}...", peer_id, peer_addr_str);
                            {
                                match tokio::time::timeout(
                                    Duration::from_millis(HEARTBEAT_INTERVAL),
                                    crate::utils::client::connect(&peer_addr_str),
                                )
                                .await
                                {
                                    Ok(Ok(new_client)) => {
                                        info!(
                                            "[Heartbeat] Successfully reconnected to peer {}",
                                            peer_id
                                        );
                                        let _ = sender
                                            .send(ActorMsg::UpdatePeerClient {
                                                node_id: peer_id,
                                                client: new_client.clone(),
                                            })
                                            .await;
                                        new_client
                                    }
                                    _ => {
                                        debug!(
                                            "[Heartbeat] Failed to reconnect to peer {}",
                                            peer_id
                                        );
                                        return;
                                    }
                                }
                            }
                        }
                    };

                    let mut context = tarpc::context::current();
                    context.deadline = std::time::Instant::now() + Duration::from_millis(1000);

                    match client
                        .append_entries(
                            context,
                            term,
                            my_id,
                            prev_log_index,
                            prev_log_term,
                            entries,
                            leader_commit,
                        )
                        .await
                    {
                        Ok(resp) => {
                            let _ = sender
                                .send(ActorMsg::AppendEntriesResult {
                                    peer_id,
                                    term: resp.term,
                                    success: resp.success,
                                    last_log_index: last_idx_sent,
                                })
                                .await;
                        }
                        Err(_) => {
                            warn!(
                                "[Heartbeat] RPC failed for peer {}. Marking disconnected.",
                                peer_id
                            );
                            let _ = sender.send(ActorMsg::PeerDisconnected { peer_id }).await;
                        }
                    }
                });
            }
        }
    }

    async fn handle_client_request(
        &mut self,
        cmd: Command,
        reply_to: oneshot::Sender<Result<String, NodeError>>,
    ) {
        if self.state.role != Role::Leader {
            let leader_id = self.state.current_leader;
            let leader_addr = leader_id.and_then(|id| self.state.peers.get(&id).cloned());
            let _ = reply_to.send(Err(NodeError::NotLeader { leader_addr }));
            return;
        }

        let new_index = self.state.last_log_index() + 1;
        let term = self.state.current_term;
        let entry = LogEntry {
            term,
            index: new_index,
            command: cmd,
        };
        self.state.log.push(entry);
        self.pending_requests.insert(new_index, reply_to);

        info!("Leader appended command to log index {}", new_index);
        if let Err(error) = self.persist_state().await {
            let _ = self
                .pending_requests
                .remove(&new_index)
                .map(|sender| sender.send(Err(error)));
            return;
        }
        self.send_heartbeats().await;

        // Try to advance commit index immediately (important for single-node clusters)
        if let Some(new_commit) = self.state.advance_commit_index() {
            self.apply_committed_entries().await;
            info!("Commit index advanced to {}", new_commit);
        }
    }

    async fn handle_apply_membership(
        &mut self,
        node_id: u64,
        node_addr: String,
        reply_to: oneshot::Sender<Result<ApplyMembershipResponse, NodeError>>,
    ) {
        if self.state.role != Role::Leader {
            let leader_id = self.state.current_leader;
            let leader_addr = leader_id.and_then(|id| self.state.peers.get(&id).cloned());
            let _ = reply_to.send(Err(NodeError::NotLeader { leader_addr }));
            return;
        }

        // When sending back the peer list, create a temporary map that includes the leader itself.
        let mut peers_with_leader = self.state.peers.clone();
        peers_with_leader.insert(self.state.my_id, self.state.my_addr.clone());

        if self.state.peers.contains_key(&node_id) || self.state.my_id == node_id {
            info!("Node {} is already a member of the cluster.", node_id);
            let response = ApplyMembershipResponse {
                peers: peers_with_leader,
            };
            let _ = reply_to.send(Ok(response));
            return;
        }

        info!("Adding new node {} at {} to cluster", node_id, node_addr);
        let new_index = self.state.last_log_index() + 1;
        let entry = LogEntry {
            term: self.state.current_term,
            index: new_index,
            command: Command::AddNode {
                id: node_id,
                address: node_addr.clone(),
            },
        };
        self.state.log.push(entry);
        if let Err(error) = self.persist_state().await {
            let _ = reply_to.send(Err(error));
            return;
        }
        // The new node participates in replication before the entry is
        // committed, but membership becomes authoritative only when applied.
        self.state.peers.insert(node_id, node_addr);
        self.state.next_index.insert(node_id, new_index);
        self.state.match_index.insert(node_id, 0);
        self.pending_membership.insert(new_index, reply_to);
        self.send_heartbeats().await;
    }

    async fn handle_remove_membership(
        &mut self,
        node_id: u64,
        reply_to: oneshot::Sender<Result<(), NodeError>>,
    ) {
        if self.state.role != Role::Leader {
            let leader_id = self.state.current_leader;
            let leader_addr = leader_id.and_then(|id| self.state.peers.get(&id).cloned());
            let _ = reply_to.send(Err(NodeError::NotLeader { leader_addr }));
            return;
        }

        // A leader does not keep itself in `peers`, but a terminating leader
        // still needs to replicate its own RemoveNode entry before exiting.
        if node_id != self.state.my_id && !self.state.peers.contains_key(&node_id) {
            let _ = reply_to.send(Err(NodeError::Internal("Node not found".into())));
            return;
        }

        info!("Removing node {} from cluster", node_id);
        let new_index = self.state.last_log_index() + 1;
        let entry = LogEntry {
            term: self.state.current_term,
            index: new_index,
            command: Command::RemoveNode { id: node_id },
        };
        self.state.log.push(entry);
        if let Err(error) = self.persist_state().await {
            let _ = reply_to.send(Err(error));
            return;
        }
        self.pending_removals.insert(new_index, reply_to);
        self.send_heartbeats().await;
    }

    pub async fn bootstrap(&mut self, contact_node_address: String) -> anyhow::Result<()> {
        const MAX_RETRIES: u32 = 3;

        let my_id = self.state.my_id;
        let my_addr_str = self.state.my_addr.clone();

        let rpc_call = |client: RaftServiceClient| {
            let my_addr_str = my_addr_str.clone();
            async move {
                client
                    .apply_membership(context::current(), my_id, my_addr_str)
                    .await
            }
        };

        match execute_with_redirect(contact_node_address, MAX_RETRIES, rpc_call).await {
            Ok(join_response) => {
                info!("Successfully joined the cluster. Updating peer list.");
                self.state.peers = join_response.peers;
                self.state.peers.remove(&self.state.my_id);
                self.persist_state().await?;
                info!("Updated peer list from leader: {:?}", self.state.peers);
                Ok(())
            }
            Err(e) => {
                error!("Failed to bootstrap after multiple retries: {}", e);
                Err(e)
            }
        }
    }
}
