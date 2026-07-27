use raft_core::raft::actor::{RaftActor, ActorMsg};
use raft_core::raft::state::RaftState;
use raft_core::domain::Command;
use raft_core::domain::Snapshot;
use tokio::sync::{mpsc, oneshot};
use std::collections::HashMap;
use std::time::Duration;
use std::fs;

// Helper to cleanup files
fn cleanup(id: u64) {
    let _ = fs::remove_file(format!("node_{}.json", id));
    let _ = fs::remove_file(format!("snapshot_{}.json", id));
}

#[tokio::test]
async fn test_snapshot_trigger_and_recovery() {
    let node_id = 99; // Use a unique ID for testing
    cleanup(node_id);

    // --- Part 1: Run Node and Trigger Snapshot ---
    {
        // Setup dependencies
        let (tx, rx) = mpsc::channel(100);
        // No peers needed for this test, as we are testing local log compaction logic
        // But we need to pretend we are leader to accept client requests
        let peers = HashMap::new(); 
        
        let mut state = RaftState::new(node_id, peers.clone());
        // Force become leader to accept commands
        state.become_leader();

        // Create actor
        let actor = RaftActor::new(state, rx, tx.clone(), HashMap::new());

        // Spawn actor in background
        let handle = tokio::spawn(async move {
            actor.run().await;
        });

        // Send enough commands to trigger snapshot (Limit is 10)
        // We send 15 commands.
        for i in 0..15 {
            let (reply_to, _rx_reply) = oneshot::channel();
            let cmd = Command::Set { 
                key: format!("k{}", i), 
                value: format!("v{}", i) 
            };
            
            let _ = tx.send(ActorMsg::ClientRequest { cmd, reply_to }).await;
            // Sleep a bit to allow actor to process and "commit" (in this single node test, it advances commit index immediately? 
            // Wait, single node cluster needs logic to self-commit.
            // RaftState::advance_commit_index logic: checks match_index of peers + self.
            // If peers is empty, majority is 1 (self). So it should commit.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Give some time for the snapshot task (spawn_blocking) to finish
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Abort actor to simulate stop
        handle.abort();
    }

    // --- Part 2: Verify Artifacts ---
    
    // Check if snapshot file exists
    let snapshot_path = format!("snapshot_{}.json", node_id);
    assert!(fs::metadata(&snapshot_path).is_ok(), "Snapshot file should exist");

    // Read snapshot content
    let content = fs::read_to_string(&snapshot_path).unwrap();
    let snapshot: Snapshot = serde_json::from_str(&content).unwrap();
    
    // Check if data is correct (k0 should be there as it was early in the log)
    assert_eq!(snapshot.data.get("k0"), Some(&"v0".to_string()));
    // Last included index should be close to 15 (depends on when exactly snapshot triggered)
    assert!(snapshot.last_included_index >= 10, "Snapshot should capture at least 10 entries");

    // --- Part 3: Recovery (Restart) ---
    {
        let peers = HashMap::new(); 
        let (tx, rx) = mpsc::channel(100);
        
        // Load state
        let mut state = RaftState::new(node_id, peers.clone());
        // Even if we are not leader, we should have the state from snapshot.
        // But ClientRequest only works if Leader.
        // Force leader again for testing query.
        state.become_leader(); 

        let actor = RaftActor::new(state, rx, tx.clone(), HashMap::new());
        
        let handle = tokio::spawn(async move {
            actor.run().await;
        });

        // Query k0 (should be in snapshot)
        let (reply_to, rx_reply) = oneshot::channel();
        let cmd = Command::Get { key: "k0".to_string() };
        let _ = tx.send(ActorMsg::ClientRequest { cmd, reply_to }).await;

        let result = rx_reply.await.expect("Actor should reply");
        assert_eq!(result.unwrap(), "v0", "Value k0 should be recovered from snapshot");

        // Query k14 (should also be there, recovered from Log)
        let (reply_to, rx_reply) = oneshot::channel();
        let cmd = Command::Get { key: "k14".to_string() };
        let _ = tx.send(ActorMsg::ClientRequest { cmd, reply_to }).await;

        let result = rx_reply.await.expect("Actor should reply");
        assert_eq!(result.unwrap(), "v14", "Value k14 should be recovered from Log");

        handle.abort();
    }

    cleanup(node_id);
}
