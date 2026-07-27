use raft_core::raft::state::RaftState;
use raft_core::domain::{Command, LogEntry};
use std::collections::HashMap;
use std::fs;

// Helper to cleanup
fn cleanup(id: u64) {
    let _ = fs::remove_file(format!("node_{}.json", id));
}

fn create_test_state(id: u64) -> RaftState {
    cleanup(id);
    let peers = HashMap::new();
    let mut state = RaftState::new(id, peers);
    
    // Setup initial state:
    // last_included_index = 5, term = 1
    // log has entries 6, 7, 8 with term 2
    state.last_included_index = 5;
    state.last_included_term = 1;
    state.log = vec![
        LogEntry { term: 1, index: 5, command: Command::Ping }, // Dummy at snapshot index
        LogEntry { term: 2, index: 6, command: Command::Ping },
        LogEntry { term: 2, index: 7, command: Command::Ping },
        LogEntry { term: 2, index: 8, command: Command::Ping },
    ];
    state
}

#[test]
fn test_get_log_term() {
    let id = 100;
    let state = create_test_state(id);

    // Case 1: Index < Snapshot Index (Compacted)
    // Should return 0 (unknown)
    assert_eq!(state.get_log_term(4), 0);

    // Case 2: Index == Snapshot Index
    // Should return last_included_term (1)
    assert_eq!(state.get_log_term(5), 1);

    // Case 3: Index inside Log (In Memory)
    // Index 6 has term 2
    assert_eq!(state.get_log_term(6), 2);
    // Index 8 has term 2
    assert_eq!(state.get_log_term(8), 2);

    // Case 4: Index > Last Log Index (Future)
    // Should return 0
    assert_eq!(state.get_log_term(9), 0);

    cleanup(id);
}

#[test]
fn test_append_entries_conflict_and_overlap() {
    let id = 101;
    let mut state = create_test_state(id);

    // Initial State: [Snapshot: 5(t1)], 6(t2), 7(t2), 8(t2)

    // Scenario A: Stale Append (Old Index)
    // Leader tries to append starting at index 4 (which is compacted)
    let entries = vec![
        LogEntry { term: 1, index: 5, command: Command::Ping }
    ];
    // append_entries(prev_log_index=4, prev_log_term=1, ...)
    // Should fail because prev_log_index (4) < last_included_index (5)
    assert_eq!(state.append_entries(4, 1, entries.clone()), false);

    // Scenario B: Valid Append (Extension)
    // Append index 9(t2)
    let new_entries = vec![
        LogEntry { term: 2, index: 9, command: Command::Ping }
    ];
    // prev_log_index=8, term=2
    assert_eq!(state.append_entries(8, 2, new_entries), true);
    assert_eq!(state.last_log_index(), 9);
    assert_eq!(state.get_log_term(9), 2);

    // Scenario C: Conflict (Truncation)
    // Append index 7(t3) -> Should replace 7(t2), 8(t2), 9(t2)
    let conflict_entries = vec![
        LogEntry { term: 3, index: 7, command: Command::Ping },
        LogEntry { term: 3, index: 8, command: Command::Ping }
    ];
    // prev_log_index=6, term=2 (matches)
    assert_eq!(state.append_entries(6, 2, conflict_entries), true);
    
    // Verify log state
    assert_eq!(state.last_log_index(), 8);
    assert_eq!(state.get_log_term(7), 3);
    assert_eq!(state.get_log_term(8), 3);
    // Index 9 should be gone
    assert_eq!(state.log.len(), 4); // Dummy(5) + 6, 7, 8

    cleanup(id);
}

#[test]
fn test_virtual_indexing_helper() {
    let id = 102;
    let state = create_test_state(id);

    // Physical index 0 -> Logical index 5 (Snapshot)
    // Physical index 1 -> Logical index 6
    // Physical index 3 -> Logical index 8

    assert_eq!(state.last_log_index(), 8);
    
    // Check if `log` vector access logic is consistent manually
    assert_eq!(state.log[0].index, 5);
    assert_eq!(state.log[3].index, 8);

    cleanup(id);
}
