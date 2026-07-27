use raft_core::raft::machine::StateMachine;
use raft_core::domain::Command;

#[test]
fn test_basic_operations() {
    let mut sm = StateMachine::new();

    // Test Set
    let cmd_set = Command::Set { key: "x".into(), value: "100".into() };
    assert_eq!(sm.apply(&cmd_set), "OK");

    // Test Get
    let cmd_get = Command::Get { key: "x".into() };
    assert_eq!(sm.apply(&cmd_get), "100");
}

#[test]
fn test_transaction_bonus() {
    let mut sm = StateMachine::new();

    // Test Transaction
    let tx = Command::Transaction {
        commands: vec![
            Command::Set { key: "a".into(), value: "1".into() },
            Command::Set { key: "b".into(), value: "2".into() },
        ]
    };

    let res = sm.apply(&tx);
    assert!(res.contains("OK")); // Cek format return

    // Pastikan data masuk
    assert_eq!(sm.apply(&Command::Get { key: "a".into() }), "1");
    assert_eq!(sm.apply(&Command::Get { key: "b".into() }), "2");
}

#[test]
fn test_membership_changes() {
    let mut sm = StateMachine::new();

    // Test AddNode
    let cmd_add = Command::AddNode { id: 3, address: "127.0.0.1:8003".into() };
    assert_eq!(sm.apply(&cmd_add), "OK");

    // Test RemoveNode
    let cmd_remove = Command::RemoveNode { id: 3 };
    assert_eq!(sm.apply(&cmd_remove), "OK");
}
