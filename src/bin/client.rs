use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use tarpc::context;
use raft_core::{
    utils::client,
    domain::Command,
    rpc::RaftServiceClient,
    error::NodeError, 
};
use rustyline::{error::ReadlineError, DefaultEditor};

// Struktur untuk parsing argumen CLI
#[derive(Parser)]
struct Cli {
    // Alamat IP server (default: 127.0.0.1)
    #[arg(long, default_value = "127.0.0.1")]
    ip: String,

    // Port server (default: 8001)
    #[arg(long, default_value_t = 8001)]
    port: u16,

    // Subcommand yang akan dieksekusi
    #[command(subcommand)]
    command: Action,
}

// Daftar subcommand yang didukung client
#[derive(Subcommand)]
enum Action {
    Ping,
    Get { key: String },
    Set { key: String, value: String },
    Append { key: String, value: String },
    Del { key: String },
    Strlen { key: String },
    Log,
    RemoveNode { node_id: u64 },
    /// Start interactive mode (shell-like)
    Interactive,
}

// Entry point client
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let initial_addr: SocketAddr = format!("{}:{}", cli.ip, cli.port).parse()?;
    const MAX_RETRIES: u32 = 3;

    match &cli.command {
        Action::Log => {
            // Log doesn't need redirection, as any node can provide its log.
            println!("Connecting to server on {}...", initial_addr);
            let transport = tarpc::serde_transport::tcp::connect(initial_addr, tarpc::tokio_serde::formats::Json::default).await?;
            let client = RaftServiceClient::new(tarpc::client::Config::default(), transport).spawn();
            let logs = client.request_log(context::current()).await?;
            println!("--- LOG SERVER ---");
            for entry in logs {
                println!("Term: {}, Index: {}, Cmd: {:?}", entry.term, entry.index, entry.command);
            }
        }
        Action::RemoveNode { node_id } => {
            println!("Attempting to remove node {} from the cluster...", node_id);
            let rpc_call = |client: RaftServiceClient| async move {
                client.remove_membership(context::current(), *node_id).await
            };
            
            match client::execute_with_redirect(initial_addr, MAX_RETRIES, rpc_call).await {
                Ok(_) => println!("Successfully requested removal of node {}.", node_id),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        Action::Interactive => {
            println!("Starting interactive client connected to {}. Type 'help' for commands, 'exit' to quit.", initial_addr);
            let mut current_server_addr = initial_addr;
            let mut rl = DefaultEditor::new()?;

            loop {
                let readline = rl.readline("raft-cli> ");
                match readline {
                    Ok(line) => {
                        let input = line.trim();
                        if input.is_empty() {
                            continue;
                        }
                        
                        let _ = rl.add_history_entry(input);

                        match input {
                            "exit" | "quit" => {
                                println!("Exiting interactive mode.");
                                break;
                            }
                            "help" => {
                                println!("Available commands:");
                                println!("  ping");
                                println!("  get <key>");
                                println!("  set <key> <value>");
                                println!("  append <key> <value>");
                                println!("  del <key>");
                                println!("  strlen <key>");
                                println!("  log");
                                println!("  removenode <node_id>");
                                println!("  exit / quit");
                                continue;
                            }
                            _ => {}
                        }

                        let parts: Vec<&str> = input.split_whitespace().collect();
                        if parts.is_empty() {
                            continue;
                        }

                        let cmd_str = parts[0];
                        let mut cmd_payload: Option<Command> = None;
                        let mut _is_special_cmd = false; // For log and removenode

                        match cmd_str.to_lowercase().as_str() {
                            "ping" => cmd_payload = Some(Command::Ping),
                            "get" => {
                                if parts.len() == 2 {
                                    cmd_payload = Some(Command::Get { key: parts[1].to_string() });
                                } else {
                                    eprintln!("Usage: get <key>");
                                }
                            }
                            "set" => {
                                if parts.len() >= 3 {
                                    let key = parts[1].to_string();
                                    let value = parts[2..].join(" ");
                                    cmd_payload = Some(Command::Set { key, value });
                                } else {
                                    eprintln!("Usage: set <key> <value>");
                                }
                            }
                            "append" => {
                                if parts.len() >= 3 {
                                    let key = parts[1].to_string();
                                    let value = parts[2..].join(" ");
                                    cmd_payload = Some(Command::Append { key, value });
                                } else {
                                    eprintln!("Usage: append <key> <value>");
                                }
                            }
                            "del" => {
                                if parts.len() == 2 {
                                    cmd_payload = Some(Command::Del { key: parts[1].to_string() });
                                } else {
                                    eprintln!("Usage: del <key>");
                                }
                            }
                            "strlen" => {
                                if parts.len() == 2 {
                                    cmd_payload = Some(Command::Strlen { key: parts[1].to_string() });
                                } else {
                                    eprintln!("Usage: strlen <key>");
                                }
                            }
                            "log" => {
                                _is_special_cmd = true;
                                println!("Connecting to server on {}...", current_server_addr);
                                let transport_result = tarpc::serde_transport::tcp::connect(current_server_addr, tarpc::tokio_serde::formats::Json::default).await;
                                match transport_result {
                                    Ok(transport) => {
                                        let client = RaftServiceClient::new(tarpc::client::Config::default(), transport).spawn();
                                        match client.request_log(context::current()).await {
                                            Ok(logs) => {
                                                println!("--- LOG SERVER ---");
                                                for entry in logs {
                                                    println!("Term: {}, Index: {}, Cmd: {:?}", entry.term, entry.index, entry.command);
                                                }
                                            },
                                            Err(e) => eprintln!("Error requesting logs: {}", e),
                                        }
                                    },
                                    Err(e) => eprintln!("Failed to connect: {}", e),
                                }
                            }
                            "removenode" => {
                                _is_special_cmd = true;
                                if parts.len() == 2 {
                                    if let Ok(node_id) = parts[1].parse::<u64>() {
                                        println!("Attempting to remove node {} from the cluster...", node_id);
                                        let rpc_call = |client: RaftServiceClient| async move {
                                            client.remove_membership(context::current(), node_id).await
                                        };
                                        
                                        match client::execute_with_redirect(current_server_addr, MAX_RETRIES, rpc_call).await {
                                            Ok(_) => println!("Successfully requested removal of node {}.", node_id),
                                            Err(e) => eprintln!("Error: {}", e),
                                        }
                                    } else {
                                        eprintln!("Usage: removenode <node_id> (node_id must be a number)");
                                    }
                                } else {
                                    eprintln!("Usage: removenode <node_id>");
                                }
                            }
                            _ => eprintln!("Unknown command: {}", cmd_str),
                        }

                        if let Some(payload) = cmd_payload {
                            let rpc_call = |client: RaftServiceClient| {
                                let payload_clone = payload.clone();
                                async move {
                                    client.execute(context::current(), payload_clone).await
                                }
                            };

                            match client::execute_with_redirect(current_server_addr, MAX_RETRIES, rpc_call).await {
                                Ok(response) => println!("Response: {}", response),
                                Err(e) => {
                                    eprintln!("Error: {}", e);
                                    // Handle redirection manually for interactive mode cache update
                                    if let Some(node_error) = e.downcast_ref::<NodeError>() {
                                        if let NodeError::NotLeader { leader_addr } = node_error {
                                            if let Some(addr_str) = leader_addr {
                                                if let Ok(new_addr) = addr_str.parse::<SocketAddr>() {
                                                    current_server_addr = new_addr;
                                                    println!("Redirected to leader at {}. Retrying next command there.", current_server_addr);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    Err(ReadlineError::Interrupted) => {
                        println!("CTRL-C");
                        break;
                    },
                    Err(ReadlineError::Eof) => {
                        println!("CTRL-D");
                        break;
                    },
                    Err(err) => {
                        println!("Error: {:?}", err);
                        break;
                    }
                }
            }
        }
        _ => {
            // All other commands use the standard `execute` RPC.
            let cmd_payload = match &cli.command {
                Action::Ping => Command::Ping,
                Action::Set { key, value } => Command::Set { key: key.clone(), value: value.clone() },
                Action::Get { key } => Command::Get { key: key.clone() },
                Action::Append { key, value } => Command::Append { key: key.clone(), value: value.clone() },
                Action::Del { key } => Command::Del { key: key.clone() },
                Action::Strlen { key } => Command::Strlen { key: key.clone() },
                _ => unreachable!(), // Log, RemoveNode, and Interactive are handled above
            };

            println!("Executing command: {:?}", cmd_payload);
            let rpc_call = |client: RaftServiceClient| {
                let cmd_payload = cmd_payload.clone();
                async move {
                    client.execute(context::current(), cmd_payload).await
                }
            };

            match client::execute_with_redirect(initial_addr, MAX_RETRIES, rpc_call).await {
                Ok(response) => println!("Response: {}", response),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
    }

    Ok(())
}

