use crate::error::NodeError;
use crate::rpc::RaftServiceClient;
use tarpc::{client, tokio_serde::formats::Json};

/// Connect to a host:port string after resolving DNS.  `tarpc` only accepts a
/// concrete SocketAddr, while Kubernetes service names deliberately are not
/// concrete addresses.
pub async fn connect(addr: &str) -> anyhow::Result<RaftServiceClient> {
    let addresses: Vec<_> = tokio::net::lookup_host(addr).await?.collect();
    if addresses.is_empty() {
        anyhow::bail!("DNS returned no addresses for {addr}");
    }
    let mut last_error = None;
    for socket_addr in addresses {
        match tarpc::serde_transport::tcp::connect(socket_addr, Json::default).await {
            Ok(transport) => {
                return Ok(RaftServiceClient::new(client::Config::default(), transport).spawn())
            }
            Err(error) => last_error = Some(error),
        }
    }
    anyhow::bail!("could not connect to {addr}: {}", last_error.unwrap());
}

/// Executes a generic RPC call with a built-in redirection loop.
///
/// This helper function will attempt an RPC call on a given address. If the server
/// responds with a `NotLeader` error, it will automatically parse the new leader's
/// address and retry, up to a specified number of times.
pub async fn execute_with_redirect<F, T, Fut>(
    initial_addr: impl Into<String>,
    max_retries: u32,
    mut rpc_call: F,
) -> anyhow::Result<T>
where
    // The closure takes a client and returns a future.
    F: FnMut(RaftServiceClient) -> Fut,
    // The future resolves to the result of a tarpc RPC call.
    Fut: std::future::Future<Output = Result<Result<T, NodeError>, client::RpcError>>,
    T: Send + 'static,
{
    let initial_addr = initial_addr.into();
    let mut addr = initial_addr.clone();
    let mut retries = max_retries;

    loop {
        if retries == 0 {
            anyhow::bail!("Redirect limit reached.");
        }
        retries -= 1;

        println!("Connecting to server on {}...", addr);
        let client = match connect(&addr).await {
            Ok(client) => client,
            Err(error) => {
                // A headless Service can resolve to a pod that has just gone
                // away. Resolve it again on the next retry instead of making
                // the gateway depend on a particular ordinal.
                if addr != initial_addr {
                    addr = initial_addr.clone();
                }
                if retries == 0 {
                    return Err(error);
                }
                continue;
            }
        };

        match rpc_call(client).await {
            Ok(Ok(response)) => {
                // The RPC was successful and the server returned Ok(response).
                return Ok(response);
            }
            Ok(Err(NodeError::NotLeader { leader_addr })) => {
                // The server is a follower and told us who the leader is.
                if let Some(leader_str) = leader_addr {
                    println!("Not the leader. Redirecting to leader at {}...", leader_str);
                    addr = leader_str;
                    continue;
                } else {
                    // Re-resolve the Service; another endpoint may already
                    // know the leader while this follower is catching up.
                    addr = initial_addr.clone();
                    continue;
                }
            }
            Ok(Err(e)) => {
                // The RPC was successful, but the server returned a non-redirect error.
                anyhow::bail!("Application error from node: {}", e);
            }
            Err(e) => {
                // The RPC itself failed (network error, etc.).
                anyhow::bail!("RPC transport error: {}", e);
            }
        }
    }
}
