use axum::{
    extract::{Path, State},
    response::{Html, Json},
    routing::{get, post},
    Router,
};
use raft_core::{domain::Command, rpc::RaftServiceClient, utils::client};
use serde::{Deserialize, Serialize};
use tarpc::context;

#[derive(Clone)]
struct AppState {
    raft_addr: String,
}

#[derive(Deserialize)]
struct SetRequest {
    key: String,
    value: String,
}

#[derive(Serialize)]
struct ApiResponse {
    success: bool,
    data: Option<String>,
    error: Option<String>,
}

#[tokio::main]
async fn main() {
    let raft_addr = std::env::var("RAFT_ADDR").unwrap_or_else(|_| "127.0.0.1:8000".to_string());
    println!("API Gateway starting. Raft contact address: {}", raft_addr);

    let state = AppState { raft_addr };

    let app = Router::new()
        .route("/", get(serve_ui))
        .route("/api/get/:key", get(handle_get))
        .route("/api/set", post(handle_set))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    println!("Web Server listening on 0.0.0.0:8080");
    axum::serve(listener, app).await.unwrap();
}

async fn serve_ui() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

async fn handle_get(Path(key): Path<String>, State(state): State<AppState>) -> Json<ApiResponse> {
    let rpc_call = |client: RaftServiceClient| {
        let key_clone = key.clone();
        async move {
            client
                .execute(context::current(), Command::Get { key: key_clone })
                .await
        }
    };

    let initial_addr = state.raft_addr.clone();

    match client::execute_with_redirect(initial_addr, 3, rpc_call).await {
        Ok(response) => Json(ApiResponse {
            success: true,
            data: Some(response),
            error: None,
        }),
        Err(e) => Json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
        }),
    }
}

async fn handle_set(
    State(state): State<AppState>,
    axum::extract::Json(payload): axum::extract::Json<SetRequest>,
) -> Json<ApiResponse> {
    let rpc_call = |client: RaftServiceClient| {
        let key = payload.key.clone();
        let value = payload.value.clone();
        async move {
            client
                .execute(context::current(), Command::Set { key, value })
                .await
        }
    };

    let initial_addr = state.raft_addr.clone();

    match client::execute_with_redirect(initial_addr, 3, rpc_call).await {
        Ok(response) => Json(ApiResponse {
            success: true,
            data: Some(response),
            error: None,
        }),
        Err(e) => Json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
        }),
    }
}
