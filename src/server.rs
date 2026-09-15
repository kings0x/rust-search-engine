use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderValue, StatusCode, header},
    response::{Html, IntoResponse},
    routing::get,
};
use search_engine::search::{IndexStats, SearchIndex, SearchResponse};
use serde::Deserialize;
use std::sync::Arc;

const INDEX_HTML: &str = include_str!("web/index.html");

#[derive(Clone)]
struct AppState {
    index: Arc<SearchIndex>,
}

#[derive(Debug, Deserialize)]
struct SearchParams {
    q: Option<String>,
    limit: Option<usize>,
}

pub async fn serve(index: SearchIndex, bind_address: &str) -> Result<()> {
    let state = AppState {
        index: Arc::new(index),
    };
    let app = Router::new()
        .route("/", get(index_page))
        .route("/api/search", get(search))
        .route("/api/stats", get(stats))
        .route("/health", get(health))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(bind_address)
        .await
        .with_context(|| format!("failed to bind search server to {bind_address}"))?;

    axum::serve(listener, app)
        .await
        .context("search server stopped unexpectedly")
}

async fn index_page() -> impl IntoResponse {
    let mut response = Html(INDEX_HTML).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, max-age=0"),
    );
    response
}

async fn search(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Json<SearchResponse>, (StatusCode, &'static str)> {
    let query = params.q.unwrap_or_default();
    if query.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "query parameter `q` is required"));
    }

    Ok(Json(
        state.index.search(query.trim(), params.limit.unwrap_or(10)),
    ))
}

async fn stats(State(state): State<AppState>) -> Json<IndexStats> {
    Json(state.index.stats())
}

async fn health() -> &'static str {
    "ok"
}
