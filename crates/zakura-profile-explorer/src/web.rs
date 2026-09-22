//! Localhost HTTP reads with bounded concurrency and SQLite deadlines.

use crate::store::Reader;
use anyhow::Result;
use axum::{
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Semaphore;

#[derive(Clone)]
struct App {
    path: PathBuf,
    permits: Arc<Semaphore>,
}
type ApiResult = std::result::Result<Json<Value>, (StatusCode, String)>;

impl App {
    async fn read(&self, f: impl FnOnce(Reader) -> Result<Value> + Send + 'static) -> ApiResult {
        let permit = self.permits.clone().try_acquire_owned().map_err(|_| {
            (
                StatusCode::TOO_MANY_REQUESTS,
                "Explorer busy. Retry shortly.".into(),
            )
        })?;
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f(Reader::open(&path)?)
        })
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Read worker failed".into(),
            )
        })?
        .map(Json)
        .map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("Profile unavailable: {e}"),
            )
        })
    }
}

#[derive(Deserialize)]
struct Home {
    run: Option<String>,
    mode: Option<String>,
}
#[derive(Deserialize)]
struct Search {
    q: String,
}
async fn home(State(app): State<App>, Query(q): Query<Home>) -> ApiResult {
    app.read(move |r| r.home(q.run.as_deref(), q.mode.as_deref().unwrap_or("semantic")))
        .await
}
async fn detail(State(app): State<App>, Path((run, attempt)): Path<(String, u64)>) -> ApiResult {
    app.read(move |r| r.detail(&run, attempt)).await
}
async fn search(State(app): State<App>, Query(q): Query<Search>) -> ApiResult {
    app.read(move |r| r.search(&q.q)).await
}
async fn trace(State(app): State<App>, Path((run, attempt)): Path<(String, u64)>) -> ApiResult {
    app.read(move |r| {
        let detail = r.detail(&run,attempt)?;
        let mut events = Vec::new();
        let summary = &detail["summary"];
        if let (Some(start),Some(end)) = (summary["start_us"].as_u64(),summary["end_us"].as_u64()) {
            events.push(json!({"name":"Verifier request","cat":"elapsed","ph":"X","pid":1,"tid":0,"ts":start,"dur":end.saturating_sub(start),"args":{"outcome":summary["outcome"],"boundary":detail["boundary"]}}));
        }
        for span in detail["spans"].as_array().into_iter().flatten() {
            let start = span["start_us"].as_u64().unwrap_or(0);
            let end = span["end_us"].as_u64().unwrap_or(start);
            // Logical tracks, not OS threads. Async elapsed time is not CPU ownership.
            events.push(json!({"name":span["stage"],"cat":"elapsed","ph":"X","pid":1,"tid":span["span"],"ts":start,"dur":end.saturating_sub(start),"args":{"parent_span":span["parent"],"completion_thread":span["completion_thread"]}}));
        }
        Ok(json!({"traceEvents":events,"displayTimeUnit":"ms","metadata":{"complete":detail["complete"],"exclusion_reason":summary["exclusion_reason"],"timing":detail["timing"],"cpu":"unavailable","run":run,"attempt":attempt}}))
    }).await
}

pub(crate) async fn serve(path: PathBuf, port: u16) -> Result<()> {
    let app = App {
        path,
        permits: Arc::new(Semaphore::new(2)),
    };
    let router = Router::new()
        .route("/",get(|| async { Html(include_str!("../web/index.html")) }))
        .route("/block",get(|| async { Html(include_str!("../web/block.html")) }))
        .route("/block/{run}/{attempt}",get(|| async { Html(include_str!("../web/block.html")) }))
        .route("/app.js",get(|| async { ([(header::CONTENT_TYPE,"text/javascript")],include_str!("../web/app.js")) }))
        .route("/style.css",get(|| async { ([(header::CONTENT_TYPE,"text/css")],include_str!("../web/style.css")) }))
        .route("/api/home",get(home)).route("/api/search",get(search))
        .route("/api/attempt/{run}/{attempt}",get(detail))
        .route("/api/trace/{run}/{attempt}",get(trace))
        .with_state(app)
        .layer(axum::middleware::from_fn(|request: axum::extract::Request, next: axum::middleware::Next| async move {
            // Restrict browser access to the forwarded localhost origin. No public controls.
            let allowed = request.headers().get(header::HOST).and_then(|h|h.to_str().ok()).is_some_and(|h| h.starts_with("127.0.0.1:") || h.starts_with("localhost:"));
            if !allowed { return StatusCode::FORBIDDEN.into_response(); }
            let mut response: Response = next.run(request).await;
            for (key,value) in [ ("content-security-policy","default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"), ("x-content-type-options","nosniff"), ("cache-control","no-store") ] {
                response.headers_mut().insert(axum::http::HeaderName::from_static(key),axum::http::HeaderValue::from_static(value));
            }
            response
        }));
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
