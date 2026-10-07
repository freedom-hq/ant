//! `/v0/cache` (Ant extension, #149): see, clear and resize the chunk
//! cache over HTTP, and the writes' local-host guard.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use ant_gateway::CorsConfig;
use ant_retrieval::{
    ChunkCaches, DiskChunkCache, InMemoryChunkCache, CACHE_CAPACITY_MAX_BYTES,
    CACHE_CAPACITY_MIN_BYTES,
};
use axum::body::Body;
use axum::extract::connect_info::MockConnectInfo;
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use common::{body_bytes, cache_router, send};
use serde_json::Value;

const LOCAL: &str = "127.0.0.1:50000";

/// A disk cache in a fresh temp dir, with `unpinned` cached chunks and
/// `pinned` chunks pinned under one root.
async fn disk_cache(
    name: &str,
    unpinned: u32,
    pinned: u32,
) -> (Arc<DiskChunkCache>, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "ant-gateway-cache-{name}-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let disk =
        Arc::new(DiskChunkCache::open(dir.join("chunks.sqlite"), 256 * 1024 * 1024).unwrap());
    disk.put_batch(chunks(1, unpinned)).await.unwrap();
    let pins = chunks(2, pinned);
    if let Some((root, _)) = pins.first() {
        disk.pin_collection(root.to_vec(), pins.clone())
            .await
            .unwrap();
    }
    (disk, dir)
}

/// `n` distinct 4104-byte (span + 4 KiB) rows; the disk cache trusts
/// stored bytes, so they needn't be valid CACs.
fn chunks(tag: u8, n: u32) -> Vec<([u8; 32], Vec<u8>)> {
    (0..n)
        .map(|i| {
            let mut addr = [tag; 32];
            addr[..4].copy_from_slice(&i.to_le_bytes());
            let mut wire = vec![tag; 4104];
            wire[..4].copy_from_slice(&i.to_le_bytes());
            (addr, wire)
        })
        .collect()
}

fn from(router: Router, peer: &str) -> Router {
    router.layer(MockConnectInfo(peer.parse::<SocketAddr>().unwrap()))
}

async fn call(
    router: Router,
    method: Method,
    uri: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
) -> (StatusCode, Value) {
    let mut b = Request::builder().method(method).uri(uri);
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    let req = match body {
        Some(json) => b
            .header("content-type", "application/json")
            .body(Body::from(json.to_string())),
        None => b.body(Body::empty()),
    }
    .unwrap();
    let resp = send(router, req).await;
    let status = resp.status();
    let bytes = body_bytes(resp).await;
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn status_clear_and_capacity_over_http() {
    let (disk, dir) = disk_cache("api", 300, 5).await;
    let memory = Arc::new(InMemoryChunkCache::new(64));
    for (a, w) in chunks(1, 10) {
        memory.put(a, w);
    }
    let caches = ChunkCaches {
        disk: Some(disk.clone()),
        memory,
    };
    let router = from(cache_router(Some(caches), CorsConfig::default()), LOCAL);

    let (code, before) = call(router.clone(), Method::GET, "/v0/cache", &[], None).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(before["disk_enabled"], true);
    assert_eq!(before["chunks"], 300);
    assert_eq!(before["pinned_chunks"], 5);
    assert_eq!(before["memory_chunks"], 10);
    assert_eq!(before["capacity_bytes"], 256 * 1024 * 1024);

    let (code, cleared) = call(router.clone(), Method::POST, "/v0/cache/clear", &[], None).await;
    assert_eq!(code, StatusCode::OK, "{cleared}");
    assert_eq!(cleared["removed_chunks"], 300);
    assert_eq!(cleared["freed_bytes"], 300 * 4104);
    assert_eq!(cleared["memory_chunks_removed"], 10);
    assert_eq!(cleared["status"]["chunks"], 0);
    assert_eq!(cleared["status"]["pinned_chunks"], 5, "pins kept");
    assert_eq!(cleared["status"]["memory_chunks"], 0);
    assert!(disk.get(chunks(2, 1)[0].0).await.unwrap().is_some());

    let (code, s) = call(
        router.clone(),
        Method::PUT,
        "/v0/cache/capacity",
        &[],
        Some(r#"{"bytes":1}"#),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{s}");
    assert_eq!(s["capacity_bytes"], CACHE_CAPACITY_MIN_BYTES, "clamped up");
    let (_, s) = call(
        router.clone(),
        Method::PUT,
        "/v0/cache/capacity",
        &[],
        Some(&format!(r#"{{"bytes":{}}}"#, u64::MAX)),
    )
    .await;
    assert_eq!(
        s["capacity_bytes"], CACHE_CAPACITY_MAX_BYTES,
        "clamped down"
    );
    assert_eq!(disk.capacity_bytes(), CACHE_CAPACITY_MAX_BYTES);

    let (code, _) = call(
        router,
        Method::PUT,
        "/v0/cache/capacity",
        &[],
        Some(r#"{"chunks":1}"#),
    )
    .await;
    assert!(code.is_client_error(), "unknown key rejected: {code}");

    let _ = std::fs::remove_dir_all(&dir);
}

/// The writes answer only loopback, non-browser callers; reads are open.
#[tokio::test]
async fn writes_refuse_remote_peers_and_web_pages() {
    let (disk, dir) = disk_cache("guard", 20, 0).await;
    let caches = ChunkCaches {
        disk: Some(disk.clone()),
        memory: Arc::new(InMemoryChunkCache::new(8)),
    };
    let cors = CorsConfig::new(["null", "https://app.example"].iter());
    let router = cache_router(Some(caches), cors);
    let cap = Some(r#"{"bytes":134217728}"#);

    for peer in ["192.168.1.20:4000", "[2001:db8::1]:4000"] {
        let r = from(router.clone(), peer);
        let (code, _) = call(r.clone(), Method::POST, "/v0/cache/clear", &[], None).await;
        assert_eq!(code, StatusCode::FORBIDDEN, "clear from {peer}");
        let (code, _) = call(r.clone(), Method::PUT, "/v0/cache/capacity", &[], cap).await;
        assert_eq!(code, StatusCode::FORBIDDEN, "capacity from {peer}");
        let (code, _) = call(r, Method::GET, "/v0/cache", &[], None).await;
        assert_eq!(code, StatusCode::OK, "status from {peer}");
    }
    // No connect info at all: refused.
    let (code, _) = call(router.clone(), Method::POST, "/v0/cache/clear", &[], None).await;
    assert_eq!(code, StatusCode::FORBIDDEN);

    let local = from(router.clone(), LOCAL);
    for headers in [
        &[("origin", "null")][..],
        &[("origin", "https://evil.example")][..],
        &[("sec-fetch-site", "cross-site")][..],
    ] {
        let (code, _) = call(
            local.clone(),
            Method::POST,
            "/v0/cache/clear",
            headers,
            None,
        )
        .await;
        assert_eq!(code, StatusCode::FORBIDDEN, "clear with {headers:?}");
    }
    assert_eq!(disk.used_rows(), 20, "nothing cleared yet");

    // IPv6 and v4-mapped loopback, and an origin listed exactly, pass.
    let (code, _) = call(
        from(router.clone(), "[::ffff:127.0.0.1]:5000"),
        Method::PUT,
        "/v0/cache/capacity",
        &[],
        cap,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    let (code, _) = call(
        from(router, "[::1]:5000"),
        Method::POST,
        "/v0/cache/clear",
        &[("origin", "https://app.example")],
        None,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(disk.used_rows(), 0);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn no_disk_cache_and_no_cache_controls() {
    let memory = Arc::new(InMemoryChunkCache::new(8));
    memory.put([1; 32], vec![1; 8]);
    let router = from(
        cache_router(
            Some(ChunkCaches { disk: None, memory }),
            CorsConfig::default(),
        ),
        LOCAL,
    );
    let (code, cleared) = call(router.clone(), Method::POST, "/v0/cache/clear", &[], None).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(cleared["memory_chunks_removed"], 1);
    assert_eq!(cleared["status"]["disk_enabled"], false);
    let (code, err) = call(
        router,
        Method::PUT,
        "/v0/cache/capacity",
        &[],
        Some(r#"{"bytes":134217728}"#),
    )
    .await;
    assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        err["message"].as_str().unwrap().contains("not available"),
        "{err}"
    );

    let none = from(cache_router(None, CorsConfig::default()), LOCAL);
    let (code, _) = call(none, Method::GET, "/v0/cache", &[], None).await;
    assert_eq!(code, StatusCode::NOT_IMPLEMENTED);
}
