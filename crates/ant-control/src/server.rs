//! Tokio-based UDS server. Reads one NDJSON request, writes one or more
//! NDJSON responses, closes.
//!
//! The wire is single-shot for almost every request: one [`Request`] line
//! in, one [`Response`] line out, EOF. The exception is a `Get*` request
//! with `progress: true` — the daemon may emit zero or more
//! [`Response::Progress`] lines before the terminal response, all on the
//! same connection. Old clients that don't ask for progress only ever
//! see the terminal line, so the v1 single-shot framing is preserved.
//!
//! This module is `#[cfg(unix)]`-gated: it is the Unix domain-socket
//! transport. The command/ack data types it relays live in the
//! cross-platform `command` module so non-Unix targets (Windows) still
//! get the node-loop protocol types without the socket I/O.

use crate::command::{streaming_ack_channel, ControlAck, ControlCommand};
use crate::protocol::{Request, Response, StatusSnapshot, VersionInfo, PROTOCOL_VERSION};
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, warn};

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

const MAX_REQUEST_BYTES: usize = 64 * 1024;
/// How long a connected client has to send its one request line. Every
/// client writes the request right after connecting, so this only cuts
/// off a connection that is held open idle (issue #307), which would
/// otherwise pin a task and a file descriptor indefinitely.
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// How long to wait for the node loop to acknowledge a near-instant
/// command (e.g. `PeersReset`). If the node loop is wedged we don't want
/// to hang the socket handler indefinitely.
const FAST_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);
/// Cap for commands that may take a network round-trip (retrieval). Sized
/// to comfortably cover `ant_retrieval::retrieve_chunk`'s own 10 s
/// timeout plus per-attempt overhead, without hanging the socket forever
/// if the node loop wedges.
const NETWORK_COMMAND_TIMEOUT: Duration = Duration::from_secs(20);
/// Cap for commands that walk a chunk tree (joiner / mantaray): every
/// node and every leaf is one round-trip. A 1 MB file is ~256 leaves +
/// 2-3 intermediates; a manifest with one fork adds one more fetch on
/// top. Real-world chunk fetches usually complete in <1 s, but pad for
/// the unlucky path.
const TREE_COMMAND_TIMEOUT: Duration = Duration::from_mins(1);

/// Serve control requests on `socket_path` until the task is cancelled.
///
/// `status_rx` is the live status view maintained by the node loop; each
/// request call `.borrow()` on it and replies with the current snapshot.
///
/// `command_tx` is the single-producer channel into the node loop for
/// mutating requests (e.g. `Request::PeersReset`). `None` means mutating
/// commands will reply with `Response::Error`.
///
/// `agent` is the daemon agent string (e.g. `"antd/0.1.0"`), returned by
/// `Request::Version` and overriding `snapshot.agent` in case the sender
/// shipped an older value.
pub async fn serve(
    socket_path: PathBuf,
    agent: String,
    status_rx: watch::Receiver<StatusSnapshot>,
    command_tx: Option<mpsc::Sender<ControlCommand>>,
) -> Result<(), ServerError> {
    bind(socket_path)?.serve(agent, status_rx, command_tx).await
}

/// A control socket whose Unix listener is already bound. Split from
/// [`serve`] so the daemon can surface bind failures immediately (a
/// too-long socket path fails `bind(2)` with `SUN_LEN`, not the accept
/// loop) and decide what to do about them — e.g. fall back to a shorter
/// path or run without the socket — instead of discovering the error
/// seconds later when the spawned serve task is first joined, and
/// tearing down an otherwise healthy node over it (issue #39).
pub struct BoundControlSocket {
    listener: UnixListener,
    socket_path: PathBuf,
}

/// Remove a stale socket, create the parent directory, and bind with
/// owner-only permissions. Must be called from within a tokio runtime
/// (the listener registers with the reactor).
///
/// The socket is never reachable by another user, not even briefly:
/// `bind(2)` creates the socket file with `0777 & ~umask` (0755 under the
/// usual 022 umask), so binding straight at `socket_path` and `chmod`ing
/// afterwards left a window in which any local user could connect
/// (issue #307, CTL-01). Instead it is bound inside a fresh `0700`
/// staging directory next to `socket_path`, narrowed to `0600` there,
/// and only then `rename`d into place. The umask is never touched: it is
/// process-wide and other threads may be creating files.
///
/// An existing file at `socket_path` owned by another user is refused,
/// not removed: in a shared directory that is a squatter, and silently
/// binding elsewhere would hide it.
pub fn bind(socket_path: PathBuf) -> Result<BoundControlSocket, ServerError> {
    bind_owned_by(socket_path, effective_uid())
}

fn bind_owned_by(socket_path: PathBuf, uid: u32) -> Result<BoundControlSocket, ServerError> {
    let parent = match socket_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    std::fs::create_dir_all(&parent)?;
    remove_stale_socket(&socket_path, uid)?;
    let staging = StagingDir::create(&parent)?;
    let staged = staging.path.join("s");
    let listener = UnixListener::bind(&staged)?;
    restrict_socket_permissions(&staged)?;
    std::fs::rename(&staged, &socket_path)?;
    drop(staging);
    Ok(BoundControlSocket {
        listener,
        socket_path,
    })
}

/// Remove whatever an earlier run left at `path`, unless it belongs to
/// another user.
fn remove_stale_socket(path: &Path, uid: u32) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    match std::fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.uid() != uid {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!(
                        "refusing to replace {}: owned by uid {}, not {uid}",
                        path.display(),
                        meta.uid(),
                    ),
                ));
            }
            std::fs::remove_file(path)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// A freshly created, owner-only (`0700`) directory the socket is bound
/// in before being moved into place. Removed on drop, so a failed bind
/// leaves nothing behind. Named `.as<n>` with `n` counting up (at most
/// [`STAGING_ATTEMPTS`] tries) until `mkdir` succeeds, which claims the
/// name atomically. The staged path `<parent>/.as<n>/s` is never longer
/// than `<parent>/antd.sock`, so a data dir whose default socket fits
/// `sun_path` still fits while staged.
struct StagingDir {
    path: PathBuf,
}

const STAGING_ATTEMPTS: u32 = 64;

fn staging_name(n: u32) -> String {
    format!(".as{n}")
}

impl StagingDir {
    fn create(parent: &Path) -> std::io::Result<Self> {
        use std::os::unix::fs::DirBuilderExt;
        let mut last = None;
        for n in 0..STAGING_ATTEMPTS {
            let path = parent.join(staging_name(n));
            match std::fs::DirBuilder::new().mode(0o700).create(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => last = Some(e),
                Err(e) => return Err(e),
            }
        }
        Err(last.unwrap_or_else(|| std::io::Error::other("no free staging directory name")))
    }
}

impl Drop for StagingDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.path.join("s"));
        let _ = std::fs::remove_dir(&self.path);
    }
}

/// A private directory for the control socket when it cannot live in the
/// data dir (path too long for `sun_path`, issue #39): `$XDG_RUNTIME_DIR`
/// when it is set and private to this user, else `<temp>/antd-<uid>`,
/// created `0700`. Unlike a socket directly in the shared temp dir, the
/// path can't be pre-created or squatted by another local user: an
/// existing directory that isn't ours, isn't a real directory, or is
/// group/world-accessible is refused, never adopted (issue #307).
pub fn private_runtime_dir() -> std::io::Result<PathBuf> {
    let uid = effective_uid();
    if let Some(dir) = std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from) {
        if dir.is_absolute() && check_private_dir(&dir, uid).is_ok() {
            return Ok(dir);
        }
    }
    let dir = std::env::temp_dir().join(format!("antd-{uid}"));
    ensure_private_dir(&dir, uid)?;
    Ok(dir)
}

/// Create `dir` as `0700` if absent, then require it to be private to
/// `uid` (see [`check_private_dir`]).
fn ensure_private_dir(dir: &Path, uid: u32) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    match std::fs::DirBuilder::new().mode(0o700).create(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e),
    }
    check_private_dir(dir, uid)
}

/// `dir` must be a real directory (not a symlink), owned by `uid`, with
/// no group or world permission bits.
fn check_private_dir(dir: &Path, uid: u32) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::symlink_metadata(dir)?;
    let refuse = |why: String| {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("refusing socket directory {}: {why}", dir.display()),
        ))
    };
    if !meta.file_type().is_dir() {
        return refuse("not a directory".to_string());
    }
    if meta.uid() != uid {
        return refuse(format!("owned by uid {}, not {uid}", meta.uid()));
    }
    if meta.mode() & 0o077 != 0 {
        return refuse(format!(
            "mode {:o} is group/world-accessible",
            meta.mode() & 0o777
        ));
    }
    Ok(())
}

fn effective_uid() -> u32 {
    rustix::process::geteuid().as_raw()
}

/// Whether a connecting peer may use the socket: the daemon's own user,
/// or root (who can bypass the socket's `0600` mode anyway). The mode
/// already keeps everyone else out; this is a second check in case the
/// file is ever made wider or bound somewhere less private.
fn peer_allowed(peer_uid: u32, own_uid: u32) -> bool {
    peer_uid == own_uid || peer_uid == 0
}

impl BoundControlSocket {
    /// The path the listener is actually bound to.
    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Accept-and-dispatch loop; runs until the task is cancelled.
    /// Accept errors are logged and survived, so this only returns if
    /// the future is dropped.
    pub async fn serve(
        self,
        agent: String,
        status_rx: watch::Receiver<StatusSnapshot>,
        command_tx: Option<mpsc::Sender<ControlCommand>>,
    ) -> Result<(), ServerError> {
        debug!(
            target: "ant_control",
            "control socket listening at {}",
            self.socket_path.display(),
        );

        let own_uid = effective_uid();
        loop {
            match self.listener.accept().await {
                Ok((stream, _)) => {
                    match stream.peer_cred() {
                        Ok(cred) if peer_allowed(cred.uid(), own_uid) => {}
                        Ok(cred) => {
                            warn!(
                                target: "ant_control",
                                "refusing control connection from uid {}",
                                cred.uid(),
                            );
                            continue;
                        }
                        Err(e) => {
                            warn!(
                                target: "ant_control",
                                "refusing control connection: peer credentials unavailable: {e}",
                            );
                            continue;
                        }
                    }
                    let rx = status_rx.clone();
                    let agent = agent.clone();
                    let cmd_tx = command_tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, agent, rx, cmd_tx).await {
                            warn!(target: "ant_control", "control request failed: {e}");
                        }
                    });
                }
                Err(e) => {
                    warn!(target: "ant_control", "accept failed: {e}");
                }
            }
        }
    }
}

async fn handle_connection(
    stream: UnixStream,
    agent: String,
    status_rx: watch::Receiver<StatusSnapshot>,
    command_tx: Option<mpsc::Sender<ControlCommand>>,
) -> Result<(), ServerError> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half).take(MAX_REQUEST_BYTES as u64);
    let mut line = String::new();
    let Ok(read) = tokio::time::timeout(REQUEST_READ_TIMEOUT, reader.read_line(&mut line)).await
    else {
        debug!(target: "ant_control", "control client sent no request within {REQUEST_READ_TIMEOUT:?}; closing");
        return Ok(());
    };
    if read? == 0 {
        return Ok(());
    }
    let trimmed = line.trim_end_matches(['\r', '\n']);

    match serde_json::from_str::<Request>(trimmed) {
        Ok(Request::Status) => {
            let snap = status_rx.borrow().clone();
            write_response(&mut write_half, &Response::Status(Box::new(snap))).await?;
        }
        Ok(Request::Version) => {
            write_response(
                &mut write_half,
                &Response::Version(VersionInfo {
                    agent,
                    protocol_version: PROTOCOL_VERSION,
                }),
            )
            .await?;
        }
        Ok(Request::PeersReset) => {
            let response = dispatch_oneshot(command_tx.as_ref(), FAST_COMMAND_TIMEOUT, |ack| {
                ControlCommand::ResetPeerstore { ack }
            })
            .await;
            write_response(&mut write_half, &response).await?;
        }
        Ok(Request::Get {
            reference,
            bypass_cache: _,
            progress: _,
        }) => match parse_reference(&reference) {
            Ok(addr) => {
                let response =
                    dispatch_oneshot(command_tx.as_ref(), NETWORK_COMMAND_TIMEOUT, |ack| {
                        ControlCommand::GetChunk {
                            reference: addr,
                            ack,
                        }
                    })
                    .await;
                write_response(&mut write_half, &response).await?;
            }
            Err(e) => {
                write_response(
                    &mut write_half,
                    &Response::Error {
                        message: format!("bad reference: {e}"),
                    },
                )
                .await?;
            }
        },
        Ok(Request::GetBzz {
            reference,
            path,
            allow_degraded_redundancy,
            bypass_cache,
            progress,
        }) => match parse_reference(&reference) {
            Ok(addr) => {
                dispatch_streaming(
                    &mut write_half,
                    command_tx.as_ref(),
                    TREE_COMMAND_TIMEOUT,
                    |ack| ControlCommand::GetBzz {
                        reference: addr,
                        path,
                        allow_degraded_redundancy,
                        bypass_cache,
                        progress,
                        // CLI keeps the conservative joiner default; the
                        // HTTP gateway opts in to a higher ceiling for
                        // real-site downloads.
                        max_bytes: None,
                        ack,
                    },
                )
                .await?;
            }
            Err(e) => {
                write_response(
                    &mut write_half,
                    &Response::Error {
                        message: format!("bad reference: {e}"),
                    },
                )
                .await?;
            }
        },
        Ok(Request::GetBytes {
            reference,
            bypass_cache,
            progress,
        }) => match parse_reference(&reference) {
            Ok(addr) => {
                dispatch_streaming(
                    &mut write_half,
                    command_tx.as_ref(),
                    TREE_COMMAND_TIMEOUT,
                    |ack| ControlCommand::GetBytes {
                        reference: addr,
                        bypass_cache,
                        progress,
                        max_bytes: None,
                        ack,
                    },
                )
                .await?;
            }
            Err(e) => {
                write_response(
                    &mut write_half,
                    &Response::Error {
                        message: format!("bad reference: {e}"),
                    },
                )
                .await?;
            }
        },
        Ok(Request::UploadStart {
            source_path,
            batch_id,
            name,
            content_type,
            raw,
        }) => {
            let response = dispatch_oneshot(command_tx.as_ref(), FAST_COMMAND_TIMEOUT, |ack| {
                ControlCommand::UploadStart {
                    source_path: PathBuf::from(source_path),
                    batch_id,
                    name,
                    content_type,
                    raw,
                    ack,
                }
            })
            .await;
            write_response(&mut write_half, &response).await?;
        }
        Ok(Request::UploadList) => {
            let response = dispatch_oneshot(command_tx.as_ref(), FAST_COMMAND_TIMEOUT, |ack| {
                ControlCommand::UploadList { ack }
            })
            .await;
            write_response(&mut write_half, &response).await?;
        }
        Ok(Request::UploadStatus { job_id }) => {
            let response = dispatch_oneshot(command_tx.as_ref(), FAST_COMMAND_TIMEOUT, |ack| {
                ControlCommand::UploadStatus { job_id, ack }
            })
            .await;
            write_response(&mut write_half, &response).await?;
        }
        Ok(Request::UploadPause { job_id }) => {
            let response = dispatch_oneshot(command_tx.as_ref(), FAST_COMMAND_TIMEOUT, |ack| {
                ControlCommand::UploadPause { job_id, ack }
            })
            .await;
            write_response(&mut write_half, &response).await?;
        }
        Ok(Request::UploadResume { job_id }) => {
            let response = dispatch_oneshot(command_tx.as_ref(), FAST_COMMAND_TIMEOUT, |ack| {
                ControlCommand::UploadResume { job_id, ack }
            })
            .await;
            write_response(&mut write_half, &response).await?;
        }
        Ok(Request::UploadCancel { job_id }) => {
            let response = dispatch_oneshot(command_tx.as_ref(), FAST_COMMAND_TIMEOUT, |ack| {
                ControlCommand::UploadCancel { job_id, ack }
            })
            .await;
            write_response(&mut write_half, &response).await?;
        }
        Ok(Request::PostageStatus) => {
            let response = dispatch_oneshot(command_tx.as_ref(), FAST_COMMAND_TIMEOUT, |ack| {
                ControlCommand::PostageStatus { ack }
            })
            .await;
            write_response(&mut write_half, &response).await?;
        }
        Ok(Request::PullsyncProbe {
            target,
            bin,
            start,
            max_deliver,
        }) => match parse_reference(&target) {
            Ok(target) => {
                // The probe can long-block server-side on an empty live
                // bin; give it the network timeout, not the fast one.
                let response =
                    dispatch_oneshot(command_tx.as_ref(), NETWORK_COMMAND_TIMEOUT, |ack| {
                        ControlCommand::PullsyncProbe {
                            target,
                            bin,
                            start,
                            max_deliver: max_deliver.unwrap_or(16),
                            ack,
                        }
                    })
                    .await;
                write_response(&mut write_half, &response).await?;
            }
            Err(e) => {
                write_response(
                    &mut write_half,
                    &Response::Error {
                        message: format!("bad request: {e}"),
                    },
                )
                .await?;
            }
        },
        Ok(Request::PutChunkLocal { wire_hex }) => match parse_chunk_wire(&wire_hex) {
            Ok(wire) => {
                let response = dispatch_oneshot(command_tx.as_ref(), FAST_COMMAND_TIMEOUT, |ack| {
                    ControlCommand::PutChunkLocal { wire, ack }
                })
                .await;
                write_response(&mut write_half, &response).await?;
            }
            Err(e) => {
                write_response(
                    &mut write_half,
                    &Response::Error {
                        message: format!("bad request: {e}"),
                    },
                )
                .await?;
            }
        },
        Ok(Request::UploadFollow { job_id }) => {
            // Use the upload-specific timeout: a stalled upload may
            // emit no progress for many seconds (e.g. waiting on a
            // slow neighbour during pushsync), so reuse the
            // tree-command timeout that already covers the
            // analogous "no progress for a while" case on reads.
            dispatch_streaming(
                &mut write_half,
                command_tx.as_ref(),
                TREE_COMMAND_TIMEOUT,
                |ack| ControlCommand::UploadFollow { job_id, ack },
            )
            .await?;
        }
        Err(e) => {
            write_response(
                &mut write_half,
                &Response::Error {
                    message: format!("bad request: {e}"),
                },
            )
            .await?;
        }
    }

    write_half.flush().await?;
    write_half.shutdown().await?;
    Ok(())
}

async fn write_response<W>(write_half: &mut W, response: &Response) -> Result<(), ServerError>
where
    W: AsyncWriteExt + Unpin,
{
    let mut out = serde_json::to_vec(response).expect("serialize Response");
    out.push(b'\n');
    write_half.write_all(&out).await?;
    Ok(())
}

/// Forward a non-streaming command to the node loop and block until it
/// acks (or the timeout fires). Returns a `Response` ready to serialize
/// back to the client.
async fn dispatch_oneshot<F>(
    command_tx: Option<&mpsc::Sender<ControlCommand>>,
    timeout: Duration,
    build: F,
) -> Response
where
    F: FnOnce(oneshot::Sender<ControlAck>) -> ControlCommand,
{
    let Some(tx) = command_tx else {
        return Response::Error {
            message: "daemon has no control-command channel wired up".to_string(),
        };
    };
    let (ack_tx, ack_rx) = oneshot::channel();
    if tx.send(build(ack_tx)).await.is_err() {
        return Response::Error {
            message: "daemon node loop is no longer accepting commands".to_string(),
        };
    }
    match tokio::time::timeout(timeout, ack_rx).await {
        Ok(Ok(ack)) => ack_to_response(ack),
        Ok(Err(_)) => Response::Error {
            message: "daemon dropped the command without replying".to_string(),
        },
        Err(_) => Response::Error {
            message: format!("daemon did not ack within {} seconds", timeout.as_secs()),
        },
    }
}

/// Forward a streaming-capable command to the node loop, write each
/// non-terminal `Progress` ack as its own NDJSON line, and finish with
/// the terminal response. The `timeout` resets every time a fresh ack
/// lands so a long-but-progressing fetch isn't killed for taking longer
/// than the per-message bound.
async fn dispatch_streaming<W, F>(
    write_half: &mut W,
    command_tx: Option<&mpsc::Sender<ControlCommand>>,
    timeout: Duration,
    build: F,
) -> Result<(), ServerError>
where
    W: AsyncWriteExt + Unpin,
    F: FnOnce(mpsc::Sender<ControlAck>) -> ControlCommand,
{
    let Some(tx) = command_tx else {
        let response = Response::Error {
            message: "daemon has no control-command channel wired up".to_string(),
        };
        return write_response(write_half, &response).await;
    };
    let (ack_tx, mut ack_rx) = streaming_ack_channel();
    if tx.send(build(ack_tx)).await.is_err() {
        let response = Response::Error {
            message: "daemon node loop is no longer accepting commands".to_string(),
        };
        return write_response(write_half, &response).await;
    }
    loop {
        match tokio::time::timeout(timeout, ack_rx.recv()).await {
            Ok(Some(ack)) => {
                let is_terminal = ack.is_terminal();
                let response = ack_to_response(ack);
                write_response(write_half, &response).await?;
                if is_terminal {
                    return Ok(());
                }
            }
            Ok(None) => {
                let response = Response::Error {
                    message: "daemon dropped the command without replying".to_string(),
                };
                return write_response(write_half, &response).await;
            }
            Err(_) => {
                let response = Response::Error {
                    message: format!(
                        "daemon went silent for more than {} seconds",
                        timeout.as_secs()
                    ),
                };
                return write_response(write_half, &response).await;
            }
        }
    }
}

fn ack_to_response(ack: ControlAck) -> Response {
    match ack {
        ControlAck::Ok { message } => Response::Ok { message },
        ControlAck::Bytes { data } => Response::Bytes {
            hex: format!("0x{}", hex::encode(data)),
        },
        ControlAck::BzzBytes {
            data,
            content_type,
            filename,
        } => Response::BzzBytes {
            hex: format!("0x{}", hex::encode(data)),
            content_type,
            filename,
        },
        ControlAck::Manifest { .. } | ControlAck::ManifestPage(_) => Response::Error {
            message: "manifest listing is only supported by ant-gateway".to_string(),
        },
        ControlAck::Progress(p) => Response::Progress(p),
        ControlAck::BytesStreamStart { .. }
        | ControlAck::BzzStreamStart { .. }
        | ControlAck::BytesChunk { .. } => Response::Error {
            message: "streaming byte bodies are only supported by ant-gateway".to_string(),
        },
        ControlAck::StreamDone => Response::Ok {
            message: "stream complete".to_string(),
        },
        ControlAck::ChunkUploaded { reference } => Response::ChunkUploaded { reference },
        // Envelope issuance and stewardship checks are HTTP-gateway
        // surfaces (`POST /envelope`, `GET|PUT /stewardship`); the
        // Unix-socket protocol doesn't expose the commands, so an ack
        // leaking here means a wiring bug — surface it clearly.
        ControlAck::Envelope { .. } | ControlAck::Retrievable { .. } => Response::Error {
            message: "envelope/stewardship are only supported by ant-gateway".to_string(),
        },
        ControlAck::UploadStarted { job_id } => Response::UploadStarted { job_id },
        ControlAck::UploadJob(view) => Response::UploadJob(view),
        ControlAck::UploadList(views) => Response::UploadList { jobs: views },
        ControlAck::UploadProgress(view) => Response::UploadProgress(view),
        ControlAck::PostageStatus(view) => Response::PostageStatus(view),
        ControlAck::PullsyncProbe(view) => Response::PullsyncProbe(view),
        // Lurker message frames are a gateway WebSocket surface
        // (`/gsoc/subscribe`, `/pss/subscribe`); the Unix-socket protocol
        // doesn't expose the subscription command.
        ControlAck::LurkerMessage { .. } => Response::Error {
            message: "lurker subscriptions are only supported by ant-gateway".to_string(),
        },
        // The Unix-socket protocol returns a single batch via
        // `PostageStatus`; the multi-batch list is an HTTP-gateway-only
        // surface (`GET /stamps`).
        ControlAck::PostageList(_) => Response::Error {
            message: "postage batch listing is only supported by ant-gateway".to_string(),
        },
        // The settlement/balance snapshot backs the gateway's bee-parity
        // `/balances` / `/settlements` / `/chequebook/cheque` family;
        // the Unix-socket protocol doesn't expose the command.
        ControlAck::Accounting(_) => Response::Error {
            message: "accounting snapshots are only supported by ant-gateway".to_string(),
        },
        // The pin surface (bee `/pins`) and the per-bucket postage
        // counters are gateway HTTP endpoints; the Unix-socket protocol
        // doesn't expose the commands that produce these acks.
        ControlAck::PinAdded { .. }
        | ControlAck::PinRemoved { .. }
        | ControlAck::PinPresent { .. }
        | ControlAck::PinList { .. }
        | ControlAck::PinCheck { .. }
        | ControlAck::PostageBuckets(_)
        | ControlAck::StampedChunk { .. }
        | ControlAck::StewardshipStamps(_) => Response::Error {
            message:
                "pin / postage-bucket / stamp-record queries are only supported by ant-gateway"
                    .to_string(),
        },
        // The Unix-socket protocol used by `antctl` doesn't expose feed
        // reads today; the control surface is just for renderers and
        // ops tooling, and feed lookup lives on the gateway HTTP API.
        // If a future client wants to drive `GetFeed` over the control
        // socket we'll add `Response::FeedResolved` / `FeedNotFound`
        // variants; for now surface a clear error so the seam is visible.
        ControlAck::FeedResolved { .. } | ControlAck::FeedNotFound => Response::Error {
            message: "feed resolution is only supported by ant-gateway".to_string(),
        },
        // `NotReady` and `NotFound` exist for the gateway HTTP path so it
        // can return `503 Service Unavailable` / `404 Not Found`;
        // collapsing to a generic error here keeps the Unix-socket
        // surface unchanged.
        ControlAck::NotReady { message }
        | ControlAck::NotFound { message }
        | ControlAck::Error { message } => Response::Error { message },
    }
}

/// Parse a 32-byte chunk reference. Accepts `0x`-prefixed or bare hex.
/// Lower- or upper-case is fine. Anything else is rejected before the
/// command hits the node loop so the caller gets a tidy error.
fn parse_reference(s: &str) -> Result<[u8; 32], String> {
    let stripped = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    if stripped.len() != 64 {
        return Err(format!(
            "reference must be 32 bytes (64 hex chars); got {}",
            stripped.len()
        ));
    }
    let mut out = [0u8; 32];
    hex::decode_to_slice(stripped, &mut out).map_err(|e| format!("invalid hex: {e}"))?;
    Ok(out)
}

/// Parse the chunk wire bytes carried by `Request::PutChunkLocal`.
/// Accepts `0x`-prefixed or bare hex; even-length only. Enforces a
/// generous upper bound (~4200 bytes) so a hostile client can't OOM
/// the daemon by sending a giant "chunk"; tighter per-variant
/// validation (CAC vs SOC, BMT match) lives in the ant-p2p handler
/// where chunk crypto helpers are already in scope.
fn parse_chunk_wire(s: &str) -> Result<Vec<u8>, String> {
    let stripped = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    if !stripped.len().is_multiple_of(2) {
        return Err("chunk wire hex must have even length".to_string());
    }
    const MAX_WIRE_BYTES: usize = 4200;
    if stripped.len() / 2 > MAX_WIRE_BYTES {
        return Err(format!(
            "chunk wire {} bytes exceeds cap {} bytes",
            stripped.len() / 2,
            MAX_WIRE_BYTES
        ));
    }
    if stripped.len() < 16 {
        return Err("chunk wire must include the 8-byte span prefix".to_string());
    }
    hex::decode(stripped).map_err(|e| format!("invalid hex: {e}"))
}

fn restrict_socket_permissions(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;

    fn scratch_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("antctl-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn entries(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    fn status_rx() -> watch::Receiver<StatusSnapshot> {
        // `borrow()` keeps answering the last value after the sender
        // is gone, which is all these tests need.
        watch::channel(StatusSnapshot::default()).1
    }

    /// The socket must never be group- or world-connectable, not even in
    /// the moment between `bind(2)` and the permission change. The test
    /// opens the umask completely (so a plain bind would create the file
    /// `0777`) and has a second thread watch the final path the whole
    /// time: it must only ever see an owner-only socket there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn socket_is_never_group_or_world_connectable() {
        let dir = scratch_dir("mode");
        let path = dir.join("antd.sock");

        let stop = Arc::new(AtomicBool::new(false));
        let seen = Arc::new(AtomicU64::new(0));
        let watcher = {
            let path = path.clone();
            let stop = stop.clone();
            let seen = seen.clone();
            std::thread::spawn(move || {
                let mut loose = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    if let Ok(meta) = std::fs::symlink_metadata(&path) {
                        seen.fetch_add(1, Ordering::Relaxed);
                        if meta.mode() & 0o077 != 0 {
                            loose.push(meta.mode() & 0o777);
                        }
                    }
                }
                loose
            })
        };

        let old = rustix::process::umask(rustix::fs::Mode::empty());
        let mut result = Ok(());
        // At least 200 binds, and keep going (bounded) until the watcher
        // has actually sampled the path a fair number of times, so a slow
        // runner can't pass this by never looking.
        for i in 0..100_000u32 {
            if i >= 200 && seen.load(Ordering::Relaxed) >= 200 {
                break;
            }
            match bind(path.clone()) {
                Ok(bound) => drop(bound),
                Err(e) => {
                    result = Err(e);
                    break;
                }
            }
        }
        rustix::process::umask(old);
        stop.store(true, Ordering::Relaxed);
        let loose = watcher.join().unwrap();
        result.unwrap();
        assert!(
            seen.load(Ordering::Relaxed) > 0,
            "watcher never saw the socket"
        );
        assert!(
            loose.is_empty(),
            "socket seen {} time(s) with group/world bits, e.g. mode {:o}",
            loose.len(),
            loose.first().copied().unwrap_or_default(),
        );

        let bound = bind(path.clone()).unwrap();
        let meta = std::fs::symlink_metadata(&path).unwrap();
        assert!(meta.file_type().is_socket());
        assert_eq!(meta.mode() & 0o777, 0o600);
        // No staging directory is left behind next to the socket.
        assert_eq!(entries(&dir), vec!["antd.sock".to_string()]);

        // The owner can still connect and is served.
        let serve = tokio::spawn(bound.serve("antd/test".into(), status_rx(), None));
        let reply = tokio::task::spawn_blocking(move || {
            crate::client::request_sync(&path, &Request::Version)
        })
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(reply, Response::Version(v) if v.agent == "antd/test"));
        serve.abort();
        let _ = std::fs::remove_dir_all(&dir);
    }

    use std::os::unix::fs::FileTypeExt;

    /// Staging must not make a path that fits `sun_path` at its final
    /// name fail: `<parent>/.as<n>/s` is never longer than
    /// `<parent>/antd.sock`.
    #[test]
    fn staged_path_is_no_longer_than_the_default_socket_name() {
        for n in 0..STAGING_ATTEMPTS {
            let staged = format!("{}/s", staging_name(n));
            assert!(staged.len() <= "antd.sock".len(), "{staged}");
        }
    }

    /// The same, end to end: a data dir sized so `<dir>/antd.sock` is
    /// exactly the longest path Linux's `sun_path` (108 bytes incl. the
    /// NUL) accepts still binds there.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn socket_at_the_sun_path_limit_still_binds() {
        let base = scratch_dir("sunlen");
        let room = 107 - base.as_os_str().len() - "/antd.sock".len() - 1;
        let dir = base.join("d".repeat(room));
        let path = dir.join("antd.sock");
        assert_eq!(path.as_os_str().len(), 107);
        let bound = bind(path.clone()).unwrap();
        assert_eq!(bound.socket_path(), path);
        assert_eq!(entries(&dir), vec!["antd.sock".to_string()]);
        let _ = std::fs::remove_dir_all(&base);
    }

    /// A file already sitting at the socket path that belongs to another
    /// user (a squatter in a shared directory) is refused, not removed.
    #[tokio::test]
    async fn socket_path_owned_by_another_user_is_refused() {
        let dir = scratch_dir("squat");
        let path = dir.join("antd.sock");
        std::fs::write(&path, b"squatter").unwrap();
        let other = effective_uid().wrapping_add(1);

        let err = bind_owned_by(path.clone(), other)
            .err()
            .expect("must refuse");
        assert!(
            matches!(&err, ServerError::Io(e) if e.kind() == std::io::ErrorKind::PermissionDenied),
            "{err}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"squatter");
        assert_eq!(entries(&dir), vec!["antd.sock".to_string()]);

        // Our own stale file at the same path is replaced as before.
        let bound = bind(path.clone()).unwrap();
        assert!(std::fs::symlink_metadata(bound.socket_path())
            .unwrap()
            .file_type()
            .is_socket());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The fallback directory is refused when another user created it
    /// first, when it is group/world-accessible, or when it is a symlink.
    #[test]
    fn fallback_dir_pre_created_by_another_owner_is_refused() {
        let base = scratch_dir("fallback");
        let me = effective_uid();
        let other = me.wrapping_add(1);

        let dir = base.join("antd-1000");
        ensure_private_dir(&dir, me).unwrap();
        assert_eq!(std::fs::metadata(&dir).unwrap().mode() & 0o777, 0o700);
        // Existing and ours: adopted.
        ensure_private_dir(&dir, me).unwrap();

        // Existing but someone else's: refused.
        let err = ensure_private_dir(&dir, other).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied, "{err}");
        assert!(err.to_string().contains("owned by uid"), "{err}");

        // Ours but readable by others: refused.
        let open = base.join("open");
        std::fs::create_dir(&open).unwrap();
        std::fs::set_permissions(&open, std::fs::Permissions::from_mode(0o755)).unwrap();
        let err = ensure_private_dir(&open, me).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied, "{err}");

        // A symlink to a private dir: refused (it could be re-pointed).
        let link = base.join("link");
        std::os::unix::fs::symlink(&dir, &link).unwrap();
        let err = ensure_private_dir(&link, me).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied, "{err}");

        // A plain file in the way: refused.
        let file = base.join("file");
        std::fs::write(&file, b"").unwrap();
        assert!(ensure_private_dir(&file, me).is_err());

        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn private_runtime_dir_is_private() {
        let dir = private_runtime_dir().unwrap();
        let meta = std::fs::symlink_metadata(&dir).unwrap();
        assert!(meta.is_dir());
        assert_eq!(meta.uid(), effective_uid());
        assert_eq!(meta.mode() & 0o077, 0);
    }

    #[test]
    fn only_own_user_or_root_may_connect() {
        assert!(peer_allowed(1000, 1000));
        assert!(peer_allowed(0, 1000));
        assert!(!peer_allowed(1001, 1000));
    }

    /// A client that connects and never sends its request is cut off.
    #[tokio::test(start_paused = true)]
    async fn idle_connection_is_closed() {
        let (server, mut client) = UnixStream::pair().unwrap();
        let task = tokio::spawn(handle_connection(
            server,
            "antd/test".into(),
            status_rx(),
            None,
        ));
        tokio::time::sleep(REQUEST_READ_TIMEOUT + Duration::from_secs(1)).await;
        assert!(
            task.is_finished(),
            "handler still waiting after the idle timeout"
        );
        task.await.unwrap().unwrap();
        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, Vec::<u8>::new(), "no reply to an empty request");
    }
}
