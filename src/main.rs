//! Command-line entry point: dispatches to preparation, sending, receiving and maintenance.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use serde::Serialize;

use shardsync::api::{ReceiverOptions, run_receiver};
use shardsync::config::{ReceiverLimits, SenderSettings};
use shardsync::error::AppError;
use shardsync::sender::{SendOptions, remote_abort, remote_status, send};

#[derive(Parser)]
#[command(
    name = "shardsync",
    version,
    about = "Resumable, verified file and folder transfer"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Copy, flush, hash and freeze a quiescent file or folder into a new spool.
    Prepare {
        /// Regular file or directory to transfer.
        source: PathBuf,
        /// New directory to create for the spool.
        #[arg(long)]
        spool: PathBuf,
    },
    /// Upload a spool, resuming from whatever the receiver already holds.
    Send(SendArgs),
    /// Show the receiver's status of the session recorded in a spool.
    Status(ClientArgs),
    /// Abandon the session recorded in a spool (only while it is receiving).
    Abort(ClientArgs),
    /// Run the receiver.
    Receive(ReceiveArgs),
    /// Remove abandoned staging from a stopped receiver. Never touches completed output.
    Cleanup {
        #[arg(long)]
        root: PathBuf,
        /// Also remove staging directories that have no recorded session.
        #[arg(long)]
        remove_orphans: bool,
    },
    /// Write a development CA, receiver certificate and bearer tokens (not for real data).
    DevSetup {
        #[arg(long)]
        out: PathBuf,
        /// Client owner names; one token is written per owner.
        #[arg(long = "owner", default_value = "demo")]
        owners: Vec<String>,
    },
}

#[derive(Args)]
struct ClientArgs {
    #[arg(long)]
    spool: PathBuf,
    /// Receiver base URL, for example https://localhost:8443
    #[arg(long)]
    endpoint: String,
    /// CA certificate (PEM) that signed the receiver's certificate.
    #[arg(long)]
    ca: PathBuf,
    /// File containing this client's bearer token.
    #[arg(long)]
    token_file: PathBuf,
}

#[derive(Args)]
struct SendArgs {
    #[command(flatten)]
    client: ClientArgs,
    /// Concurrent chunk uploads.
    #[arg(long, default_value_t = 4)]
    workers: usize,
    /// Seconds one request keeps retrying transient failures before the send stops.
    #[arg(long, default_value_t = 120)]
    retry_window_secs: u64,
}

#[derive(Args)]
struct ReceiveArgs {
    #[arg(long)]
    root: PathBuf,
    #[arg(long, default_value = "127.0.0.1:8443")]
    listen: SocketAddr,
    #[arg(long)]
    tls_cert: PathBuf,
    #[arg(long)]
    tls_key: PathBuf,
    /// JSON file of client owners and token digests (mode 600).
    #[arg(long)]
    tokens: PathBuf,
    /// Run outside the supported Linux/ext4 durability profile (development only).
    #[arg(long)]
    allow_unvalidated_platform: bool,
    /// Global 4 MiB chunk-buffer permits.
    #[arg(long, default_value_t = 8)]
    chunk_permits: usize,
    /// Capacity of the queue in front of the storage worker.
    #[arg(long, default_value_t = 8)]
    storage_queue: usize,
    #[arg(long, default_value_t = 64)]
    max_connections: usize,
    /// Milliseconds a request waits for a buffer permit before receiving 429.
    #[arg(long, default_value_t = 2_000)]
    permit_wait_ms: u64,
    /// Milliseconds a request body may stall before the request fails.
    #[arg(long, default_value_t = 10_000)]
    body_idle_timeout_ms: u64,
    #[arg(long, default_value_t = 1_000)]
    missing_page_limit: usize,
}

fn client_options(args: ClientArgs, settings: SenderSettings) -> SendOptions {
    SendOptions {
        spool: args.spool,
        endpoint: args.endpoint,
        ca: args.ca,
        token_file: args.token_file,
        settings,
    }
}

fn print_json(value: &impl Serialize) -> Result<(), AppError> {
    let text =
        serde_json::to_string_pretty(value).map_err(|e| AppError::environment(e.to_string()))?;
    println!("{text}");
    Ok(())
}

async fn run(cli: Cli) -> Result<(), AppError> {
    match cli.command {
        Command::Prepare { source, spool } => {
            print_json(&shardsync::snapshot::prepare(&source, &spool)?)
        }
        Command::Send(args) => {
            let settings = SenderSettings {
                upload_workers: args.workers,
                retry_window: Duration::from_secs(args.retry_window_secs),
                ..SenderSettings::default()
            };
            print_json(&send(client_options(args.client, settings)).await?)
        }
        Command::Status(args) => {
            print_json(&remote_status(client_options(args, SenderSettings::default())).await?)
        }
        Command::Abort(args) => {
            print_json(&remote_abort(client_options(args, SenderSettings::default())).await?)
        }
        Command::Receive(args) => {
            let limits = ReceiverLimits {
                chunk_buffer_permits: args.chunk_permits,
                storage_queue_depth: args.storage_queue,
                max_connections: args.max_connections,
                permit_wait: Duration::from_millis(args.permit_wait_ms),
                body_idle_timeout: Duration::from_millis(args.body_idle_timeout_ms),
                missing_page_limit: args.missing_page_limit,
                ..ReceiverLimits::default()
            };
            run_receiver(ReceiverOptions {
                root: args.root,
                listen: args.listen,
                tls_cert: args.tls_cert,
                tls_key: args.tls_key,
                tokens_file: args.tokens,
                allow_unvalidated_platform: args.allow_unvalidated_platform,
                limits,
            })
            .await
        }
        Command::Cleanup {
            root,
            remove_orphans,
        } => print_json(&shardsync::cleanup::cleanup(&root, remove_orphans)?),
        Command::DevSetup { out, owners } => {
            shardsync::devcert::write_dev_setup(&out, &owners).map_err(AppError::environment)?;
            eprintln!(
                "wrote development certificates and tokens to {} (not for real data)",
                out.display()
            );
            Ok(())
        }
    }
}

fn main() {
    let cli = Cli::parse();
    use std::io::IsTerminal;
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("SHARDSYNC_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("error: start async runtime: {error}");
            std::process::exit(4);
        }
    };
    if let Err(error) = runtime.block_on(run(cli)) {
        eprintln!("error: {error}");
        std::process::exit(error.exit_code());
    }
}
