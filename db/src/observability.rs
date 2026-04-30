//! Tracing / observability setup for the `StoreMy` binary.
//!
//! Three sinks can run concurrently, all driven by the same `tracing` events:
//!
//! 1. **Terminal** (always on). Format chosen by `STOREMY_LOG_FORMAT`:
//!    - unset / `compact` — single-line records with span close events.
//!    - `tree` — indented hierarchy from `tracing-tree`. Best for following a query through parser
//!      → binder → executor → storage.
//!    - `pretty` — multi-line, easier to read for one statement at a time.
//!    - `json` — one JSON object per event, pipe to `jq`.
//!
//! 2. **Perfetto / Chrome trace file** (on by default). Every run writes
//!    `./data/traces/trace-<unix_ts>.json`. Drag-and-drop the file into
//!    <https://ui.perfetto.dev> to see a flamegraph + timeline UI. Disable
//!    with `STOREMY_NO_TRACE_FILE=1`. Override the directory with
//!    `STOREMY_TRACE_DIR=/some/path`.
//!
//! 3. **OpenTelemetry → Jaeger / Tempo / etc.** (opt-in). Set `STOREMY_OTEL=1` to export spans over
//!    OTLP/gRPC to `http://localhost:4317` (the default Jaeger all-in-one OTLP port). Override with
//!    `STOREMY_OTEL_ENDPOINT=...`. Then open the Jaeger UI at <http://localhost:16686> to search
//!    across runs.
//!
//! ## Why a guard?
//!
//! Both the Chrome layer and the `OTel` batch exporter buffer events in the
//! background. If the process exits without flushing, the trace file is
//! truncated and Jaeger loses the tail. [`init`] therefore returns a
//! [`TracingGuards`] value that the caller (typically `main`) must keep alive
//! for the lifetime of the program. Dropping it flushes everything cleanly.
//!
//! ## Filtering verbosity
//!
//! `RUST_LOG` is parsed by [`tracing_subscriber::EnvFilter`]. Default:
//! `info,storemy=debug`. Examples:
//!
//! ```bash
//! cargo run                                        # terminal + Perfetto file
//! STOREMY_LOG_FORMAT=tree cargo run                # nicer terminal output
//! RUST_LOG=storemy=trace cargo run                 # crank verbosity
//! STOREMY_OTEL=1 cargo run                         # also export to Jaeger
//! STOREMY_NO_TRACE_FILE=1 cargo run                # skip Perfetto file
//! ```

use std::{env, fs, path::PathBuf, sync::Arc, time::SystemTime};

use opentelemetry::{KeyValue, trace::TracerProvider as _};
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::{Resource, runtime, trace::TracerProvider as SdkTracerProvider};
use tokio::runtime::Runtime;
use tracing_chrome::{ChromeLayerBuilder, FlushGuard};
use tracing_subscriber::{
    EnvFilter, Layer, Registry, fmt, fmt::format::FmtSpan, layer::SubscriberExt,
    util::SubscriberInitExt,
};
use tracing_tree::HierarchicalLayer;

const DEFAULT_FILTER: &str = "info,storemy=debug";

const FORMAT_ENV: &str = "STOREMY_LOG_FORMAT";
const TRACE_DIR_ENV: &str = "STOREMY_TRACE_DIR";
const NO_TRACE_FILE_ENV: &str = "STOREMY_NO_TRACE_FILE";
const OTEL_ENV: &str = "STOREMY_OTEL";
const OTEL_ENDPOINT_ENV: &str = "STOREMY_OTEL_ENDPOINT";

const SERVICE_NAME: &str = "storemy";
const DEFAULT_TRACE_DIR: &str = "./data/traces";
const DEFAULT_OTEL_ENDPOINT: &str = "http://localhost:4317";

/// Holds resources that must outlive every emitted span.
///
/// - `_chrome_guard` flushes the Perfetto JSON file on drop.
/// - `_runtime` keeps the Tokio runtime that hosts the `OTel` batch exporter alive — the exporter
///   spawns a background task that drains the span queue and ships it to the collector. If the
///   runtime dies, the queue is lost.
/// - `otel_provider` is the global tracer provider; we call `shutdown()` on it in `Drop` to make
///   sure pending spans are flushed before exit.
/// - `chrome_path` is just a record of where the trace file landed so `main` can print a hint to
///   the user.
pub struct TracingGuards {
    _chrome_guard: Option<FlushGuard>,
    _runtime: Option<Arc<Runtime>>,
    otel_provider: Option<SdkTracerProvider>,
    chrome_path: Option<PathBuf>,
    otel_endpoint: Option<String>,
}

impl TracingGuards {
    /// Path of the Perfetto trace file for this run, if one was written.
    pub fn chrome_path(&self) -> Option<&std::path::Path> {
        self.chrome_path.as_deref()
    }

    /// OTLP endpoint we're exporting to, if any.
    pub fn otel_endpoint(&self) -> Option<&str> {
        self.otel_endpoint.as_deref()
    }
}

impl Drop for TracingGuards {
    fn drop(&mut self) {
        if let Some(provider) = &self.otel_provider {
            // `shutdown()` flushes pending spans and stops the batch worker.
            // Errors here aren't actionable at exit time.
            if let Err(e) = provider.shutdown() {
                eprintln!("otel shutdown error: {e}");
            }
        }
        // _chrome_guard is dropped here automatically, flushing the file.
    }
}

/// Install the global tracing subscriber. Call exactly once from `main` and
/// hold the returned guard for the lifetime of the process.
pub fn init() -> TracingGuards {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER));

    let term_layer = build_terminal_layer();

    // Chrome and OTel layers are generic over the subscriber type `S`, so they
    // must be constructed inline where `.with(...)` can infer `S` from the
    // surrounding chain. The helpers below set up the side-effecting state
    // (file path, runtime, tracer); we wrap that into layers right here.
    let chrome_path = prepare_chrome_path();
    let (chrome_layer, chrome_guard) = match &chrome_path {
        Some(path) => {
            let (layer, guard) = ChromeLayerBuilder::new()
                .file(path.clone())
                .include_args(true)
                .build();
            (Some(layer), Some(guard))
        }
        None => (None, None),
    };

    let (otel_setup, otel_runtime, otel_endpoint) = build_otel_setup();
    let (otel_layer, otel_provider) = match otel_setup {
        Some((tracer, provider)) => (
            Some(tracing_opentelemetry::layer().with_tracer(tracer)),
            Some(provider),
        ),
        None => (None, None),
    };

    // The boxed terminal layer only `impl Layer<Registry>` (we erased its
    // concrete type to keep the format match arms uniform), so it sits
    // directly on `Registry`. Everything after it is generic over `S` and
    // composes freely.
    let init_result = tracing_subscriber::registry()
        .with(term_layer)
        .with(filter)
        .with(chrome_layer)
        .with(otel_layer)
        .try_init();
    if let Err(e) = init_result {
        eprintln!("tracing init failed: {e}");
    }

    TracingGuards {
        _chrome_guard: chrome_guard,
        _runtime: otel_runtime,
        otel_provider,
        chrome_path,
        otel_endpoint,
    }
}

/// Pick the terminal layer based on `STOREMY_LOG_FORMAT`. Boxed because each
/// branch produces a different concrete type but they all `impl Layer<Registry>`.
fn build_terminal_layer() -> Box<dyn Layer<Registry> + Send + Sync> {
    let format = env::var(FORMAT_ENV).unwrap_or_default();
    match format.as_str() {
        "tree" => HierarchicalLayer::new(2)
            .with_targets(true)
            .with_bracketed_fields(true)
            .with_indent_lines(true)
            .boxed(),
        "json" => fmt::layer()
            .json()
            .with_current_span(true)
            .with_span_list(true)
            .boxed(),
        "pretty" => fmt::layer()
            .pretty()
            .with_span_events(FmtSpan::CLOSE)
            .boxed(),
        _ => fmt::layer()
            .compact()
            .with_target(true)
            .with_span_events(FmtSpan::CLOSE)
            .boxed(),
    }
}

/// Decide where (and whether) to write the Perfetto trace file. Returns the
/// path the layer should write to, or `None` if disabled.
fn prepare_chrome_path() -> Option<PathBuf> {
    if env::var(NO_TRACE_FILE_ENV).is_ok() {
        return None;
    }

    let dir =
        env::var(TRACE_DIR_ENV).map_or_else(|_| PathBuf::from(DEFAULT_TRACE_DIR), PathBuf::from);
    if let Err(e) = fs::create_dir_all(&dir) {
        eprintln!("could not create trace dir {}: {e}", dir.display());
        return None;
    }
    let stamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    Some(dir.join(format!("trace-{stamp}.json")))
}

/// If the user opted in, build a Tokio runtime + OTLP exporter + tracer
/// provider. Returns the tracer + provider for the caller to wrap in a layer,
/// plus the runtime the caller must keep alive. Failures (no Jaeger running,
/// bad endpoint) are downgraded to a warning so we don't crash the database.
type OTelTracerSetup = (opentelemetry_sdk::trace::Tracer, SdkTracerProvider);
type OTelSetup = (
    Option<OTelTracerSetup>,
    Option<Arc<Runtime>>,
    Option<String>,
);

fn build_otel_setup() -> OTelSetup {
    let enabled = env::var(OTEL_ENV)
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
        || env::var(OTEL_ENDPOINT_ENV).is_ok();
    if !enabled {
        return (None, None, None);
    }

    let endpoint =
        env::var(OTEL_ENDPOINT_ENV).unwrap_or_else(|_| DEFAULT_OTEL_ENDPOINT.to_string());

    let runtime = match Runtime::new() {
        Ok(r) => Arc::new(r),
        Err(e) => {
            eprintln!("could not start tokio runtime for OTel: {e}");
            return (None, None, None);
        }
    };

    // The batch exporter spawns its worker via `tokio::spawn`, which requires
    // the current thread to be inside a runtime context. Holding `_enter` for
    // the duration of provider construction satisfies that requirement; the
    // worker keeps running on `runtime` after `_enter` drops.
    let _enter = runtime.enter();

    let exporter = match SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .build()
    {
        Ok(e) => e,
        Err(e) => {
            eprintln!("could not build OTLP exporter for {endpoint}: {e}");
            return (None, None, Some(endpoint));
        }
    };

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter, runtime::Tokio)
        .with_resource(Resource::new(vec![KeyValue::new(
            "service.name",
            SERVICE_NAME,
        )]))
        .build();
    let tracer = provider.tracer(SERVICE_NAME);
    let _ = opentelemetry::global::set_tracer_provider(provider.clone());

    (Some((tracer, provider)), Some(runtime), Some(endpoint))
}
