/*
 * Based on:
 * - https://github.com/smithy-lang/smithy-rs/blob/main/aws/sdk/integration-tests/s3/tests/concurrency.rs
 */

use aws_config;
use aws_config::timeout::TimeoutConfig;
use aws_sdk_s3::error::BoxError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder;
use aws_types::SdkConfig;

use aws_types::sdk_config::SharedHttpClient;
use hdrhistogram::sync::SyncHistogram;
use hdrhistogram::Histogram;
use hyper_rustls;
use std::env;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::{Duration, Instant};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

// WARNING:
// Depending on the settings bellow, you may run into errors related to "Too many open files".
// Tune limit with before run: ulimit -n <num_fd>

const TASK_COUNT: usize = 1_000;
// Larger requests take longer to send, which means we'll consume more network resources per
// request, which means we can't support as many concurrent connections to S3.
const TASK_PAYLOAD_LENGTH: usize = 134_422_528;
// At 130 and above, this test will fail with a `ConnectorError` from `hyper`. I've seen:
// - ConnectorError { kind: Io, source: hyper::Error(Canceled, hyper::Error(Io, Os { code: 54, kind: ConnectionReset, message: "Connection reset by peer" })) }
// - ConnectorError { kind: Io, source: hyper::Error(BodyWrite, Os { code: 32, kind: BrokenPipe, message: "Broken pipe" }) }
// These errors don't necessarily occur when actually running against S3 with concurrency levels
// above 129. You can test it for yourself by running the
// `test_concurrency_put_object_against_live` test that appears at the bottom of this file.
const CONCURRENCY_LIMIT: usize = 64;

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrency_http1() {
    let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .https_only()
        .enable_http1()
        .build();

    let http_client = HyperClientBuilder::new().build(https_connector);

    run_test(http_client).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrency_http2() {
    let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .https_only()
        .enable_http2()
        .build();

    let http_client = HyperClientBuilder::new().build(https_connector);

    run_test(http_client).await;
}

fn generate_test_file() -> Result<PathBuf, BoxError> {
    let path = format!("/dev/shm/s3_bench").into();

    let mut yes_process = Command::new("yes")
        .arg("01234567890abcdefghijklmnopqrstuvwxyz")
        .stdout(Stdio::piped())
        .spawn()?;

    let mut head_process = Command::new("head")
        .arg("-c")
        .arg(format!("{}", TASK_PAYLOAD_LENGTH))
        .stdin(yes_process.stdout.take().unwrap())
        .stdout(Stdio::piped())
        .spawn()?;

    let mut file = std::fs::File::create(&path)?;
    head_process.stdout.as_mut().unwrap();
    std::io::copy(&mut head_process.stdout.take().unwrap(), &mut file)?;

    let exit_status = head_process.wait()?;

    if !exit_status.success() {
        Err("failed to generate temp file")?
    }

    Ok(path)
}

async fn run_test(http_client: SharedHttpClient) {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let sdk_config = aws_config::from_env()
        .timeout_config(
            TimeoutConfig::builder()
                .connect_timeout(Duration::from_secs(30))
                .read_timeout(Duration::from_secs(30))
                .build(),
        )
        .http_client(http_client)
        .load()
        .await;

    let bucket_name = env::var("BENCH_BUCKET_NAME").unwrap();

    let mut concurrency_limit = 1;
    while concurrency_limit <= CONCURRENCY_LIMIT {
        run_concurrency(bucket_name.clone(), sdk_config.clone(), concurrency_limit).await;
        concurrency_limit *= 2;
    }
}

async fn run_concurrency(bucket_name: String, sdk_config: SdkConfig, concurrency_limit: usize) {
    let client = Client::new(&sdk_config);

    let histogram =
        Histogram::new_with_bounds(1, Duration::from_secs(60 * 60).as_nanos() as u64, 3)
            .unwrap()
            .into_sync();

    let path = generate_test_file().unwrap();

    let test_start = Instant::now();

    println!("creating futures");
    // This semaphore ensures we only run up to <concurrency_limit> requests at once.
    let semaphore = Arc::new(Semaphore::new(concurrency_limit));
    let futures = (0..TASK_COUNT).map(|i| {
        let client = client.clone();
        let key = format!("concurrency/test_object_{:05}", i);
        // make a clone of the semaphore and the recorder that can live in the future
        let semaphore = semaphore.clone();
        let mut histogram_recorder = histogram.recorder();
        let path_clone = path.clone();
        let bucket_name_clone = bucket_name.clone();

        // because we wait on a permit from the semaphore, only <CONCURRENCY_LIMIT> futures
        // will be run at once. Otherwise, we'd quickly get rate-limited by S3.
        async move {
            let permit = semaphore
                .acquire()
                .await
                .expect("we'll get one if we wait long enough");

            let stream = ByteStream::from_path(path_clone).await.unwrap();

            let start = Instant::now();

            let res = client
                .put_object()
                .bucket(bucket_name_clone)
                .key(key)
                .body(stream)
                .send()
                .await
                .expect("request should succeed");

            histogram_recorder.saturating_record(start.elapsed().as_nanos() as u64);
            drop(permit);

            res
        }
    });

    println!("joining futures");
    let res: Vec<_> = ::futures_util::future::join_all(futures).await;
    // Assert we ran all the tasks
    assert_eq!(TASK_COUNT, res.len());

    println!(
        "Concurrency limit: {}, duration: {} seconds",
        concurrency_limit,
        test_start.elapsed().as_secs()
    );

    display_metrics(
        "Request Latency",
        histogram,
        "s",
        Duration::from_secs(1).as_nanos() as f64,
    );
}

fn display_metrics(name: &str, mut h: SyncHistogram<u64>, unit: &str, scale: f64) {
    // Refreshing is required or else we won't see any results at all
    h.refresh();
    println!("displaying {} results from {name} histogram", h.len());
    println!(
        "{name}\n\
        \tmean:\t{:.1}{unit},\n\
        \tp50:\t{:.1}{unit},\n\
        \tp90:\t{:.1}{unit},\n\
        \tp99:\t{:.1}{unit},\n\
        \tmax:\t{:.1}{unit}",
        h.mean() / scale,
        h.value_at_quantile(0.5) as f64 / scale,
        h.value_at_quantile(0.9) as f64 / scale,
        h.value_at_quantile(0.99) as f64 / scale,
        h.max() as f64 / scale,
    );
}
