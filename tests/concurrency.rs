/*
 * Based on:
 * - https://github.com/smithy-lang/smithy-rs/blob/main/aws/sdk/integration-tests/s3/tests/concurrency.rs
 */

mod common;

use aws_config;
use aws_config::timeout::TimeoutConfig;
use aws_sdk_s3::Client;
use aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use aws_types::SdkConfig;

use hdrhistogram::sync::SyncHistogram;
use hdrhistogram::Histogram;
use hyper_rustls;

use std::env;
use std::sync::Arc;

use tokio::fs;
use tokio::sync::Semaphore;
use tokio::time::{Duration, Instant};

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
// Size of buffer used to read file (in bytes)
const BUFFER_SIZE: usize = 32_784;

#[derive(Debug)]
enum ByteStreamType {
    FromPath,
    ReadFrom,
    SdkBody,
}

#[derive(Debug)]
enum Protocol {
    Http1,
    Http2,
}

struct TestParams {
    task_count: usize,
    payload_length: usize,
    concurrency_initial: usize,
    concurrency_step: usize,
    concurrency_limit: usize,
    byte_stream_type: ByteStreamType,
    protocol: Protocol,
    buffer_size: usize,
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrency_http1() {
    let test_params = TestParams {
        task_count: TASK_COUNT,
        payload_length: TASK_PAYLOAD_LENGTH,
        concurrency_initial: 1,
        concurrency_step: 2,
        concurrency_limit: CONCURRENCY_LIMIT,
        byte_stream_type: ByteStreamType::ReadFrom,
        protocol: Protocol::Http1,
        buffer_size: BUFFER_SIZE,
    };

    run_test(test_params).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrency_http2() {
    let test_params = TestParams {
        task_count: TASK_COUNT,
        payload_length: TASK_PAYLOAD_LENGTH,
        concurrency_initial: 1,
        concurrency_step: 2,
        concurrency_limit: CONCURRENCY_LIMIT,
        byte_stream_type: ByteStreamType::ReadFrom,
        protocol: Protocol::Http2,
        buffer_size: BUFFER_SIZE,
    };

    run_test(test_params).await;
}

#[ignore = "ignore utility test"]
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrency_bytestream() {
    for byte_stream_type in vec![
        ByteStreamType::SdkBody,
        ByteStreamType::FromPath,
        ByteStreamType::ReadFrom,
    ] {
        println!("Start test with {:?}", byte_stream_type);

        let test_params = TestParams {
            task_count: 1,
            payload_length: 5 * 1024 * 1024 * 1024,
            concurrency_initial: 1,
            concurrency_step: 1,
            concurrency_limit: 1,
            byte_stream_type: byte_stream_type,
            protocol: Protocol::Http1,
            buffer_size: BUFFER_SIZE,
        };

        run_test(test_params).await;
    }
}

async fn run_test(test_params: TestParams) {
    let http_client = match test_params.protocol {
        Protocol::Http1 => {
            let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()
                .https_only()
                .enable_http1()
                .build();

            HyperClientBuilder::new().build(https_connector)
        }
        Protocol::Http2 => {
            let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()
                .https_only()
                .enable_http2()
                .build();

            HyperClientBuilder::new().build(https_connector)
        }
    };

    let mut config_loader = aws_config::from_env()
        .timeout_config(
            TimeoutConfig::builder()
                .connect_timeout(Duration::from_secs(120))
                .read_timeout(Duration::from_secs(120))
                .build(),
        )
        .http_client(http_client);

    if let Ok(endpoint_url) = env::var("BENCH_ENDPOINT_URL") {
        config_loader = config_loader.endpoint_url(endpoint_url);
    }

    let sdk_config = config_loader.load().await;

    let bucket_name = env::var("BENCH_BUCKET_NAME").unwrap();

    let mut concurrency_limit = test_params.concurrency_initial;
    while concurrency_limit <= test_params.concurrency_limit {
        run_concurrency(&test_params, &bucket_name, &sdk_config, concurrency_limit).await;
        concurrency_limit += test_params.concurrency_step;
    }
}

async fn run_concurrency(
    test_params: &TestParams,
    bucket_name: &String,
    sdk_config: &SdkConfig,
    concurrency_limit: usize,
) {
    let client = Client::new(&sdk_config);

    let histogram =
        Histogram::new_with_bounds(1, Duration::from_secs(60 * 60).as_nanos() as u64, 3)
            .unwrap()
            .into_sync();

    let path = common::generate_test_file(test_params.payload_length).unwrap();

    let test_start = Instant::now();

    println!("creating futures");
    // This semaphore ensures we only run up to <concurrency_limit> requests at once.
    let semaphore = Arc::new(Semaphore::new(concurrency_limit));
    let futures = (0..test_params.task_count).map(|i| {
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

            let stream = match test_params.byte_stream_type {
                ByteStreamType::FromPath => ByteStream::from_path(path_clone).await.unwrap(),
                ByteStreamType::ReadFrom => ByteStream::read_from()
                    .path(path_clone)
                    .buffer_size(test_params.buffer_size)
                    .build()
                    .await
                    .unwrap(),
                ByteStreamType::SdkBody => {
                    let source_file = fs::File::open(path_clone).await.unwrap();

                    let reader = tokio_util::io::ReaderStream::with_capacity(
                        source_file,
                        test_params.buffer_size,
                    );
                    let body = hyper::body::Body::wrap_stream(reader);
                    ByteStream::new(SdkBody::from_body_0_4(body))
                }
            };

            let start = Instant::now();

            let res = client
                .put_object()
                .bucket(bucket_name_clone)
                .key(key.clone())
                .body(stream)
                .send()
                .await
                .expect("request should succeed");

            println!("Put response: {:?}", &res);

            histogram_recorder.saturating_record(start.elapsed().as_nanos() as u64);
            println!(
                "Uploaded key {} for: {}",
                key,
                start.elapsed().as_secs_f64()
            );
            drop(permit);

            res
        }
    });

    println!("joining futures");
    let res: Vec<_> = ::futures_util::future::join_all(futures).await;
    // Assert we ran all the tasks
    assert_eq!(test_params.task_count, res.len());

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
    println!("displaying {} results", h.len());
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
