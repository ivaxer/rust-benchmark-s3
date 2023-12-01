# rust-benchmark-s3

Simple tests to benchmark S3 concurrency with http/1 vs http/2.

Linux only supported.

## How to run

Configure env:

```(bash)
export AWS_REGION=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_ACCESS_KEY_ID=...
export BENCH_BUCKET_NAME=...
```

Run:

```(bash)
cargo test --release -- --nocapture --test-threads 1
```
