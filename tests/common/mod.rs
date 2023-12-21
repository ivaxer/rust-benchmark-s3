use std::error;
use std::path::PathBuf;
use std::process::{Command, Stdio};

pub fn generate_test_file(payload_lenght: usize) -> Result<PathBuf, Box<dyn error::Error>> {
    let path = format!("/dev/shm/s3_bench").into();

    let mut yes_process = Command::new("yes")
        .arg("01234567890abcdefghijklmnopqrstuvwxyz")
        .stdout(Stdio::piped())
        .spawn()?;

    let mut head_process = Command::new("head")
        .arg("-c")
        .arg(format!("{}", payload_lenght))
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
