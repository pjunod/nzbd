use std::error::Error;
use std::io;
use std::process::Command;

#[cfg(unix)]
pub fn sampled_rss_bytes() -> Result<u64, Box<dyn Error>> {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "ps failed while sampling RSS: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
        .into());
    }

    let rss_kib = String::from_utf8(output.stdout)?.trim().parse::<u64>()?;
    rss_kib
        .checked_mul(1024)
        .ok_or_else(|| io::Error::other("sampled RSS overflow").into())
}

#[cfg(windows)]
pub fn sampled_rss_bytes() -> Result<u64, Box<dyn Error>> {
    let command = format!("(Get-Process -Id {}).WorkingSet64", std::process::id());
    let output = Command::new("powershell.exe")
        .args(["-NoProfile", "-NonInteractive", "-Command", &command])
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "PowerShell failed while sampling RSS: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
        .into());
    }

    Ok(String::from_utf8(output.stdout)?.trim().parse::<u64>()?)
}

#[cfg(not(any(unix, windows)))]
pub fn sampled_rss_bytes() -> Result<u64, Box<dyn Error>> {
    Err(io::Error::other("RSS sampling is supported only on Unix and Windows").into())
}

pub fn observe_rss(max_sampled_rss_bytes: &mut u64) -> Result<u64, Box<dyn Error>> {
    let sampled_rss_bytes = sampled_rss_bytes()?;
    *max_sampled_rss_bytes = (*max_sampled_rss_bytes).max(sampled_rss_bytes);
    Ok(sampled_rss_bytes)
}
