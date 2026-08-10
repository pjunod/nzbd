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

pub fn enforce_rss_growth_ceiling(
    probe: &str,
    sampled_rss_growth_bytes: u64,
    ceiling_bytes: u64,
) -> Result<(), Box<dyn Error>> {
    if sampled_rss_growth_bytes > ceiling_bytes {
        return Err(io::Error::other(format!(
            "{probe} sampled RSS growth {sampled_rss_growth_bytes} exceeded the {ceiling_bytes}-byte regression ceiling"
        ))
        .into());
    }
    Ok(())
}

pub fn verify_rss_growth_ceiling_guard(
    probe: &str,
    ceiling_bytes: u64,
) -> Result<(), Box<dyn Error>> {
    enforce_rss_growth_ceiling(probe, ceiling_bytes, ceiling_bytes)?;
    let first_excess_byte = ceiling_bytes
        .checked_add(1)
        .ok_or_else(|| io::Error::other("RSS ceiling negative control overflowed"))?;
    if enforce_rss_growth_ceiling(probe, first_excess_byte, ceiling_bytes).is_ok() {
        return Err(io::Error::other(format!(
            "{probe} RSS ceiling negative control accepted the first excess byte"
        ))
        .into());
    }
    Ok(())
}
