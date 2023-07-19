#[cfg(feature = "env")]
use std::io::Write;

#[cfg(feature = "env")]
pub fn try_init_log() {
    let _ = env_logger::builder()
        .is_test(true)
        .format(|buf, record| {
            writeln!(
                buf,
                "{}:{} {} [{}] - {}",
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f"),
                record.level(),
                record.args()
            )
        })
        .try_init();
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "env")]
    #[test]
    fn test_init_log() {
        super::try_init_log();
        log::trace!("Record at trace");
        log::debug!("Record at debug");
        log::info!("Record at info");
        log::warn!("Record at warn");
        log::error!("Record at error");
    }
}
