use std::{cmp::Ordering, os::unix::prelude::OsStrExt, path::Path, sync::Arc};

use config::Configuration;
use log::{error, info};
use pyroscope::PyroscopeAgent;
use pyroscope_pprofrs::{pprof_backend, PprofConfig};
use tokio::sync::broadcast::{self, error::TryRecvError};

pub fn start_profiling(
    config: Arc<Configuration>,
    shutdown: broadcast::Receiver<()>,
    tags: Vec<(&str, &str)>,
) {
    if config.observation.profiles.enable {
        let server_endpoint = config.observation.profiles.server_endpoint.clone();
        if server_endpoint.is_empty() {
            generate_flame_graph(config, shutdown);
            info!("Continuous profiling starts, generate flame graph locally");
        } else {
            start_exporter(config, tags);
            info!("Continuous profiling starts, report to {}", server_endpoint);
        }
    }
}

fn start_exporter(config: Arc<Configuration>, tags: Vec<(&str, &str)>) {
    PyroscopeAgent::builder(
        config.observation.profiles.server_endpoint.clone(),
        "elastic-stream.range-server".to_string(),
    )
    .backend(pprof_backend(
        PprofConfig::new()
            .sample_rate(config.observation.profiles.sampling_frequency as u32)
            .report_thread_name(),
    ))
    .tags(tags)
    .build()
    .expect("build pyroscope agent failed")
    .start()
    .expect("start pyroscope agent failed");
}

fn generate_flame_graph(config: Arc<Configuration>, mut shutdown: broadcast::Receiver<()>) {
    std::thread::Builder::new()
        .name("flamegraph_generator".to_owned())
        .spawn(move || {
            // Bind to CPU processor 0.
            core_affinity::set_for_current(core_affinity::CoreId { id: 0 });

            let guard = match pprof::ProfilerGuardBuilder::default()
                .frequency(config.observation.profiles.sampling_frequency)
                .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                .build()
            {
                Ok(guard) => guard,
                Err(e) => {
                    error!("Failed to build profiler guard: {}", e);
                    return;
                }
            };

            let mut last_generation_instant = std::time::Instant::now();
            let cwd = match std::env::current_dir() {
                Ok(path) => path,
                Err(e) => {
                    error!("Failed to acquire current working directory: {}", e);
                    return;
                }
            };

            let base_path = cwd.join(&config.observation.profiles.report_path);
            if !base_path.exists() {
                if let Err(_e) = std::fs::create_dir_all(base_path.as_path()) {
                    error!(
                        "Failed to create directory[{:?}] to save flamegraph files",
                        base_path
                    )
                }
            }

            loop {
                match shutdown.try_recv() {
                    Err(TryRecvError::Empty) => {}
                    _ => {
                        info!("Continuous Profiling stopped");
                        break;
                    }
                }

                std::thread::sleep(std::time::Duration::from_secs(1));
                if last_generation_instant.elapsed().as_secs()
                    < config.observation.profiles.report_interval
                {
                    continue;
                }

                // Update last generation instant
                last_generation_instant = std::time::Instant::now();

                let report = match guard.report().build() {
                    Ok(report) => report,
                    Err(e) => {
                        error!("Failed to generated report: {}", e);
                        break;
                    }
                };

                let time = chrono::Utc::now();
                let time = time.format("%Y-%m-%d-%H-%M-%S").to_string();
                let file_path = base_path.join(format!("{}.svg", time));
                let file = match std::fs::File::create(file_path.as_path()) {
                    Ok(file) => file,
                    Err(e) => {
                        error!("Failed to create file to save flamegraph: {}", e);
                        break;
                    }
                };

                if let Err(e) = report.flamegraph(file) {
                    error!("Failed to write flamegraph to file: {}", e);
                }

                // Sweep expired flamegraph files
                if let Err(e) = sweep_expired(base_path.as_path(), &config) {
                    error!("Failed to clean expired flamegraph file: {}", e);
                }
            }
        })
        .unwrap();
}

fn sweep_expired(report_path: &Path, config: &Arc<Configuration>) -> std::io::Result<()> {
    let mut entries = std::fs::read_dir(report_path)?
        .flatten()
        .filter(|entry| {
            let file_name = entry.file_name();
            file_name.as_bytes().ends_with(".svg".as_bytes())
        })
        .collect::<Vec<_>>();

    if entries.len() <= config.observation.profiles.max_report_backup {
        return Ok(());
    }

    // Sort by created time in ascending order
    entries.sort_by(|lhs, rhs| {
        let l = match lhs.metadata() {
            Ok(metadata) => match metadata.created() {
                Ok(time) => time,
                Err(_) => {
                    return Ordering::Less;
                }
            },
            Err(_) => {
                return Ordering::Less;
            }
        };

        let r = match rhs.metadata() {
            Ok(metadata) => match metadata.created() {
                Ok(time) => time,
                Err(_) => {
                    return Ordering::Greater;
                }
            },
            Err(_) => {
                return Ordering::Greater;
            }
        };
        l.cmp(&r)
    });

    let mut entries = entries.into_iter().rev().collect::<Vec<_>>();

    loop {
        if entries.len() <= config.observation.profiles.max_report_backup {
            break;
        }

        if let Some(entry) = entries.pop() {
            match std::fs::remove_file(entry.path()) {
                Ok(_) => {
                    info!("Removed flamegraph file[{:?}]", entry.path());
                }
                Err(e) => {
                    error!("Failed to remove file[{:?}]: {}", entry.path(), e);
                }
            }
        }
    }
    Ok(())
}
