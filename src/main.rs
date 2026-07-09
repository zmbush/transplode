use clap::Parser;
use eyre::Context as _;
use ffmpeg_sidecar::ffprobe::ffprobe_path;
use notify::{EventKind, RecursiveMode};
use notify_debouncer_full::{DebouncedEvent, new_debouncer};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use walkdir::WalkDir;

static HANDBRAKE_CLI: &str = r#"C:\Program Files\HandBrake\HandBrakeCLI.exe"#;

#[derive(Debug, Deserialize)]
struct Config {}

#[derive(Parser, Clone)]
#[command(version, about)]
struct Cmd {
    from: PathBuf,
    to: PathBuf,
    #[arg(short, long)]
    config: PathBuf,
}

async fn get_duration(path: &Path) -> eyre::Result<f32> {
    Ok(String::from_utf8_lossy(
        &tokio::process::Command::new(ffprobe_path())
            .args([
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
            ])
            .arg(path)
            .output()
            .await?
            .stdout,
    )
    .trim()
    .parse()?)
}

#[derive(Debug)]
enum Disposition {
    Skip,
    NeedsTranscode,
    NeedsCopy,
    TryUpload,
}

async fn file_disposition(path: &Path, cmd: &Cmd) -> Disposition {
    let from_path = cmd.from.join(path);
    let to_path = cmd.to.join(path);

    let path_str = path.to_string_lossy();
    if path_str.contains("_riplex") {
        return Disposition::Skip;
    }
    if let Some(ext) = path.extension().and_then(|os_str| os_str.to_str()) {
        match ext {
            "png" | "jpg" | "jpeg" => {
                if to_path.exists() {
                    Disposition::Skip
                } else {
                    Disposition::NeedsCopy
                }
            }
            "mkv" => {
                let from_duration = get_duration(&from_path).await.ok();

                if to_path.exists() {
                    let to_duration = get_duration(&to_path).await.ok();

                    if let Some(fd) = from_duration
                        && let Some(td) = to_duration
                        && approx::abs_diff_eq!(fd, td, epsilon = 1.0e-1)
                    {
                        // The file has been transcoded fully. Try uploading it next.
                        Disposition::TryUpload
                    } else {
                        Disposition::NeedsTranscode
                    }
                } else {
                    Disposition::NeedsTranscode
                }
            }
            _ => Disposition::Skip,
        }
    } else {
        Disposition::Skip
    }
}

enum Task {
    Discover { path: PathBuf, initial: bool },
    Transcode(PathBuf),
    Upload(PathBuf),
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    ffmpeg_sidecar::download::auto_download()
        .map_err(|e| eyre::eyre!("Could not download ffmpeg {e}"))?;

    let cmd = Cmd::parse();

    std::fs::create_dir_all(&cmd.to)?;

    let (task_tx, mut task_rx) = tokio::sync::mpsc::unbounded_channel();
    let (transcode_tx, mut transcode_rx) = tokio::sync::mpsc::channel::<PathBuf>(25);

    let mut debouncer = new_debouncer(Duration::from_secs(5), None, {
        let tx = task_tx.clone();
        move |debounced_events: Result<Vec<DebouncedEvent>, _>| match debounced_events {
            Ok(events) => {
                for DebouncedEvent { event, .. } in events {
                    match event.kind {
                        EventKind::Any => todo!(),
                        EventKind::Create(_) | EventKind::Modify(_) => {
                            for path in event.paths {
                                tx.send(Task::Discover {
                                    path,
                                    initial: false,
                                })
                                .expect("Could net send discovery task to queue");
                            }
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => println!("Watch error: {:?}", e),
        }
    })?;
    debouncer.watch(&cmd.from, RecursiveMode::Recursive)?;

    for entry in WalkDir::new(&cmd.from) {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        task_tx
            .send(Task::Discover {
                path: path.to_path_buf(),
                initial: true,
            })
            .context("Failed to fill initial task queue")?;
    }

    let transcoder = tokio::spawn({
        let cmd = cmd.clone();
        let tx = task_tx.clone();
        async move {
            let mut transcoded = HashSet::new();
            while let Some(path) = transcode_rx.recv().await {
                if !matches!(
                    file_disposition(&path, &cmd).await,
                    Disposition::NeedsTranscode
                ) {
                    println!(
                        "Unnecessary transcode skipped... Trying upload ({})",
                        shrinkpath::shrink_to(&path.to_string_lossy(), 50)
                    );
                    tx.send(Task::Upload(path))
                        .context("Failed to enqueue upload event")?;
                    continue;
                }
                if transcoded.contains(&path) {
                    continue;
                }
                transcoded.insert(path.clone());
                if let Some(dir) = cmd.to.join(&path).parent() {
                    std::fs::create_dir_all(dir)?;
                }
                println!(
                    "[Transcode] Processing {}",
                    shrinkpath::shrink_to(&path.to_string_lossy(), 50)
                );
                let status = tokio::process::Command::new(HANDBRAKE_CLI)
                    .arg("--preset-import-file")
                    .arg(&cmd.config)
                    .args(["-Z", "Archiving Old Movies"])
                    .arg("-i")
                    .arg(cmd.from.join(&path))
                    .arg("-o")
                    .arg(cmd.to.join(&path))
                    .stdout(Stdio::inherit())
                    .stderr(Stdio::null())
                    .status()
                    .await?;
                if !status.success() {
                    println!("Handbrake failed! Exiting...");
                    eyre::bail!("Handbrake failed: {status}");
                }
                tx.send(Task::Upload(path))
                    .context("Failed to enqueue upload event")?;
            }

            Ok(())
        }
    });

    while let Some(task) = task_rx.recv().await {
        match task {
            Task::Discover { path, initial } => {
                let path = path.strip_prefix(&cmd.from)?.to_path_buf();

                let disposition = file_disposition(&path, &cmd).await;
                if !initial {
                    println!(
                        "[Discover] Checking ({disposition:?}) {}",
                        shrinkpath::shrink_to(&path.to_string_lossy(), 50)
                    );
                }
                if let Some(task) = match disposition {
                    Disposition::NeedsTranscode => Some(Task::Transcode(path)),
                    Disposition::TryUpload => Some(Task::Upload(path)),
                    // The copy path isn't written yet, treat it the same as skip.
                    Disposition::NeedsCopy | Disposition::Skip => None,
                } {
                    task_tx.send(task).context("Failed to enqueue next task")?;
                }
            }
            Task::Transcode(path) => {
                transcode_tx
                    .send(path)
                    .await
                    .context("Failed to send transcode path")?;
            }
            Task::Upload(path) => {
                println!(
                    "[Upload] STUB - upload to server ({})",
                    shrinkpath::shrink_to(&path.to_string_lossy(), 50)
                );
            }
        }
    }

    transcoder.await?
}
