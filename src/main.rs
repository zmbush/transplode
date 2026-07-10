use clap::Parser;
use eyre::{Context as _, OptionExt as _};
use ffmpeg_sidecar::ffprobe::ffprobe_path;
use notify::{Config, ErrorKind::InvalidConfig, EventKind, RecursiveMode};
use notify_debouncer_full::{DebouncedEvent, new_debouncer};
use path_slash::{PathBufExt as _, PathExt as _};
use rpassword::read_password;
use run_on_drop::on_drop;
use russh_sftp::client::{self, SftpSession};
use std::{
    collections::HashSet,
    io::Write as _,
    net::TcpStream,
    os::windows::fs::MetadataExt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use walkdir::WalkDir;

mod config;

static HANDBRAKE_CLI: &str = r#"C:\Program Files\HandBrake\HandBrakeCLI.exe"#;

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

async fn file_disposition(path: &Path, config: &config::Config) -> Disposition {
    let from_path = config.paths.organized.join(path);
    let to_path = config.paths.encoded.join(path);

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
    Cleanup(PathBuf),
}

async fn run_transcoder(
    mut transcode_rx: tokio::sync::mpsc::Receiver<PathBuf>,
    tx: tokio::sync::mpsc::UnboundedSender<Task>,
    config: config::Config,
) -> eyre::Result<()> {
    let mut transcoded = HashSet::new();
    while let Some(path) = transcode_rx.recv().await {
        let _next_step = on_drop({
            let path = path.clone();
            || {
                tx.send(Task::Upload(path))
                    .expect("Couldn't move on to next step")
            }
        });
        if !matches!(
            file_disposition(&path, &config).await,
            Disposition::NeedsTranscode
        ) {
            println!(
                "Unnecessary transcode skipped... Trying upload ({})",
                shrinkpath::shrink_to(&path.to_string_lossy(), 50)
            );
            continue;
        }
        if transcoded.contains(&path) {
            continue;
        }
        transcoded.insert(path.clone());
        if let Some(dir) = config.paths.encoded.join(&path).parent() {
            std::fs::create_dir_all(dir)?;
        }
        println!(
            "[Transcode] Processing {}",
            shrinkpath::shrink_to(&path.to_string_lossy(), 50)
        );
        let status = tokio::process::Command::new(HANDBRAKE_CLI)
            .arg("--preset-import-file")
            .arg(&config.handbrake.cfg)
            .args(["-Z", &config.handbrake.preset])
            .arg("-i")
            .arg(config.paths.organized.join(&path))
            .arg("-o")
            .arg(config.paths.encoded.join(&path))
            .stdout(Stdio::inherit())
            .stderr(Stdio::null())
            .status()
            .await?;
        if !status.success() {
            println!("Handbrake failed! Exiting...");
            eyre::bail!("Handbrake failed: {status}");
        }
    }

    Ok(())
}

async fn exists_at(sftp: &SftpSession, expected_size: u64, path: &Path) -> bool {
    let path = match path.to_slash() {
        Some(p) => p.to_string(),
        None => {
            return false;
        }
    };
    let file = match sftp.open(path).await {
        Ok(f) => f,
        Err(e) => {
            return false;
        }
    };
    let metadata = match file.metadata().await {
        Ok(f) => f,
        Err(e) => {
            return false;
        }
    };

    let remote_size = metadata.size.unwrap_or(0);
    expected_size == remote_size
}

async fn ensure_dir_for_file(sftp: &SftpSession, file: &str) -> eyre::Result<()> {
    let file = PathBuf::from(file);
    let mut current_path = String::new();

    let parent_dir = file.parent().ok_or_eyre("no parent")?;

    for component in parent_dir.iter() {
        if component == "/" || component == "\\" {
            continue;
        }
        current_path.push('/');
        current_path.push_str(component.to_str().ok_or_eyre("can't convert component")?);

        if sftp.create_dir(&current_path).await.is_err()
            && sftp.fs_info(&current_path).await.is_err()
        {
            eyre::bail!("can't make parent directory");
        }
    }

    Ok(())
}

async fn run_uploader(
    sftp: SftpSession,
    mut upload_rx: tokio::sync::mpsc::Receiver<PathBuf>,
    tx: tokio::sync::mpsc::UnboundedSender<Task>,
    ssh_config: config::Ssh,
    config: config::Config,
) -> eyre::Result<()> {
    'main: while let Some(path) = upload_rx.recv().await {
        let _next_step = on_drop({
            let path = path.clone();
            || {
                tx.send(Task::Cleanup(path))
                    .expect("Couldn't move on to next step")
            }
        });
        let remote_path = ssh_config.path.join(&path);
        let local_path = config.paths.encoded.join(&path);
        let Ok(local_metadata) = std::fs::metadata(&local_path) else {
            println!(
                "[Upload] Can't stat local file, skipping {}",
                shrinkpath::shrink_to(&local_path.to_string_lossy(), 50)
            );
            _next_step.forget();
            continue;
        };
        let local_size = local_metadata.file_size();

        if exists_at(&sftp, local_size, &remote_path).await {
            // already exists at expected location
            continue;
        }
        for alternate in &ssh_config.other_paths {
            if exists_at(&sftp, local_size, &alternate.join(&path)).await {
                // exists in a legacy location
                continue 'main;
            }
        }

        let remote_path = match remote_path.to_slash() {
            Some(p) => p.to_string(),
            None => {
                _next_step.forget();
                continue;
            }
        };

        if let Err(e) = ensure_dir_for_file(&sftp, &remote_path).await {
            println!("[Upload] Can't make containing dir: {e}");
            _next_step.forget();
            continue;
        }

        let mut remote_file = match sftp.create(&remote_path).await {
            Ok(f) => f,
            Err(e) => {
                _next_step.forget();
                println!("[Upload] Can't create remote file: {e}");
                continue;
            }
        };

        let mut local_file = match tokio::fs::File::open(&local_path).await {
            Ok(f) => f,
            Err(e) => {
                _next_step.forget();
                println!("[Upload] Can't open local file to upload: {e}");
                continue;
            }
        };

        println!(
            "[Upload] Starting... {}",
            shrinkpath::shrink_to(&local_path.to_string_lossy(), 50)
        );

        if let Err(e) = tokio::io::copy(&mut local_file, &mut remote_file).await {
            println!("[Upload] Upload failed! {e}");
            _next_step.forget();
            continue;
        }

        println!(
            "[Upload] {} uploaded",
            shrinkpath::shrink_to(&local_path.to_string_lossy(), 50)
        );
    }
    Ok(())
}

struct Client;
impl russh::client::Handler for Client {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &russh::keys::ssh_key::PublicKey,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    ffmpeg_sidecar::download::auto_download()
        .map_err(|e| eyre::eyre!("Could not download ffmpeg {e}"))?;

    let config = config::load()?;

    std::fs::create_dir_all(&config.paths.organized)?;

    let (task_tx, mut task_rx) = tokio::sync::mpsc::unbounded_channel();
    let (transcode_tx, transcode_rx) = tokio::sync::mpsc::channel::<PathBuf>(25);
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel::<PathBuf>(25);

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
    debouncer.watch(&config.paths.organized, RecursiveMode::Recursive)?;

    for entry in WalkDir::new(&config.paths.organized) {
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

    let transcoder = tokio::spawn(run_transcoder(
        transcode_rx,
        task_tx.clone(),
        config.clone(),
    ));

    let uploader = if let Some(ssh) = &config.ssh {
        let ssh_config = russh::client::Config::default();
        let sh = Client;
        let mut session =
            russh::client::connect(Arc::new(ssh_config), (ssh.address.clone(), 22), sh).await?;

        print!("Enter password: ");
        std::io::stdout().flush()?;

        let pw = read_password()?;
        if !session
            .authenticate_password(&ssh.username, &pw)
            .await?
            .success()
        {
            eyre::bail!("No SSH connection");
        }
        let channel = session.channel_open_session().await?;
        channel.request_subsystem(true, "sftp").await?;
        let sftp = SftpSession::new(channel.into_stream()).await?;

        Some(tokio::spawn(run_uploader(
            sftp,
            upload_rx,
            task_tx.clone(),
            ssh.clone(),
            config.clone(),
        )))
    } else {
        None
    };

    while let Some(task) = task_rx.recv().await {
        match task {
            Task::Discover { path, initial } => {
                let path = path.strip_prefix(&config.paths.organized)?.to_path_buf();

                let disposition = file_disposition(&path, &config).await;
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
                upload_tx
                    .send(path)
                    .await
                    .context("Failed to send upload path")?;
            }
            Task::Cleanup(path) => {
                println!(
                    "[Cleanup] STUB - {}",
                    shrinkpath::shrink_to(&path.to_string_lossy(), 50)
                );
            }
        }
    }

    transcoder.await??;
    if let Some(uploader) = uploader {
        uploader.await??;
    }

    Ok(())
}
