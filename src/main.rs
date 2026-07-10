use clap::Parser;
use eyre::{Context as _, OptionExt as _};
use ffmpeg_sidecar::ffprobe::ffprobe_path;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use notify::{EventKind, RecursiveMode};
use notify_debouncer_full::{DebouncedEvent, new_debouncer};
use path_slash::{PathBufExt as _, PathExt as _};
use rpassword::read_password;
use run_on_drop::on_drop;
use russh_sftp::client::SftpSession;
use std::{
    collections::HashSet,
    io::Write as _,
    os::windows::fs::MetadataExt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tokio::io::{AsyncReadExt as _, BufReader};
use walkdir::WalkDir;

mod config;

static HANDBRAKE_CLI: &str = r#"C:\Program Files\HandBrake\HandBrakeCLI.exe"#;

static P_D: &str = "[Discover] ";
static P_E: &str = "  [Encode] ";
static P_U: &str = "  [Upload] ";
static P_C: &str = " [Cleanup] ";

fn short_path<P: AsRef<Path>>(p: P) -> String {
    shrinkpath::shrink_to(&p.as_ref().to_string_lossy(), 50)
}

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
    context: Context,
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

        let spinner = context.progress(1, format!("{P_E}{}", short_path(&path)));

        if !matches!(
            file_disposition(&path, &context.config).await,
            Disposition::NeedsTranscode
        ) {
            spinner.finish_and_clear();
            continue;
        }
        if transcoded.contains(&path) {
            continue;
        }
        transcoded.insert(path.clone());
        if let Some(dir) = context.config.paths.encoded.join(&path).parent() {
            std::fs::create_dir_all(dir)?;
        }
        spinner.set_message("processing");
        let mut cmd = tokio::process::Command::new(HANDBRAKE_CLI)
            .arg("--preset-import-file")
            .arg(&context.config.handbrake.cfg)
            .args(["-Z", &context.config.handbrake.preset])
            .arg("-i")
            .arg(context.config.paths.organized.join(&path))
            .arg("-o")
            .arg(context.config.paths.encoded.join(&path))
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;

        let stdout_stream = cmd.stdout.take().ok_or_eyre("no stdout to capture")?;
        let mut reader = BufReader::new(stdout_stream);
        let mut buffer = [0; 128];
        let mut line = Vec::new();

        loop {
            let bytes_read = reader.read(&mut buffer).await?;
            if bytes_read == 0 {
                break; // EOF
            }
            for &byte in &buffer[..bytes_read] {
                if byte == b'\r' {
                    spinner.set_message(std::str::from_utf8(&line)?.to_string());
                    line.clear();
                } else {
                    line.push(byte);
                }
            }
        }

        let status = cmd.wait().await?;
        if !status.success() {
            spinner.finish_with_message("Handbrake failed!");
            eyre::bail!("Handbrake failed: {status}");
        } else {
            spinner.finish_with_message("done.");
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
        Err(_) => {
            return false;
        }
    };
    let metadata = match file.metadata().await {
        Ok(f) => f,
        Err(_) => {
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
    context: Context,
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
        let local_path = context.config.paths.encoded.join(&path);
        let bar = context.bar(1, format!("{P_U}{}", short_path(&path)));
        let Ok(local_metadata) = std::fs::metadata(&local_path) else {
            bar.finish_with_message("Can't stat local file.");
            _next_step.forget();
            continue;
        };
        let local_size = local_metadata.file_size();
        bar.set_length(local_size);
        bar.set_message("preparing...");

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
            bar.finish_with_message(format!("Can't make containing dir on remote. {e}"));
            _next_step.forget();
            continue;
        }

        let mut remote_file = match sftp.create(&remote_path).await {
            Ok(f) => f,
            Err(e) => {
                _next_step.forget();
                bar.finish_with_message(format!("Can't open file on remote. {e}"));
                continue;
            }
        };

        let local_file = match tokio::fs::File::open(&local_path).await {
            Ok(f) => f,
            Err(e) => {
                _next_step.forget();
                bar.finish_with_message(format!("Can't open local file to upload. {e}"));
                continue;
            }
        };

        bar.set_message("copying...");

        let mut local_file = bar.wrap_async_read(local_file);

        if let Err(e) = tokio::io::copy(&mut local_file, &mut remote_file).await {
            bar.finish_with_message(format!("Failed {e}"));
            _next_step.forget();
            continue;
        } else {
            bar.finish_with_message("done.")
        }
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

#[derive(Clone)]
struct Context {
    config: config::Config,
    progress: MultiProgress,
    progress_bar: ProgressStyle,
    progress_plain: ProgressStyle,
}

impl Context {
    fn bar<S: Into<String>>(&self, len: u64, prefix: S) -> ProgressBar {
        let pb = self.progress.add(ProgressBar::new(len));
        pb.set_style(self.progress_bar.clone());
        pb.set_prefix(prefix.into());

        pb
    }

    fn progress<S: Into<String>>(&self, len: u64, prefix: S) -> ProgressBar {
        let pb = self.progress.add(ProgressBar::new(len));
        pb.set_style(self.progress_plain.clone());
        pb.set_prefix(prefix.into());

        pb
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    ffmpeg_sidecar::download::auto_download()
        .map_err(|e| eyre::eyre!("Could not download ffmpeg {e}"))?;

    let progress = MultiProgress::new();
    let progress_bar = ProgressStyle::with_template(
        "[{elapsed_precise}] {prefix:.bold.dim} {bar:40.cyan/blue} {pos:7}/{len:7} {msg}",
    )?
    .progress_chars("##-");
    let progress_plain =
        ProgressStyle::with_template("[{elapsed_precise}] {prefix:.bold.dim} {wide_msg}")?;

    let config = config::load()?;
    let context = Context {
        config,
        progress,
        progress_bar,
        progress_plain,
    };

    std::fs::create_dir_all(&context.config.paths.organized)?;

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
    debouncer.watch(&context.config.paths.organized, RecursiveMode::Recursive)?;

    for entry in WalkDir::new(&context.config.paths.organized) {
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
        context.clone(),
    ));

    let uploader = if let Some(ssh) = &context.config.ssh {
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
            context.clone(),
        )))
    } else {
        None
    };

    while let Some(task) = task_rx.recv().await {
        match task {
            Task::Discover { path, initial } => {
                let path = path
                    .strip_prefix(&context.config.paths.organized)?
                    .to_path_buf();
                let spinner = context.progress(1, format!("{P_D}{}", short_path(&path)));
                let disposition = file_disposition(&path, &context.config).await;
                if let Some(task) = match disposition {
                    Disposition::NeedsTranscode => Some(Task::Transcode(path)),
                    Disposition::TryUpload => Some(Task::Upload(path)),
                    // The copy path isn't written yet, treat it the same as skip.
                    Disposition::NeedsCopy | Disposition::Skip => None,
                } {
                    task_tx.send(task).context("Failed to enqueue next task")?;
                }

                if initial || matches!(disposition, Disposition::Skip) {
                    spinner.finish_and_clear();
                } else {
                    spinner.finish_with_message(format!("{disposition:?}"));
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
                let spinner = context.progress(1, format!("{P_C}{}", short_path(&path)));
                if let Err(e) = trash::delete(context.config.paths.organized.join(&path)) {
                    spinner.finish_with_message(format!("failed. {e}"));
                } else {
                    spinner.finish_with_message("done.")
                }
            }
        }
    }

    transcoder.await??;
    if let Some(uploader) = uploader {
        uploader.await??;
    }

    Ok(())
}
