use clap::Parser;
use eyre::Context as _;
use indicatif::{MultiProgress, ProgressStyle};
use std::path::{Path, PathBuf};

use crate::maybe_progress::MaybeProgressBar;

mod config;
mod discovery;
mod files;
mod maybe_progress;
mod transcode;
mod upload;

static P_D: &str = "[Discover] ";
static P_E: &str = "[  Encode] ";
static P_U: &str = "[  Upload] ";
static P_C: &str = "[ Cleanup] ";
static P_F: &str = "[    Done] ";

fn short_path<P: AsRef<Path>>(p: P) -> String {
    shrinkpath::shrink_to(&p.as_ref().to_string_lossy(), 40)
}

#[derive(Parser, Clone)]
#[command(version, about)]
struct Cmd {
    from: PathBuf,
    to: PathBuf,
    #[arg(short, long)]
    config: PathBuf,
}

enum Task {
    Discover { path: PathBuf, initial: bool },
    Transcode(PathBuf, MaybeProgressBar),
    Upload(PathBuf, MaybeProgressBar),
    Cleanup(PathBuf, MaybeProgressBar),
}

#[derive(Clone)]
struct Context {
    config: config::Config,
    progress: MultiProgress,
    upload_bar: ProgressStyle,
    transcode_bar: ProgressStyle,
    progress_plain: ProgressStyle,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    ffmpeg_sidecar::download::auto_download()
        .map_err(|e| eyre::eyre!("Could not download ffmpeg {e}"))?;

    let progress = MultiProgress::new();
    let upload_bar = ProgressStyle::with_template(
        "[{elapsed_precise}] {prefix:50!.bold.dim} {wide_bar:.cyan/blue} {bytes}/{total_bytes} {msg}",
    )?;
    let transcode_bar = ProgressStyle::with_template(
        "[{elapsed_precise}] {prefix:50!.bold.dim} {wide_bar:.cyan/blue} {percent_precise}% {msg}",
    )?;
    let progress_plain =
        ProgressStyle::with_template("[{elapsed_precise}] {prefix:50.bold.dim} {wide_msg}")?;

    let config = config::load()?;
    let context = Context {
        config,
        progress,
        upload_bar,
        transcode_bar,
        progress_plain,
    };

    std::fs::create_dir_all(&context.config.paths.organized)?;

    let (task_tx, mut task_rx) = tokio::sync::mpsc::unbounded_channel();
    let (transcode_tx, transcode_rx) =
        tokio::sync::mpsc::channel::<(PathBuf, MaybeProgressBar)>(50);
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel::<(PathBuf, MaybeProgressBar)>(50);

    discovery::start_discovery(task_tx.clone(), &context)
        .context("while starting discovery subprocess")?;

    let transcoder = tokio::spawn(transcode::run_transcoder(
        transcode_rx,
        task_tx.clone(),
        context.clone(),
    ));

    let uploader = upload::start_uploader(&context, upload_rx, task_tx.clone()).await?;

    while let Some(task) = task_rx.recv().await {
        match task {
            Task::Discover { path, initial } => {
                let path = path
                    .strip_prefix(&context.config.paths.organized)
                    .or_else(|_| path.strip_prefix(&context.config.paths.encoded))?
                    .to_path_buf();
                let spinner = MaybeProgressBar::new(context.clone());
                spinner
                    .set_prefix(format!("{P_D}{}", short_path(&path)))
                    .await;
                let disposition = files::file_disposition(&path, &context.config).await;

                if !initial {
                    spinner.set_message(format!("{disposition:?}")).await;
                }
                if let Some(task) = match disposition {
                    files::Disposition::NeedsTranscode => Some(Task::Transcode(path, spinner)),
                    files::Disposition::MaybeUpload => Some(Task::Upload(path, spinner)),
                    // The copy path isn't written yet, treat it the same as skip.
                    files::Disposition::NeedsCopy | files::Disposition::Skip => {
                        spinner.finish_and_clear().await;
                        None
                    }
                } {
                    task_tx.send(task).context("Failed to enqueue next task")?;
                }
            }
            Task::Transcode(path, bar) => {
                transcode_tx
                    .send((path, bar))
                    .await
                    .context("Failed to send transcode path")?;
            }
            Task::Upload(path, bar) => {
                upload_tx
                    .send((path, bar))
                    .await
                    .context("Failed to send upload path")?;
            }
            Task::Cleanup(path, bar) => {
                bar.set_prefix(format!("{P_C}{}", short_path(&path))).await;
                bar.set_style(context.progress_plain.clone()).await;
                let to_cleanup = context.config.paths.organized.join(&path);
                if !to_cleanup.exists() {
                    // Nothing to clean up.
                    bar.finish_with_message("done.").await;
                } else {
                    if let Err(e) = trash::delete(to_cleanup) {
                        bar.finish_with_message(format!("failed. {e}")).await;
                    } else {
                        bar.finish_with_message("done.").await;
                    }
                }
                bar.set_prefix(format!("{P_F}{}", short_path(&path))).await;
            }
        }
    }

    transcoder.await??;
    if let Some(uploader) = uploader {
        uploader.await??;
    }

    Ok(())
}
