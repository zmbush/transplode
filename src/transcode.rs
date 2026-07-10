use std::{collections::HashSet, path::PathBuf, process::Stdio};

use eyre::OptionExt as _;
use run_on_drop::on_drop;
use tokio::io::{AsyncReadExt as _, BufReader};

use crate::{
    Context, P_E, Task,
    files::{Disposition, file_disposition},
    maybe_progress::MaybeProgressBar,
    short_path,
};

static HANDBRAKE_CLI: &str = r#"C:\Program Files\HandBrake\HandBrakeCLI.exe"#;

pub(crate) async fn run_transcoder(
    mut transcode_rx: tokio::sync::mpsc::Receiver<(PathBuf, MaybeProgressBar)>,
    tx: tokio::sync::mpsc::UnboundedSender<Task>,
    context: Context,
) -> eyre::Result<()> {
    let percent_re = regex::Regex::new(r#"(?<percent>\d+.\d+ *)% *(?<extra_stats>\(.*\))?"#)?;
    let mut transcoded = HashSet::new();
    while let Some((path, bar)) = transcode_rx.recv().await {
        let _next_step = on_drop({
            let path = path.clone();
            let bar = bar.clone();
            || {
                tx.send(Task::Upload(path, bar))
                    .expect("Couldn't move on to next step")
            }
        });

        bar.set_style(context.transcode_bar.clone()).await;
        bar.set_prefix(format!("{P_E}{}", short_path(&path))).await;
        bar.set_length(10000).await;

        if !matches!(
            file_disposition(&path, &context.config).await,
            Disposition::NeedsTranscode
        ) {
            continue;
        }
        if transcoded.contains(&path) {
            continue;
        }
        transcoded.insert(path.clone());
        if let Some(dir) = context.config.paths.encoded.join(&path).parent() {
            std::fs::create_dir_all(dir)?;
        }
        bar.set_message("processing").await;
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
                    let line_str = std::str::from_utf8(&line)?.to_string();
                    if let Some(segment) = percent_re.captures(&line_str) {
                        if let Ok(percent) = segment["percent"].trim().parse::<f32>() {
                            bar.set_position((percent * 100.) as u64).await;
                            if let Some(extra) = segment.name("extra_stats") {
                                bar.set_message(extra.as_str().to_string()).await;
                            } else {
                                bar.set_message("").await;
                            }
                        } else {
                            bar.set_position(0).await;
                            bar.set_message(line_str).await;
                        }
                    }
                    line.clear();
                } else {
                    line.push(byte);
                }
            }
        }

        let status = cmd.wait().await?;
        if !status.success() {
            bar.finish_with_message(format!("Handbrake failed! {status}"))
                .await;
            _next_step.forget();
        }
    }

    Ok(())
}
