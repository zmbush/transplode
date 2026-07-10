use std::{
    io::Write as _,
    os::windows::fs::MetadataExt as _,
    path::{Path, PathBuf},
    sync::Arc,
};

use eyre::OptionExt as _;
use path_slash::PathExt as _;
use rpassword::read_password;
use run_on_drop::on_drop;
use russh_sftp::client::SftpSession;
use tokio::{
    sync::mpsc::{Receiver, UnboundedSender},
    task::JoinHandle,
};

use crate::{Context, P_U, Task, config, maybe_progress::MaybeProgressBar, short_path};

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

pub(crate) async fn start_uploader(
    context: &Context,
    upload_rx: Receiver<(PathBuf, MaybeProgressBar)>,
    task_tx: UnboundedSender<Task>,
) -> eyre::Result<Option<JoinHandle<eyre::Result<()>>>> {
    Ok(if let Some(ssh) = &context.config.ssh {
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
    })
}

pub(crate) async fn exists_at(sftp: &SftpSession, expected_size: u64, path: &Path) -> bool {
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

pub(crate) async fn ensure_dir_for_file(sftp: &SftpSession, file: &str) -> eyre::Result<()> {
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

pub(crate) async fn run_uploader(
    sftp: SftpSession,
    mut upload_rx: Receiver<(PathBuf, MaybeProgressBar)>,
    tx: tokio::sync::mpsc::UnboundedSender<Task>,
    ssh_config: config::Ssh,
    context: Context,
) -> eyre::Result<()> {
    'main: while let Some((path, bar)) = upload_rx.recv().await {
        let _next_step = on_drop({
            let path = path.clone();
            let bar = bar.clone();
            || {
                tx.send(Task::Cleanup(path, bar))
                    .expect("Couldn't move on to next step")
            }
        });
        bar.set_prefix(format!("{P_U}{}", short_path(&path))).await;
        bar.set_style(context.upload_bar.clone()).await;

        let remote_path = ssh_config.path.join(&path);
        let local_path = context.config.paths.encoded.join(&path);
        let Ok(local_metadata) = std::fs::metadata(&local_path) else {
            bar.finish_with_message("Can't stat local file.").await;
            _next_step.forget();
            continue;
        };
        let local_size = local_metadata.file_size();
        bar.set_length(local_size).await;

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
            bar.finish_with_message(format!("Can't make containing dir on remote. {e}"))
                .await;
            _next_step.forget();
            continue;
        }

        let mut remote_file = match sftp.create(&remote_path).await {
            Ok(f) => f,
            Err(e) => {
                _next_step.forget();
                bar.finish_with_message(format!("Can't open file on remote. {e}"))
                    .await;
                continue;
            }
        };

        let local_file = match tokio::fs::File::open(&local_path).await {
            Ok(f) => f,
            Err(e) => {
                _next_step.forget();
                bar.finish_with_message(format!("Can't open local file to upload. {e}"))
                    .await;
                continue;
            }
        };

        bar.set_message("uploading...").await;

        let mut local_file = bar.wrap_async_read(local_file).await;

        if let Err(e) = tokio::io::copy(&mut local_file, &mut remote_file).await {
            bar.finish_with_message(format!("Failed {e}")).await;
            _next_step.forget();
            continue;
        }
    }
    Ok(())
}
