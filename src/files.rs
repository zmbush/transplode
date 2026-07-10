use ffmpeg_sidecar::ffprobe::ffprobe_path;

use std::path::Path;

pub(crate) async fn get_duration(path: &Path) -> eyre::Result<f32> {
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
pub(crate) enum Disposition {
    Skip,
    NeedsTranscode,
    NeedsCopy,
    MaybeUpload,
}

pub(crate) enum FileClass {
    Transcodable,
    Copyable,
}

pub(crate) fn file_class(path: &Path) -> Option<FileClass> {
    let path_str = path.to_string_lossy();
    if path_str.contains("_riplex") {
        return None;
    }

    if let Some(ext) = path.extension().and_then(|os_str| os_str.to_str()) {
        match ext {
            "png" | "jpg" | "jpeg" | "txt" => Some(FileClass::Copyable),
            "mkv" => Some(FileClass::Transcodable),
            _ => None,
        }
    } else {
        None
    }
}

pub(crate) async fn file_disposition(path: &Path, config: &crate::config::Config) -> Disposition {
    let from_path = config.paths.organized.join(path);
    let to_path = config.paths.encoded.join(path);
    let Some(class) = file_class(path) else {
        return Disposition::Skip;
    };

    // The file has already been copied/transcoded. Just needs to try uploading.
    if to_path.exists() && !from_path.exists() {
        return Disposition::MaybeUpload;
    }

    match class {
        FileClass::Copyable => {
            if to_path.exists() {
                Disposition::MaybeUpload
            } else {
                Disposition::NeedsCopy
            }
        }
        FileClass::Transcodable => {
            let from_duration = get_duration(&from_path).await.ok();

            if to_path.exists() {
                let to_duration = get_duration(&to_path).await.ok();

                if let Some(fd) = from_duration
                    && let Some(td) = to_duration
                    && approx::abs_diff_eq!(fd, td, epsilon = 1.0e-1)
                {
                    // The file has been transcoded fully. Try uploading it next.
                    Disposition::MaybeUpload
                } else {
                    Disposition::NeedsTranscode
                }
            } else {
                Disposition::NeedsTranscode
            }
        }
    }
}
