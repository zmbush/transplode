use std::{collections::HashSet, time::Duration};

use eyre::Context as _;
use notify::{EventKind, RecursiveMode};
use notify_debouncer_full::{DebouncedEvent, new_debouncer};
use tokio::sync::mpsc::UnboundedSender;
use walkdir::WalkDir;

use crate::{Context, Task};

pub(crate) fn start_discovery(
    task_tx: UnboundedSender<Task>,
    context: &Context,
) -> eyre::Result<()> {
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

    let mut in_organized = HashSet::new();
    for entry in WalkDir::new(&context.config.paths.organized) {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        in_organized.insert(path.to_path_buf());
        task_tx
            .send(Task::Discover {
                path: path.to_path_buf(),
                initial: true,
            })
            .context("Failed to fill initial task queue")?;
    }

    for entry in WalkDir::new(&context.config.paths.encoded) {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if in_organized.contains(path) {
            continue;
        }
        task_tx
            .send(Task::Discover {
                path: path.to_path_buf(),
                initial: true,
            })
            .context("Failed to fill initial task queue")?;
    }

    Ok(())
}
