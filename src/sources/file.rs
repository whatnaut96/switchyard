use crate::Envelope;
use notify::{Config, Event, EventKind, INotifyWatcher, RecursiveMode, Watcher};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use tokio::{
    fs::{self, File},
    io::{self, AsyncReadExt, AsyncSeekExt, SeekFrom},
    sync::mpsc::{Sender, channel},
};
use tracing::{debug, error};

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub struct FileConfig {
    pub path: PathBuf,
    pub read_full: bool,
}

fn is_file_change(kind: &EventKind) -> bool {
    kind.is_create() || kind.is_modify() || kind.is_remove()
}

fn event_touches_path(event: &Event, path: &Path) -> bool {
    event.paths.iter().any(|event_path| {
        event_path == path
            || event_path
                .file_name()
                .is_some_and(|file_name| Some(file_name) == path.file_name())
    })
}

pub async fn start_watch(path: PathBuf, read_full: bool, sender: Sender<Envelope>) {
    let (file_watcher_sender, mut file_watcher_receiver) = channel(100);
    let watch_path = path.parent().expect("INVALID WATCH PATH");
    let mut watcher = INotifyWatcher::new(
        move |res| {
            let _ = file_watcher_sender.blocking_send(res);
        },
        Config::default(),
    )
    .expect("COULD NOT ESTABLISH FILE WATCHER: {e}");
    watcher
        .watch(watch_path, RecursiveMode::NonRecursive)
        .expect("COULD NOT START FILE WATCHER");

    let mut last_position = 0;
    let mut last_payload: Option<Vec<u8>> = None;
    while let Some(event) = file_watcher_receiver.recv().await {
        match event {
            Ok(v) => {
                if !is_file_change(&v.kind) || !event_touches_path(&v, &path) {
                    continue;
                }

                let mut contents: Vec<u8> = Vec::new();
                if read_full {
                    contents = match fs::read(&path).await {
                        Ok(contents) => contents,
                        Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                        Err(e) => panic!("could not read watched file: {e}"),
                    };
                } else {
                    let mut file = match File::open(&path).await {
                        Ok(f) => f,
                        Err(e) => {
                            error!(segment = "source", error = %e, "could not open file");
                            continue;
                        }
                    };
                    let file_size = match file.metadata().await {
                        Ok(f) => f.len(),
                        Err(e) => {
                            error!(segment = "source", error = %e, "could not get file length");
                            continue;
                        }
                    };
                    if file_size < last_position {
                        last_position = 0;
                    }

                    match file.seek(SeekFrom::Start(last_position)).await {
                        Ok(_) => debug!(segment = "source", "file position established"),
                        Err(e) => {
                            error!(segment = "source", error = %e, "coule not establish file position");
                            continue;
                        }
                    };

                    match file.read_to_end(&mut contents).await {
                        Ok(val) => {
                            if val > 0 {
                                last_position += contents.len() as u64;
                            }
                        }
                        Err(e) => {
                            error!(segment = "source", error = %e, "coule not read file to end");
                            continue;
                        }
                    }
                }

                if contents.is_empty() {
                    continue;
                }

                if read_full && last_payload.as_ref().is_some_and(|last| last == &contents) {
                    continue;
                }

                if read_full {
                    last_payload = Some(contents.clone());
                }

                let envelope = Envelope { payload: contents };
                let vol = envelope.payload.len();

                match sender.send(envelope).await {
                    Ok(_) => {
                        debug!(segment = "source", "sent {vol} bytes to channel");
                    }
                    Err(e) => {
                        error!(segment = "source", error = %e, "could not send data to the receiver");
                    }
                }
            }
            Err(e) => {
                error!(segment="source", error = %e, "could not receive event");
            }
        }
    }
}
