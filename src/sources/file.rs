use crate::Envelope;
use notify::{Config, Event, EventKind, INotifyWatcher, RecursiveMode, Watcher};
use serde::Deserialize;
use tokio::{
    fs::{self, File},
    io::{self, AsyncReadExt, AsyncSeekExt, SeekFrom},
    sync::mpsc::{Sender, channel},
};

use std::path::{Path, PathBuf};
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
    let watch_path = path.parent().unwrap_or_else(|| Path::new("."));
    let mut watcher = INotifyWatcher::new(
        move |res| {
            let _ = file_watcher_sender.blocking_send(res);
        },
        Config::default(),
    )
    .unwrap();
    watcher
        .watch(watch_path, RecursiveMode::NonRecursive)
        .unwrap();

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
                    let mut file = File::open(&path).await.unwrap();
                    let file_size = file.metadata().await.unwrap().len();
                    if file_size < last_position {
                        last_position = 0;
                    }

                    file.seek(SeekFrom::Start(last_position)).await.unwrap();
                    if file.read_to_end(&mut contents).await.unwrap() > 0 {
                        last_position += contents.len() as u64;
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

                let message = Envelope { payload: contents };
                sender.send(message).await.unwrap();
            }
            Err(_) => {
                todo!()
            }
        }
    }
}
