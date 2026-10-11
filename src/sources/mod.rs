use crate::Envelope;
use serde::Deserialize;
use tokio::sync::mpsc::Sender;

mod file;

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceConfig {
    File(file::FileConfig),
}

pub async fn start_source(source: SourceConfig, sender: Sender<Envelope>) {
    match source {
        SourceConfig::File(file::FileConfig { path, read_full }) => {
            file::start_watch(path, read_full, sender).await;
        }
    }
}
