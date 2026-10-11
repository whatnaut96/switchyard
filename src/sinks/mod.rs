use crate::Envelope;
use serde::Deserialize;
use tokio::io::{AsyncWriteExt, stdout};
use tokio::sync::mpsc::Receiver;

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SinkConfig {
    Stdout,
}

pub async fn start_sink(sink: SinkConfig, receiver: &mut Receiver<Envelope>) {
    while let Some(_message) = receiver.recv().await {
        match sink {
            SinkConfig::Stdout => {
                let mut stdout = stdout();
                stdout.write_all(&_message.payload).await.unwrap();
                stdout.flush().await.unwrap();
            }
        }
    }
}
