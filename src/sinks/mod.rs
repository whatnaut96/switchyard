use crate::Envelope;
use serde::Deserialize;
use tokio::io::{AsyncWriteExt, stdout};
use tokio::sync::mpsc::Receiver;
use tracing::error;

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
                if let Err(e) = stdout.write_all(&_message.payload).await {
                    error!(error = %e, "could not write payload to stdout");
                    panic!("could not write payload to stdout: {e}");
                }
                if let Err(e) = stdout.flush().await {
                    error!(error = %e, "could not flush stdout");
                    panic!("could not flush stdout: {e}");
                }
            }
        }
    }
}
