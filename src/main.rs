// TODO: Logging
// TODO: Remove all unwrap
// TODO: Make the buffer going into this a runtime flag? Also allow for unbounded as toggle?
use crate::sinks::{SinkConfig, start_sink};
use crate::sources::{SourceConfig, start_source};
use crate::transforms::{TransformConfig, start_transform};
use clap::Parser;
use serde::Deserialize;
use std::path::PathBuf;
use tokio::{fs, io, sync::mpsc::channel, task::JoinSet};
use tracing::{Level, error, info, span};

mod sinks;
mod sources;
mod transforms;

const LONG_ABOUT: &str = "Switchyard is a configurable data routing engine for building reusable data pipelines. It connects data sources, transformations, and sinks to move and process data between systems without requiring a separate application for every integration.";

#[derive(Debug, Parser)]
#[command(version, about, long_about=LONG_ABOUT)]
struct Args {
    /// Path to the config file
    #[arg(short, long)]
    file: PathBuf,
}

#[derive(Debug, Deserialize)]
struct SwitchyardConfig {
    pipeline: PipelineData,
    source: SourceConfig,
    transform: Option<Vec<TransformConfig>>,
    sink: SinkConfig,
}

#[derive(Debug, Deserialize)]
struct PipelineData {
    id: String,
}
#[derive(Debug)]
struct Envelope {
    payload: Vec<u8>,
}
#[tokio::main]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let config_contents = fs::read_to_string(args.file).await?;
    let switchyard_config: SwitchyardConfig = match toml::from_str(&config_contents) {
        Ok(v) => v,
        Err(e) => {
            panic!("could not deserialize config: {e}")
        }
    };

    let pipeline_id = switchyard_config.pipeline.id.as_str();
    let span = span!(Level::INFO, "pipeline_operation");
    let _guard = span.enter();

    let (source_tx, mut source_rx) = channel::<Envelope>(32);

    let mut set = JoinSet::new();

    let source = switchyard_config.source;
    let transforms = switchyard_config.transform;
    let sink = switchyard_config.sink;

    info!(pipeline_id, "starting source");
    set.spawn(async move { start_source(source, source_tx).await });

    match transforms {
        Some(transforms) => {
            let (transform_tx, mut transform_rx) = channel::<Envelope>(32);

            info!(pipeline_id, "starting transforms");
            set.spawn(
                async move { start_transform(transforms, transform_tx, &mut source_rx).await },
            );

            info!(pipeline_id, "starting sink");
            set.spawn(async move { start_sink(sink, &mut transform_rx).await });
        }
        None => {
            info!(pipeline_id, "starting sink");
            set.spawn(async move { start_sink(sink, &mut source_rx).await });
        }
    }
    while let Some(res) = set.join_next().await {
        match res {
            Ok(_) => info!("component finished"),
            Err(e) => error!(error = %e, "component failed"),
        }
    }
    Ok(())
}
