// TODO: Remove all unwrap
// TODO: Make the buffer going into this a runtime flag? Also allow for unbounded as toggle?
use clap::Parser;
use notify::{Config, Event, EventKind, INotifyWatcher, RecursiveMode, Watcher};
use serde::Deserialize;
use serde_json::{Number, Value};
use std::path::{Path, PathBuf};
use tokio::{
    fs::{self, File},
    io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom},
    sync::mpsc,
    task::JoinSet,
};
use tracing::{Level, error, info, span};

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

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SourceConfig {
    File { path: PathBuf, read_full: bool },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TransformConfig {
    Filter {
        key: String,
        condition: TransformCondition,
        compare_to: toml::Value,
        codec: AcceptableCodecs,
    },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AcceptableCodecs {
    Json,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SinkConfig {
    Stdout,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TransformCondition {
    EqualTo,
    GreaterThanEqual,
    GreaterThan,
    LessThanEqual,
    LessThan,
    NotEqual,
    Matches,
}

#[derive(Debug)]
struct Envelope {
    payload: Vec<u8>,
}

fn toml_to_json_value(value: &toml::Value) -> Value {
    match value {
        toml::Value::String(value) => Value::String(value.clone()),
        toml::Value::Integer(value) => Value::Number(Number::from(*value)),
        toml::Value::Float(value) => Number::from_f64(*value)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        toml::Value::Boolean(value) => Value::Bool(*value),
        toml::Value::Datetime(value) => Value::String(value.to_string()),
        toml::Value::Array(values) => Value::Array(values.iter().map(toml_to_json_value).collect()),
        toml::Value::Table(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), toml_to_json_value(value)))
                .collect(),
        ),
    }
}

fn get_json_value_at_key<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    key.split('.').try_fold(value, |current, key_part| {
        current.as_object()?.get(key_part)
    })
}

fn values_equal(left: &Value, right: &Value) -> bool {
    match (left.as_f64(), right.as_f64()) {
        (Some(left), Some(right)) => left == right,
        _ => left == right,
    }
}

fn compare_ordered(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left.as_f64(), right.as_f64()) {
        (Some(left), Some(right)) => left.partial_cmp(&right),
        _ => match (left.as_str(), right.as_str()) {
            (Some(left), Some(right)) => Some(left.cmp(right)),
            _ => None,
        },
    }
}

fn condition_matches(
    data_value: &Value,
    condition: &TransformCondition,
    compare_to: &Value,
) -> bool {
    match condition {
        TransformCondition::EqualTo => values_equal(data_value, compare_to),
        TransformCondition::NotEqual => !values_equal(data_value, compare_to),
        TransformCondition::GreaterThanEqual => {
            compare_ordered(data_value, compare_to).is_some_and(|ordering| ordering.is_ge())
        }
        TransformCondition::GreaterThan => {
            compare_ordered(data_value, compare_to).is_some_and(|ordering| ordering.is_gt())
        }
        TransformCondition::LessThanEqual => {
            compare_ordered(data_value, compare_to).is_some_and(|ordering| ordering.is_le())
        }
        TransformCondition::LessThan => {
            compare_ordered(data_value, compare_to).is_some_and(|ordering| ordering.is_lt())
        }
        TransformCondition::Matches => match (data_value.as_str(), compare_to.as_str()) {
            (Some(data_value), Some(compare_to)) => data_value.contains(compare_to),
            _ => values_equal(data_value, compare_to),
        },
    }
}

async fn start_sink(sink: SinkConfig, receiver: &mut mpsc::Receiver<Envelope>) {
    while let Some(_message) = receiver.recv().await {
        match sink {
            SinkConfig::Stdout => {
                let mut stdout = tokio::io::stdout();
                stdout.write_all(&_message.payload).await.unwrap();
                stdout.flush().await.unwrap();
            }
        }
    }
}

fn event_touches_path(event: &Event, path: &Path) -> bool {
    event.paths.iter().any(|event_path| {
        event_path == path
            || event_path
                .file_name()
                .is_some_and(|file_name| Some(file_name) == path.file_name())
    })
}

fn is_file_change(kind: &EventKind) -> bool {
    kind.is_create() || kind.is_modify() || kind.is_remove()
}

async fn start_transform(
    transforms: Vec<TransformConfig>,
    sender: mpsc::Sender<Envelope>,
    receiver: &mut mpsc::Receiver<Envelope>,
) {
    while let Some(message) = receiver.recv().await {
        let data = message.payload;
        let envelope_data: Vec<u8> = data.clone();
        let mut should_send = true;
        for transform in &transforms {
            match transform {
                TransformConfig::Filter {
                    key,
                    condition,
                    compare_to,
                    codec,
                } => match codec {
                    AcceptableCodecs::Json => {
                        let json_data: Value = match serde_json::from_slice(data.as_slice()) {
                            Ok(value) => value,
                            Err(e) => {
                                error!(error = %e, "failed to parse json payload");
                                should_send = false;
                                break;
                            }
                        };
                        let compare_to = toml_to_json_value(compare_to);
                        should_send =
                            get_json_value_at_key(&json_data, key).is_some_and(|data_value| {
                                condition_matches(data_value, condition, &compare_to)
                            });
                    }
                },
            }

            if !should_send {
                break;
            }
        }

        if !should_send {
            continue;
        }

        let envelope = Envelope {
            payload: envelope_data,
        };

        sender.send(envelope).await.unwrap();
    }
}

async fn start_source(source: SourceConfig, sender: mpsc::Sender<Envelope>) {
    match source {
        SourceConfig::File { path, read_full } => {
            let (file_watcher_sender, mut file_watcher_receiver) = mpsc::channel(100);
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

                        if read_full && last_payload.as_ref().is_some_and(|last| last == &contents)
                        {
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
    }
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

    let (source_tx, mut source_rx) = mpsc::channel::<Envelope>(32);

    let mut set = JoinSet::new();

    let source = switchyard_config.source;
    let transforms = switchyard_config.transform;
    let sink = switchyard_config.sink;

    info!(pipeline_id, "starting source");
    set.spawn(async move { start_source(source, source_tx).await });

    match transforms {
        Some(transforms) => {
            let (transform_tx, mut transform_rx) = mpsc::channel::<Envelope>(32);

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
