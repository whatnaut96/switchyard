use std::{
    fs::File,
    io::{self, Read},
    path::PathBuf,
};

use clap::Parser;
use serde::Deserialize;

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
    transform: Vec<TransformConfig>,
    sink: SinkConfig,
}

#[derive(Debug, Deserialize)]
struct PipelineData {
    id: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SourceConfig {
    File { path: PathBuf },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TransformConfig {
    Filter {
        key: String,
        val: String,
        condition: TransformCondition,
        compare_to: toml::Value,
    },
    ToJson,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SinkConfig {
    Http {
        uri: String,
        encoding: HttpSinkEncoding,
    },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum HttpSinkEncoding {
    Json,
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

fn main() -> io::Result<()> {
    let args = Args::parse();
    let mut config_file = File::open(args.file)?;
    let mut config_contents = String::new();
    config_file.read_to_string(&mut config_contents)?;
    let switchyard_config: SwitchyardConfig = match toml::from_str(&config_contents) {
        Ok(v) => v,
        Err(e) => {
            panic!("could not deserialize config: {e}")
        }
    };
    println!("Hello, world!");
    println!("{:?}", switchyard_config);
    Ok(())
}
