use crate::Envelope;
use mlua::{Function, Lua, Result, Table};
use serde::Deserialize;
use serde_json::{Number, Value};
use std::path::PathBuf;
use tokio::{
    fs,
    sync::mpsc::{Receiver, Sender},
};
use tracing::{debug, error};

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TransformConfig {
    Filter {
        key: String,
        condition: TransformCondition,
        compare_to: toml::Value,
        codec: AcceptableCodecs,
    },
    Lua {
        path: PathBuf,
    },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AcceptableCodecs {
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

pub async fn start_transform(
    transforms: Vec<TransformConfig>,
    sender: Sender<Envelope>,
    receiver: &mut Receiver<Envelope>,
) {
    while let Some(message) = receiver.recv().await {
        let data = message.payload;
        let mut envelope_data: Vec<u8> = data.clone();
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
                TransformConfig::Lua { path } => {
                    let path_display = path.display();
                    let script = match fs::read_to_string(path).await {
                        Ok(contents) => contents,
                        Err(e) => {
                            error!(error = %e, custom_script_path = %path_display, "could not read the custom lua script");
                            should_send = false;
                            break;
                        }
                    };
                    let lua = Lua::new();
                    let globals = lua.globals();
                    match mlua_json::preload(&lua) {
                        Ok(_) => {}
                        Err(e) => {
                            panic!(
                                "COULD NOT LOAD LUA JSON MODULE. SWITCHYARD CANNOT CONTINUE IN THIS STATE: {e}"
                            );
                        }
                    };

                    let script_name = match path.clone().into_os_string().into_string() {
                        Ok(s) => s,
                        Err(_) => {
                            error!(custom_script_path = %path_display, "could not convert the path of the script to a string");
                            should_send = false;
                            break;
                        }
                    };

                    if let Err(e) = lua.load(&script).set_name(script_name).exec() {
                        error!(error = %e, custom_script_path = %path_display, "could not parse or execute the custom lua transform");
                        should_send = false;
                        break;
                    }

                    let lua_input_string = match lua.create_string(&envelope_data) {
                        Ok(s) => s,
                        Err(e) => {
                            error!(error = %e, custom_script_path = %path_display, "could not create a custom lua string for the payload");
                            should_send = false;
                            break;
                        }
                    };

                    let lua_transform_function: Function = match globals.get("lua_transform") {
                        Ok(f) => f,
                        Err(e) => {
                            error!(error = %e, custom_script_path = %path_display, "the custom script does not have a function called lua_transform");
                            should_send = false;
                            break;
                        }
                    };

                    let lua_transform_return_table: Table = match lua_transform_function
                        .call(lua_input_string)
                    {
                        Ok(t) => t,
                        Err(e) => {
                            error!(error = %e, custom_script_path = %path_display, "the custom transform function's return value could not be read as a table");
                            should_send = false;
                            break;
                        }
                    };

                    let values = match lua_transform_return_table
                        .sequence_values::<u8>()
                        .collect::<Result<Vec<_>>>()
                    {
                        Ok(v) => v,
                        Err(e) => {
                            error!(error = %e, custom_script_path = %path_display, "could not convert the table to a vector of bytes");
                            should_send = false;
                            break;
                        }
                    };

                    envelope_data = values;
                    should_send = !envelope_data.is_empty();
                }
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

        let vol = envelope.payload.len();
        match sender.send(envelope).await {
            Ok(_) => {
                debug!(segment = "transform", "sent {vol} bytes to channel");
            }
            Err(e) => {
                error!(segment = "transform", error = %e, "could not send data to the receiver");
            }
        }
    }
}
