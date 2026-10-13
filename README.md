# Switchyard

Switchyard is a small configurable data pipeline engine. It reads data from a source, optionally applies transforms, and writes the result to a sink.

The project is early-stage. Embedded Lua transforms are in development while the main focus is adding more sources and sinks.

## Current shape

- Sources: file
- Transforms: JSON filter, embedded Lua experimental
- Sinks: stdout
- Config: TOML

## Sample config

```toml
[pipeline]
id = "pipeline1"

[source]
type = "file"
path = "/home/khill/Documents/Repositories/switchyard/test_logs/Status.json"
read_full = true

# [[transform]]
# type = "filter"
# key = "event"
# condition = "equal_to"
# compare_to = "Status"
# codec = "json"

[sink]
type = "stdout"
```

Run it with:

```bash
cargo run -- --file config.toml
```

## Goal

Switchyard is intended to make simple integration pipelines reusable without writing a new service for each source-to-sink path.
