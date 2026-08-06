# CLI

```text
loadr run          run locally or submit to a controller
loadr validate     validate YAML
loadr controller   serve coordination, API, UI, and fleet metrics
loadr agent        join a controller and generate load
loadr plugin       manage local native feeders
loadr schema       print the JSON Schema
loadr completions  generate shell completions
loadr version      print build version and revision
```

Common commands:

```bash
loadr validate test.yaml
loadr run --ui test.yaml
loadr run --output prometheus=127.0.0.1:9091 test.yaml
loadr controller --bind 0.0.0.0:7625
loadr agent --join controller:7625 --plugins-dir /opt/loadr/feeders
loadr run --controller controller:6464 test.yaml
```

The plugin registry is local-only. `loadr plugin install DIR` copies a
directory containing `plugin.toml` and its native library into the selected
plugin root.
