Couchbase Jaeger Storage Plugin
===============================

This plugin allows the reading and writing of spans, and the reading of dependencies to/from Jaeger over gRPC.

> WARNING: This plugin is still under development.

Usage
-----
To use this plugin you must first build (`go build`) and then create a `config.yaml` file based off of the example file.
Once complete you need to run a version of Jaeger that supports gRPC plugins.


1. Clone this repository and build the plugin. You'll need a version of Go which supports modules and have modules enabled:
    * `export GO111MODULE=on`
    * `go build`
    * `cp config.yaml.example config.yaml`
    * Update `config.yaml` with your own values.

2. At time of writing Jaeger does not have a released version with storage plugin gRPC support so it's easiest to checkout
and build with latest. (It's also easiest to use `all-in-one` for testing).
    * `git clone https://github.com/jaegertracing/jaeger`
    * `cd https://github.com/jaegertracing/jaeger/cmd/all-in-one`
    * `go build`

3. Run it! When running Jaeger there are two command line flags that can be used to configure the plugin. 
    * `--grpc-storage-plugin.binary` is required and is the path to the plugin **binary**.
    * `--grpc-storage-plugin.configuration-file` is optional and is the path to the configuration file.

```
./all-in-one --grpc-storage-plugin.binary=/path/to/my/plugin --grpc-storage-plugin.configuration-file=/path/to/my/config
```

Note: This plugin supports setting any config file values can also be as environment variables in the shell in which Jaeger 
is run, see `Dockerfile` for example usage of this.


Docker
------
To use Docker with this plugin for evaluation purposes requires the plugin be built for a Linux target with the name
`couchbase-jaeger-storage-plugin-linux`, or just run `make buildlinux`. It also currently requires a custom image of Jaeger
to be built with the tag `jaeger:1.12`, the custom built image requires Jaeger to support gRPC plugins. 
Once the binary and Jaeger image are created you can just use `docker build` and `docker run` as usual. This will spin up
Jaeger using the plugin for storage.

You can also use `docker compose` to run Couchbase Server Enterprise Edition and Jaeger (setup to run this plugin) together,
this should be used only for evaluation purposes. See the [Couchbase blog](https://blog.couchbase.com/couchbase-server-editions-explained-open-source-community-edition-and-enterprise-edition/)
for information on licensing.


Configuration
--------------
There are several configuration options that can be used for setting up the plugin. These can be set within the `config.yaml`
file provided to Jaeger at runtime, or they can be set via environment variables set in the same shell as the Jaeger runtime.

| Config file | Environment | Description |
|---|---|---|
| bucket | COUCHBASE_BUCKET | The name of the bucket to use. |
| username | COUCHBASE_USERNAME | The username to use for authentication. |
| password | COUCHBASE_PASSWORD | The password to use for authentication. |
| connString | COUCHBASE_CONNSTRING | The connection string to use for connecting to Couchbase Server (e.g. `couchbase://localhost`). |
| useAnalytics | COUCHBASE_USEANALYTICS | Sets whether or not to use Analytics for queries (note: this an Enterprise Edition feature). The plugin expects a dataset with the same as the bucket to be setup. |
| n1qlFallback | COUCHBASE_N1QLFALLBACK | If the analytics engine cannot be reached at start up then fallback to using N1QL for queries. The plugin expects at least a primary index to exist on the bucket. |
| autoSetup | COUCHBASE_AUTOSETUP | This is primarily aimed at `docker compose` support. If set then the plugin will expect an uninitialized Couchbase Server cluster and will attempt to set it up and enable querying through analytics. |





License
--------
Copyright 2019 Couchbase Inc.

Licensed under the Apache License, Version 2.0.
