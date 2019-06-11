Couchbase Jaeger Storage Plugin
===============================

This plugin allows the reading and writing of spans, and the reading of dependencies to/from Jaeger over gRPC.

> WARNING: This plugin is still under development.

Couchbase Setup
---------------
In order to use this plugin with Couchbase it is required that either Couchbase Analytics or N1QL is setup and running
on the server. In the Analytics case it is required that a dataset with the same of the bucket provided to the plugin
is already created. In the N1QL case a primary index (at the very least) should be created.

> Further advice on indexes will be provided in time.

Docker
------
The plugin contains support for a `Dockerfile`, providing an image based upon the Jaeger [all-in-one](https://hub.docker.com/r/jaegertracing/all-in-one)
image. To build and run this image:

1. Build the plugin for a Linux target with the name `couchbase-jaeger-storage-plugin-linux`.
    * Run `make buildlinux` from any platform supporting make. 
    * If you do not have make then you can use `export GOOS=linux; go build -o couchbase-jaeger-storage-plugin-linux`.

2. Update the `Dockerfile` with your own values or set the environment variables (covered later) and build the image: `docker build . -t couchbase-jaeger-storage-plugin`.

3. Run it! 
    ```
    docker run -d --name couchbase-jaeger-storage-plugin \
      -e COLLECTOR_ZIPKIN_HTTP_PORT=9411 \
      -e COUCHBASE_USERNAME="username" \
      -e COUCHBASE_PASSWORD="password" \
      -p 5775:5775/udp \
      -p 6831:6831/udp \
      -p 6832:6832/udp \
      -p 5778:5778 \
      -p 16686:16686 \
      -p 14268:14268 \
      -p 9411:9411 \
      couchbase-jaeger-storage-plugin
    ```
    * See the [Jaeger docs](https://www.jaegertracing.io/docs/1.13/getting-started/) for information on these ports and general Jaeger Docker usage.

Docker Compose
--------------
You can also use `docker compose` to run Couchbase Server Enterprise Edition and Jaeger (set up to run this plugin) together,

> The docker-compose setup provided should be used __only__ for experimentation purposes. See the [Couchbase blog](https://blog.couchbase.com/couchbase-server-editions-explained-open-source-community-edition-and-enterprise-edition/)
for information on licensing.
 
 Running with `docker-compose` will automatically setup a Couchbase Server single node cluster for you with some basic default values, and analytics support.
The Couchbase UI will be accessible on http://localhost:8091, and the Jaeger UI will be accessible on http://localhost:16686.

```
docker-compose build
docker-compose up
```


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


Building
--------
To use this plugin without Docker you must first build (`go build`) and then create a `config.yaml` file based upon the example file.
Once complete you need to run a version of Jaeger that supports gRPC plugins.


1. Clone this repository and build the plugin. You'll need a version of Go which supports modules, and have modules enabled:
    ```
    export GO111MODULE=on
    go build
    cp config.yaml.example config.yaml
    ```
    
    * Update `config.yaml` with your own values.

2. Jaeger supports storage plugin gRPC as of version 1.12. (It's easiest to use `all-in-one` for testing).
    ```
    git clone https://github.com/jaegertracing/jaeger
    cd https://github.com/jaegertracing/jaeger/cmd/all-in-one
    git checkout v1.12.0
    go build
    ```

3. Run it! When running Jaeger there are two command line flags that can be used to configure the plugin. 
    * `--grpc-storage-plugin.binary` is required and is the path to the plugin **binary**.
    * `--grpc-storage-plugin.configuration-file` is optional and is the path to the configuration file.

```
./all-in-one --grpc-storage-plugin.binary=/path/to/my/plugin --grpc-storage-plugin.configuration-file=/path/to/my/config
```

Note: This plugin supports setting any config file values can also be as environment variables in the shell in which Jaeger 
is run, see `Dockerfile` for example usage of this.

License
--------
Copyright 2019 Couchbase Inc.

Licensed under the Apache License, Version 2.0.
