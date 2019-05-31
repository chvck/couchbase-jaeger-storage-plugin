Couchbase Jaeger Storage Plugin
===============================

This plugin allows the reading and writing of spans, and the reading of dependencies to/from Jaeger over gRPC.

> WARNING: This plugin is still under development.

Usage
-----
To use this plugin you must first build (`go build`) and then create a `config.yaml` file based off of the example file.
Once complete you need to run a version of Jaeger that supports gRPC plugins. 


There are two command line flags that can be used to configure the plugin. The first is 
`--grpc-storage-plugin.binary` which is required and is the path to the plugin **binary**. The second is 
`--grpc-storage-plugin.configuration-file` which is optional and is the path to the configuration file:

```
./all-in-one --grpc-storage-plugin.binary=/path/to/my/plugin --grpc-storage-plugin.configuration-file=/path/to/my/config
```

The config file values can also be set as environment variables in the shell in which Jaeger is run, see `Dockerfile` for
example usage.


Docker
------
To use Docker with this plugin for evaluation purposes requires the plugin be built for a Linux target with the name
`couchbase-jaeger-storage-plugin-linux`, or just run `make buildlinux`. It also currently requires a custom image of Jaeger
to be built with the tag `jaeger:1.12`, the custom built image requires Jaeger to support gRPC plugins. 
Once the binary and Jaeger image are created you can just use `docker build` and `docker run` as usual. This will spin up
Jaeger using the plugin for storage. 
