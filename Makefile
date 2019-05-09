buildlinux:
	export GOOS=linux; go build -o couchbase-jaeger-storage-plugin-linux
    export GOOS=""

.PHONY: buildlinux
