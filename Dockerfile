FROM jaeger:1.12

ENV SPAN_STORAGE_TYPE=grpc-plugin
ENV COUCHBASE_USERNAME=""
ENV COUCHBASE_PASSWORD=""
ENV COUCHBASE_BUCKET="default"
ENV COUCHBASE_CONNSTRING="couchbase://host.docker.internal"
ENV GRPC_STORAGE_PLUGIN_BINARY="/go/bin/couchbase-jaeger-storage-plugin-linux"

# This is such a hack
COPY ./tmp /tmp

COPY ./couchbase-jaeger-storage-plugin-linux /go/bin/
