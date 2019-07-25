FROM jaegertracing/all-in-one:1.13

ENV SPAN_STORAGE_TYPE=grpc-plugin
ENV COUCHBASE_USERNAME=""
ENV COUCHBASE_PASSWORD=""
ENV COUCHBASE_BUCKET="default"
ENV COUCHBASE_CONNSTRING="couchbase://host.docker.internal"
ENV COUCHBASE_USEANALYTICS=true
ENV COUCHBASE_N1QLFALLBACK=true
ENV COUCHBASE_AUTOSETUP=false
ENV GRPC_STORAGE_PLUGIN_BINARY="/go/bin/couchbase-jaeger-storage-plugin-linux"
ENV GRPC_VERBOSITY=DEBUG
ENV GRPC_TRACE=all

# This is such a hack, necessary because the base scratch dockerfile is very minimal.
# Go-plugin tries to write to /tmp but /tmp doesn't exist in scratch, neither does the
# mkdir command so we can't just use that.
COPY ./tmp /tmp

COPY ./couchbase-jaeger-storage-plugin-linux /go/bin/
