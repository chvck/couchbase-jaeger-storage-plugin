package main

import (
	"flag"
	"fmt"

	"github.com/prometheus/common/log"

	"gopkg.in/couchbase/gocb.v1"

	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/spf13/viper"
)

type couchbaseStore struct {
	bucket *gocb.Bucket
}

func (cs *couchbaseStore) SpanReader() spanstore.Reader {
	return &couchbaseSpanReader{
		bucket: cs.bucket,
	}
}

func (cs *couchbaseStore) SpanWriter() spanstore.Writer {
	return &couchbaseSpanWriter{
		bucket: cs.bucket,
	}
}

func (cs *couchbaseStore) DependencyReader() dependencystore.Reader {
	return &couchbaseDependencyReader{
		bucket: cs.bucket,
	}
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "A path to the plugin's configuration file")
	flag.Parse()

	v := viper.New()
	if configPath != "" {
		v.SetConfigFile(configPath)
	}

	v.SetDefault(bucketName, "default")
	v.SetDefault(connStr, "couchbase://localhost")

	if configPath != "" {
		err := v.ReadInConfig()
		if err != nil {
			log.Error(err)
			panic(err)
		}
	}

	var options Options
	options.InitFromViper(v)

	gocb.SetLogger(gocb.VerboseStdioLogger())
	cluster, err := gocb.Connect(options.ConnStr)
	if err != nil {
		log.Error(err)
		panic(err)
	}

	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: options.Username,
		Password: options.Password,
	})
	if err != nil {
		log.Error(err)
		panic(err)
	}

	bucket, err := cluster.OpenBucket(options.BucketName, "")
	if err != nil {
		log.Error(err)
		panic(err)
	}

	store := couchbaseStore{bucket: bucket}

	querySpanByTraceID = fmt.Sprintf(querySpanByTraceID, options.BucketName)
	queryServiceNames = fmt.Sprintf(queryServiceNames, options.BucketName)
	queryOperationNames = fmt.Sprintf(queryOperationNames, options.BucketName)
	queryIDsByTag = fmt.Sprintf(queryIDsByTag, options.BucketName)
	queryIDsByServiceName = fmt.Sprintf(queryIDsByServiceName, options.BucketName)
	queryIDsByServiceAndOperationName = fmt.Sprintf(queryIDsByServiceAndOperationName, options.BucketName)
	queryIDsByServiceAndOperationNameAndTags = fmt.Sprintf(queryIDsByServiceAndOperationNameAndTags, options.BucketName)
	queryIDsByDuration = fmt.Sprintf(queryIDsByDuration, options.BucketName)

	depsSelectStmt = fmt.Sprintf(depsSelectStmt, options.BucketName)

	grpc.Serve(&store)
}
