package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"gopkg.in/couchbase/gocb.v1"

	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/spf13/viper"
)

type Store interface {
	Query(query string, params interface{}) (Result, error)
	Insert(key string, value interface{}, expiry int) error
	Name() string
}

type Result interface {
	Next(valuePtr interface{}) bool
	Close() error
}

type couchbaseStore struct {
	bucket  *gocb.Bucket
	cluster *gocb.Cluster
}

func (cs *couchbaseStore) Query(queryString string, params interface{}) (Result, error) {
	query := gocb.NewAnalyticsQuery(queryString)
	result, err := cs.bucket.ExecuteAnalyticsQuery(query, params)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (cs *couchbaseStore) Insert(key string, value interface{}, expiry int) error {
	_, err := cs.bucket.Insert(key, value, 0)

	return err
}

func (cs *couchbaseStore) Name() string {
	return cs.bucket.Name()
}

func (cs *couchbaseStore) SpanReader() spanstore.Reader {
	return &couchbaseSpanReader{
		store: cs,
	}
}

func (cs *couchbaseStore) SpanWriter() spanstore.Writer {
	return &couchbaseSpanWriter{
		store: cs,
	}
}

func (cs *couchbaseStore) DependencyReader() dependencystore.Reader {
	return &couchbaseDependencyReader{
		store: cs,
	}
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "A path to the plugin's configuration file")
	flag.Parse()

	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	if configPath != "" {
		v.SetConfigFile(configPath)
	}

	v.SetDefault(bucketName, "default")
	v.SetDefault(connStr, "couchbase://localhost")

	if configPath != "" {
		err := v.ReadInConfig()
		if err != nil {
			log.Fatal(err)
		}
	}

	var options Options
	options.InitFromViper(v)

	gocb.SetLogger(gocb.VerboseStdioLogger())
	cluster, err := gocb.Connect(options.ConnStr)
	if err != nil {
		log.Fatal(err)
	}

	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: options.Username,
		Password: options.Password,
	})
	if err != nil {
		log.Fatal(err)
	}

	bucket, err := cluster.OpenBucket(options.BucketName, "")
	if err != nil {
		log.Fatal(err)
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
