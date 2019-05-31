package main

import (
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/pkg/errors"
	"gopkg.in/couchbase/gocb.v1"
)

type Store interface {
	UseAnalytics(use bool)
	Connect(bucketName string) error
	Query(query string, params interface{}) (Result, error)
	Insert(key string, value interface{}, expiry int) error
	Name() string
	SpanReader() spanstore.Reader
	SpanWriter() spanstore.Writer
	DependencyReader() dependencystore.Reader
}

type Result interface {
	Next(valuePtr interface{}) bool
	Close() error
}

type couchbaseStore struct {
	bucket       *gocb.Bucket
	cluster      *gocb.Cluster
	useAnalytics bool
	logger       hclog.Logger
}

func NewCouchbaseStore(options Options, logger hclog.Logger) (*couchbaseStore, error) {
	cluster, err := gocb.Connect(options.ConnStr)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create cluster")
	}

	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: options.Username,
		Password: options.Password,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to authenticate")
	}

	return &couchbaseStore{
		cluster: cluster,
		logger:  logger,
	}, nil
}

func (cs *couchbaseStore) UseAnalytics(use bool) {
	cs.useAnalytics = use
}

func (cs *couchbaseStore) Connect(bucketName string) error {
	bucket, err := cs.cluster.OpenBucket(bucketName, "")
	if err != nil {
		return err
	}

	cs.bucket = bucket
	return nil
}

func (cs *couchbaseStore) Query(queryString string, params interface{}) (Result, error) {
	var result Result
	var err error
	if cs.useAnalytics {
		query := gocb.NewAnalyticsQuery(queryString)
		result, err = cs.bucket.ExecuteAnalyticsQuery(query, params)
	} else {
		query := gocb.NewN1qlQuery(queryString)
		result, err = cs.bucket.ExecuteN1qlQuery(query, params)
	}
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
