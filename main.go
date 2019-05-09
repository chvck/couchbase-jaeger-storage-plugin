package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
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
	bucket       *gocb.Bucket
	cluster      *gocb.Cluster
	useAnalytics bool
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

	var auth gocb.Authenticator
	if options.UseCertAuth {
		auth = gocb.CertAuthenticator{}
	} else {
		auth = gocb.PasswordAuthenticator{
			Username: options.Username,
			Password: options.Password,
		}
	}

	err = cluster.Authenticate(auth)
	if err != nil {
		log.Fatal(err)
	}

	bucket, err := cluster.OpenBucket(options.BucketName, "")
	if err != nil {
		log.Fatal(err)
	}

	populateQueries(options.BucketName)

	store := couchbaseStore{bucket: bucket}

	if options.UseAnalytics {
		err := verifyAnalyticsSupported(options.ConnStr)
		if err == nil {
			store.useAnalytics = true
		} else {
			if options.UseN1QLFallback {
				err := verifyN1QLSupported(options.ConnStr)
				if err != nil {
					log.Fatal("Neither analytics or N1QL available")
				}
			} else {
				log.Fatal("Analytics not available")
			}
		}
	} else {
		err := verifyN1QLSupported(options.ConnStr)
		if err != nil {
			log.Fatal("N1QL not available")
		}
	}

	grpc.Serve(&store)
}

func verifyServiceSupported(connStr, port, endpoint string) error {
	splitConnStr := strings.Split(connStr, "://")
	var conn string
	if len(splitConnStr) > 1 {
		conn = splitConnStr[1]
	} else {
		conn = splitConnStr[0]
	}
	_, err := http.Get(fmt.Sprintf("http://%s:%s/%s", conn, port, endpoint))
	if err != nil {
		return err
	}

	// the service exists so let's assume it's supported
	return nil
}

func verifyAnalyticsSupported(connStr string) error {
	return verifyServiceSupported(connStr, "8095", "/analytics/config/node")
}

func verifyN1QLSupported(connStr string) error {
	return verifyServiceSupported(connStr, "8093", "/query/service")
}

func populateQueries(bucketName string) {
	querySpanByTraceID = fmt.Sprintf(querySpanByTraceID, bucketName)
	queryServiceNames = fmt.Sprintf(queryServiceNames, bucketName)
	queryOperationNames = fmt.Sprintf(queryOperationNames, bucketName)
	queryIDsByTag = fmt.Sprintf(queryIDsByTag, bucketName)
	queryIDsByServiceName = fmt.Sprintf(queryIDsByServiceName, bucketName)
	queryIDsByServiceAndOperationName = fmt.Sprintf(queryIDsByServiceAndOperationName, bucketName)
	queryIDsByServiceAndOperationNameAndTags = fmt.Sprintf(queryIDsByServiceAndOperationNameAndTags, bucketName)
	queryIDsByDuration = fmt.Sprintf(queryIDsByDuration, bucketName)

	depsSelectStmt = fmt.Sprintf(depsSelectStmt, bucketName)
}
