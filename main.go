package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/chvck/couchbase-jaeger-storage-plugin/setup"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"

	"github.com/hashicorp/go-hclog"

	"gopkg.in/couchbase/gocb.v1"

	"github.com/spf13/viper"
)

func main() {
	logger := hclog.New(&hclog.LoggerOptions{
		Level:      hclog.Warn,
		Output:     os.Stdout,
		JSONFormat: true,
		Name:       "jaeger-couchbase",
	})

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
			logger.Error("failed to parse configuration file", "error", err)
			os.Exit(1)
		}
	}

	var options Options
	options.InitFromViper(v)

	splitConnStr := strings.Split(options.ConnStr, "://")
	var conn string
	if len(splitConnStr) > 1 {
		conn = splitConnStr[1]
	} else {
		conn = splitConnStr[0]
	}

	if options.AutoSetup {
		timeoutDuration := time.Duration(1 * time.Second)
		client := http.Client{
			Timeout: timeoutDuration,
		}

		err := setup.Run(conn, options.Username, options.Password, options.BucketName, client)
		if err != nil {
			logger.Error("Failed to setup cluster", "error", err)
			os.Exit(1)
		}
	}

	cluster, err := gocb.Connect(options.ConnStr)
	if err != nil {
		logger.Error("failed to create cluster", "error", err)
		os.Exit(1)
	}

	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: options.Username,
		Password: options.Password,
	})
	if err != nil {
		logger.Error("failed to authenticate", "error", err)
		os.Exit(1)
	}

	bucket, err := cluster.OpenBucket(options.BucketName, "")
	if err != nil {
		logger.Error("failed to open bucket", "error", err)
		os.Exit(1)
	}

	var useAnalytics bool
	if options.UseAnalytics {
		err := verifyAnalyticsSupported(conn)
		if err == nil {
			useAnalytics = true
		} else {
			if options.UseN1QLFallback {
				err := verifyN1QLSupported(conn)
				if err != nil {
					logger.Error("failed to verify n1ql supported", "error", err)
					os.Exit(1)
				}
			} else {
				logger.Error("failed to verify analytics supported", "error", err)
				os.Exit(1)
			}
		}
	} else {
		err := verifyN1QLSupported(options.ConnStr)
		if err != nil {
			logger.Error("failed to verify n1ql supported", "error", err)
			os.Exit(1)
		}
	}

	populateQueries(options.BucketName)

	store := couchbaseStore{
		logger:       logger,
		bucket:       bucket,
		useAnalytics: useAnalytics,
	}

	grpc.Serve(&store)
}

func verifyServiceSupported(conn, port, endpoint string) error {
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
