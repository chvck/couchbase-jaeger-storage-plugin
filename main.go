package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/chvck/couchbase-jaeger-storage-plugin/setup"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/pkg/errors"

	"github.com/spf13/viper"
)

func main() {
	logger := hclog.New(&hclog.LoggerOptions{
		Level:      hclog.Warn,
		Name:       "jaeger-couchbase",
		JSONFormat: true,
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
	v.SetDefault(useAnalytics, true)
	v.SetDefault(n1qlFallback, true)

	if configPath != "" {
		err := v.ReadInConfig()
		if err != nil {
			logger.Error("failed to parse configuration file", "error", err)
			os.Exit(1)
		}
	}

	var options Options
	options.InitFromViper(v)

	store, err := NewCouchbaseStore(options, logger)
	if err != nil {
		logger.Error("failed to create couchbase store", "error", err)
		os.Exit(1)
	}

	serve(store, options, logger)
}

func serve(store Store, options Options, logger hclog.Logger) {
	splitConnStr := strings.Split(options.ConnStr, "://")
	var conn string
	if len(splitConnStr) > 1 {
		conn = splitConnStr[1]
	} else {
		conn = splitConnStr[0]
	}

	if options.AutoSetup {
		timeoutDuration := time.Duration(5 * time.Second)
		client := http.Client{
			Timeout: timeoutDuration,
		}

		err := setup.Run(conn, options.Username, options.Password, options.BucketName, client, logger)
		if err != nil {
			os.Exit(1)
		}
	}

	err := openBucket(store, options.BucketName, logger)
	if err != nil {
		logger.Error("failed to open bucket", "error", err)
		os.Exit(1)
	}
	if options.UseAnalytics {
		err := verifyAnalyticsSupported(conn)
		if err == nil {
			store.UseAnalytics(true)
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
	grpc.Serve(store)
}

func openBucket(store Store, bucketName string, logger hclog.Logger) error {
	timer := time.NewTimer(10 * time.Second)
	waitCh := make(chan struct{})
	go func() {
		for {
			err := store.Connect(bucketName)
			if err != nil {
				logger.Warn("error opening bucket", "reason", err)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			waitCh <- struct{}{}
			return
		}
	}()

	select {
	case <-timer.C:
		return errors.New("timed out trying to open bucket")
	case <-waitCh:
		timer.Stop()
		return nil
	}
}

func verifyServiceSupported(conn, port, endpoint string) error {
	timer := time.NewTimer(20 * time.Second)
	waitCh := make(chan error)
	go func() {
		for {
			resp, err := http.Get(fmt.Sprintf("http://%s:%s/%s", conn, port, endpoint))
			if err != nil {
				timer.Stop()
				waitCh <- err
				return
			}

			if resp.StatusCode != 200 {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			waitCh <- nil
			return
		}
	}()

	select {
	case <-timer.C:
		return errors.New("timed out waiting for service")
	case err := <-waitCh:
		timer.Stop()
		return err
	}
}

func verifyAnalyticsSupported(connStr string) error {
	return verifyServiceSupported(connStr, "8091", "_p/cbas-admin/admin/ping")
}

func verifyN1QLSupported(connStr string) error {
	return verifyServiceSupported(connStr, "8091", "_p/query/admin/ping")
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
