package main

import (
	"flag"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/chvck/couchbase-jaeger-storage-plugin/setup"

	"github.com/jaegertracing/jaeger/plugin/storage/grpc"

	"github.com/chvck/couchbase-jaeger-storage-plugin/options"
	"github.com/chvck/couchbase-jaeger-storage-plugin/plugin"

	"github.com/hashicorp/go-hclog"

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

	if configPath != "" {
		err := v.ReadInConfig()
		if err != nil {
			logger.Error("failed to parse configuration file", "error", err)
			os.Exit(1)
		}
	}

	var options options.Options
	options.InitFromViper(v)

	store, err := plugin.NewCouchbaseStore(options, logger)
	if err != nil {
		logger.Error("failed to create couchbase store", "error", err)
		os.Exit(1)
	}

	timeoutDuration := time.Duration(5 * time.Second)
	cli := &http.Client{
		Timeout: timeoutDuration,
	}

	splitConnStr := strings.Split(options.ConnStr, "://")
	var conn string
	if len(splitConnStr) > 1 {
		conn = splitConnStr[1]
	} else {
		conn = splitConnStr[0]
	}

	if options.AutoSetup {
		err := setup.Run(options, conn, cli, logger)
		if err != nil {
			logger.Error("failed to run setup", "error", err)
			os.Exit(1)
		}
	}

	err = plugin.OpenBucket(store, options.BucketName, logger)
	if err != nil {
		logger.Error("failed to open bucket", "error", err)
		os.Exit(1)
	}

	err = plugin.VerifyServices(options, cli, conn, store, logger)
	if err != nil {
		logger.Error("failed to verify services", "error", err)
		os.Exit(1)
	}

	grpc.Serve(store)
}
