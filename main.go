package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"gopkg.in/couchbase/gocb.v1"

	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/spf13/viper"
)

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

	splitConnStr := strings.Split(connStr, "://")
	var conn string
	if len(splitConnStr) > 1 {
		conn = splitConnStr[1]
	} else {
		conn = splitConnStr[0]
	}

	if options.AutoSetup {
		runSetup(conn, options.Username, options.Password, options.BucketName)
	}

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

	populateQueries(options.BucketName)

	store := couchbaseStore{bucket: bucket}

	if options.UseAnalytics {
		err := verifyAnalyticsSupported(conn)
		if err == nil {
			store.useAnalytics = true
		} else {
			if options.UseN1QLFallback {
				err := verifyN1QLSupported(conn)
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

func runSetup(server, username, password, bucket string) {
	timeout, err := runRequestWithTimeout("GET", fmt.Sprintf("http://%s:8091", server),
		"", "", 25*time.Second, nil)
	if err != nil {
		panic(err)
	}
	if timeout {
		panic("timed out waiting for cluster to come online")
	}

	b, err := json.Marshal(map[string]int{
		"memoryQuota":      512,
		"indexMemoryQuota": 512,
	})
	if err != nil {
		panic(err)
	}
	timeout, err = runRequestWithTimeout("POST", fmt.Sprintf("http://%s:8091/pools/default", server),
		"", "", 1*time.Second, bytes.NewBuffer(b))
	if err != nil {
		panic(err)
	}
	if timeout {
		panic("timed out waiting for cluster to come online")
	}

	b, err = json.Marshal(map[string]string{
		"services": "kv,cbas,index",
	})
	if err != nil {
		panic(err)
	}
	timeout, err = runRequestWithTimeout("POST", fmt.Sprintf("http://%s:8091/node/controller/setupService", server),
		"", "", 1*time.Second, bytes.NewBuffer(b))
	if err != nil {
		panic(err)
	}
	if timeout {
		panic("timed out waiting for cluster to come online")
	}

	b, err = json.Marshal(map[string]interface{}{
		"port":     8091,
		"password": "password",
	})
	if err != nil {
		panic(err)
	}
	timeout, err = runRequestWithTimeout("POST", fmt.Sprintf("http://%s:8091/settings/web", server),
		"", "", 1*time.Second, bytes.NewBuffer(b))
	if err != nil {
		panic(err)
	}
	if timeout {
		panic("timed out waiting for cluster to come online")
	}

	b, err = json.Marshal(map[string]interface{}{
		"name":          bucket,
		"ramQuotaMB":    512,
		"authType":      "none",
		"replicaNumber": 0,
	})
	if err != nil {
		panic(err)
	}
	timeout, err = runRequestWithTimeout("POST", fmt.Sprintf("http://%s:8091/pools/default/buckets", server),
		username, password, 1*time.Second, bytes.NewBuffer(b))
	if err != nil {
		panic(err)
	}
	if timeout {
		panic("timed out waiting for cluster to come online")
	}
}

func runRequestWithTimeout(method, url, username, password string, timeout time.Duration, body io.Reader) (bool, error) {
	timeoutCh := time.NewTimer(timeout)
	doneCh := make(chan error)
	go func() {
		req, err := http.NewRequest(method, url, body)
		if err != nil {
			doneCh <- err
			return
		}
		if username != "" || password != "" {
			req.SetBasicAuth(username, password)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			doneCh <- err
			return
		}

		if resp.StatusCode == 200 {
			doneCh <- nil
			return
		}

		time.Sleep(1 * time.Second)
	}()

	select {
	case <-timeoutCh.C:
		return true, nil
	case err := <-doneCh:
		if err != nil {
			return false, err
		}
	}

	return false, nil
}
