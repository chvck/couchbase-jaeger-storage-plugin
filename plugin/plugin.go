package plugin

import (
	"fmt"
	"net/http"
	"time"

	"github.com/chvck/couchbase-jaeger-storage-plugin/httpclient"
	"github.com/chvck/couchbase-jaeger-storage-plugin/options"
	"github.com/hashicorp/go-hclog"
	"github.com/pkg/errors"
)

func VerifyServices(opts options.Options, httpClient httpclient.Client, conn string, store *couchbaseStore, logger hclog.Logger) error {
	if opts.UseAnalytics {
		err := VerifyAnalyticsSupported(httpClient, conn, logger)
		if err == nil {
			store.UseAnalytics(true)
		} else {
			if opts.UseN1QLFallback {
				err := verifyN1QLSupported(httpClient, conn, logger)
				if err != nil {
					return errors.Wrap(err, "failed to verify n1ql supported")
				}
			} else {
				return errors.Wrap(err, "failed to verify analytics supported")
			}
		}
	} else {
		err := verifyN1QLSupported(httpClient, conn, logger)
		if err != nil {
			return errors.Wrap(err, "failed to verify n1ql supported")
		}
	}

	return nil
}

func OpenBucket(store Store, bucketName string, logger hclog.Logger) error {
	timer := time.NewTimer(10 * time.Second)
	waitCh := make(chan struct{})
	go func() {
		for {
			err := store.Connect(bucketName)
			if err != nil {
				logger.Warn("error opening bucket, retrying", "reason", err)
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
		populateQueries(bucketName)
		return nil
	}
}

func verifyServiceSupported(client httpclient.Client, conn, port, endpoint string, logger hclog.Logger) error {
	timer := time.NewTimer(20 * time.Second)
	waitCh := make(chan error)
	go func() {
		for {
			req, err := http.NewRequest(
				"GET",
				fmt.Sprintf("http://%s:%s/%s", conn, port, endpoint),
				nil,
			)
			if err != nil {
				waitCh <- err
				return
			}

			resp, err := client.Do(req)
			if err != nil {
				waitCh <- err
				return
			}

			if resp.StatusCode != 200 {
				logger.Warn("Retrying verification that service is supported")
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

func VerifyAnalyticsSupported(client httpclient.Client, connStr string, logger hclog.Logger) error {
	return verifyServiceSupported(client, connStr, "8091", "_p/cbas-admin/admin/ping", logger)
}

func verifyN1QLSupported(client httpclient.Client, connStr string, logger hclog.Logger) error {
	return verifyServiceSupported(client, connStr, "8091", "_p/query/admin/ping", logger)
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
