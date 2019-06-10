package setup

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"github.com/chvck/couchbase-jaeger-storage-plugin/options"
	"github.com/chvck/couchbase-jaeger-storage-plugin/plugin"

	"github.com/chvck/couchbase-jaeger-storage-plugin/httpclient"

	"github.com/hashicorp/go-hclog"

	"github.com/pkg/errors"
)

func Run(opts options.Options, conn string, client httpclient.Client, logger hclog.Logger) error {
	err := waitForCluster(conn, 45*time.Second, client, logger)
	if err != nil {
		return errors.Wrap(err, "failed to wait for cluster to come online")
	}

	err = doHTTP(
		client,
		"POST",
		fmt.Sprintf("http://%s:8091/pools/default", conn),
		"application/x-www-form-urlencoded",
		"",
		"",
		strings.NewReader("memoryQuota=1024&indexMemoryQuota=512"),
	)
	if err != nil {
		return errors.Wrap(err, "failed to setup memory quotas")
	}

	err = doHTTP(
		client,
		"POST",
		fmt.Sprintf("http://%s:8091/node/controller/setupServices", conn),
		"application/x-www-form-urlencoded",
		"",
		"",
		strings.NewReader("services=kv,cbas,index"),
	)
	if err != nil {
		return errors.Wrap(err, "failed to setup services")
	}

	err = doHTTP(
		client,
		"POST",
		fmt.Sprintf("http://%s:8091/settings/web", conn),
		"application/x-www-form-urlencoded",
		"",
		"",
		strings.NewReader(fmt.Sprintf("port=8091&password=%s&username=%s", opts.Password, opts.Username)),
	)
	if err != nil {
		return errors.Wrap(err, "failed to setup authentication")
	}

	err = doHTTP(
		client,
		"POST",
		fmt.Sprintf("http://%s:8091/pools/default/buckets", conn),
		"application/x-www-form-urlencoded",
		opts.Username,
		opts.Password,
		strings.NewReader(fmt.Sprintf("name=%s&ramQuotaMB=512&authType=none&replicaNumber=0&bucketType=couchbase", opts.BucketName)),
	)
	if err != nil {
		return errors.Wrap(err, "failed to setup bucket")
	}

	err = plugin.VerifyAnalyticsSupported(client, conn, logger)
	if err != nil {
		return errors.Wrap(err, "failed to setup dataset, could not verify analytics")
	}

	time.Sleep(1 * time.Second)

	analyticsQuery := struct {
		Statement string `json:"statement"`
		Timeout   string `json:"timeout"`
	}{
		Statement: fmt.Sprintf("CREATE DATASET `%s` ON `%s`", opts.BucketName, opts.BucketName),
		Timeout:   (500 * time.Millisecond).String(),
	}

	body, err := json.Marshal(analyticsQuery)
	if err != nil {
		return errors.Wrap(err, "failed to setup dataset")
	}

	err = doHTTP(
		client,
		"POST",
		fmt.Sprintf("http://%s:8095/analytics/service", conn),
		"application/json",
		opts.Username,
		opts.Password,
		ioutil.NopCloser(bytes.NewReader(body)),
	)
	if err != nil {
		return errors.Wrap(err, "failed to setup dataset, dataset query failed")
	}

	linkQuery := struct {
		Statement string `json:"statement"`
	}{
		"CONNECT LINK Local",
	}

	body, err = json.Marshal(linkQuery)
	if err != nil {
		return errors.Wrap(err, "failed to setup dataset")
	}

	err = doHTTP(
		client,
		"POST",
		fmt.Sprintf("http://%s:8095/analytics/service", conn),
		"application/json",
		opts.Username,
		opts.Password,
		ioutil.NopCloser(bytes.NewReader(body)),
	)
	if err != nil {
		return errors.Wrap(err, "failed to setup dataset, link connect failed")
	}

	return nil
}

func doHTTP(client httpclient.Client, method, uri, contentType, username, password string, body io.Reader) error {
	req, err := http.NewRequest(
		method,
		uri,
		body,
	)
	if err != nil {
		return err
	}
	if contentType != "" {
		req.Header.Add("content-type", contentType)
	}
	if username != "" || password != "" {
		req.SetBasicAuth(username, password)
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 && resp.StatusCode != 401 {
		dump, err := httputil.DumpResponse(resp, true)
		if err != nil {
			return errors.New("request failed, no error detail could be determined")
		}

		return fmt.Errorf("request failed: %q", dump)
	}

	return nil
}

func waitForCluster(server string, timeout time.Duration, client httpclient.Client, logger hclog.Logger) error {
	timeoutCh := time.NewTimer(timeout)
	doneCh := make(chan error)
	go func() {
		for {
			req, err := http.NewRequest("GET", fmt.Sprintf("http://%s:8091/ui/index.html", server), nil)
			if err != nil {
				doneCh <- err
				return
			}

			resp, err := client.Do(req)
			if err != nil {
				if strings.Contains(err.Error(), "connection refused") { // :(
					logger.Warn("Connection was refused whilst waiting for cluster, retrying")
					time.Sleep(500 * time.Millisecond)
					continue
				}

				doneCh <- err
				return
			}

			if resp.StatusCode == 200 {
				doneCh <- nil
				return
			}

			logger.Warn("Status code whilst waiting for cluster was non-200, retrying")
			time.Sleep(1000 * time.Millisecond)
		}
	}()

	select {
	case <-timeoutCh.C:
		return errors.New("timed out")
	case err := <-doneCh:
		return err
	}
}
