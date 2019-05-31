package setup

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"github.com/chvck/couchbase-jaeger-storage-plugin/httpclient"

	"github.com/hashicorp/go-hclog"

	"github.com/pkg/errors"
)

func Run(server, username, password, bucket string, client httpclient.Client, logger hclog.Logger) error {
	err := waitForCluster(server, 45*time.Second, client, logger)
	if err != nil {
		return errors.Wrap(err, "failed to wait for cluster to come online")
	}

	err = doHTTP(
		client,
		"POST",
		fmt.Sprintf("http://%s:8091/pools/default", server),
		"application/x-www-form-urlencoded",
		"",
		"",
		strings.NewReader("memoryQuota=512&indexMemoryQuota=512"),
	)
	if err != nil {
		return errors.Wrap(err, "failed to setup memory quotas")
	}

	err = doHTTP(
		client,
		"POST",
		fmt.Sprintf("http://%s:8091/node/controller/setupServices", server),
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
		fmt.Sprintf("http://%s:8091/settings/web", server),
		"application/x-www-form-urlencoded",
		"",
		"",
		strings.NewReader(fmt.Sprintf("port=8091&password=%s&username=%s", password, username)),
	)
	if err != nil {
		return errors.Wrap(err, "failed to setup authentication")
	}

	err = doHTTP(
		client,
		"POST",
		fmt.Sprintf("http://%s:8091/pools/default/buckets", server),
		"application/x-www-form-urlencoded",
		username,
		password,
		strings.NewReader(fmt.Sprintf("name=%s&ramQuotaMB=512&authType=none&replicaNumber=0&bucketType=couchbase", bucket)),
	)
	if err != nil {
		return errors.Wrap(err, "failed to setup bucket")
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
