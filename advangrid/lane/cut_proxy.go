package lane

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go/catalog"
	"github.com/stretchr/testify/require"
)

// CutProxy starts an S3 reverse proxy to Garage that cuts the body of the
// first cuts ranged GETs of data files halfway, as a dropped connection does.
// Every write is flushed so the client receives the headers and part of the
// body before the cut. The Host header is kept so SigV4 signatures still
// verify. It returns the proxy's endpoint.
func CutProxy(t testing.TB, cuts int32) string {
	t.Helper()
	target, err := url.Parse(S3Endpoint)
	require.NoError(t, err)
	var remaining atomic.Int32
	remaining.Store(cuts)

	proxy := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(target)
			r.Out.Host = r.In.Host
		},
		ModifyResponse: func(resp *http.Response) error {
			req := resp.Request
			if req.Method != http.MethodGet || req.Header.Get("Range") == "" ||
				!strings.Contains(req.URL.Path, "/data/") || resp.ContentLength < 2 {
				return nil
			}
			if remaining.Add(-1) < 0 {
				return nil
			}
			resp.Body = cutBody{Reader: io.LimitReader(resp.Body, resp.ContentLength/2), body: resp.Body}

			return nil
		},
		FlushInterval: -1,
		ErrorLog:      log.New(io.Discard, "", 0),
	}
	srv := httptest.NewServer(proxy)
	t.Cleanup(srv.Close)

	return srv.URL
}

// cutBody yields part of a response body and then fails, which makes the
// proxy abort the response mid-body.
type cutBody struct {
	io.Reader
	body io.ReadCloser
}

func (c cutBody) Read(p []byte) (int, error) {
	n, err := c.Reader.Read(p)
	if err == io.EOF {
		return n, io.ErrUnexpectedEOF
	}

	return n, err
}

func (c cutBody) Close() error { return c.body.Close() }

// CatalogVia loads the lane catalog with its storage pointed at endpoint
// instead of the endpoint the catalog's config names.
func CatalogVia(t testing.TB, endpoint string) catalog.Catalog {
	t.Helper()
	props := CatalogProperties(Warehouse)
	props["s3.endpoint"] = endpoint
	props["rest.prefer-client-config"] = "true"
	cat, err := catalog.Load(context.Background(), "lane-via-proxy", props)
	require.NoError(t, err)

	return cat
}
