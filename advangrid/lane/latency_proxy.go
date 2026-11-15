package lane

import (
	"io"
	"log"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// LatencyProxy starts an S3 reverse proxy to Garage that answers every
// request delay late, as a remote object store such as S3 does; Garage on
// the same host answers in well under a millisecond, which hides the cost
// of each extra object read. The Host header is kept so SigV4 signatures
// still verify. It returns the proxy's endpoint.
func LatencyProxy(t testing.TB, delay time.Duration) string {
	t.Helper()
	target, err := url.Parse(S3Endpoint)
	require.NoError(t, err)

	proxy := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			time.Sleep(delay)
			r.SetURL(target)
			r.Out.Host = r.In.Host
		},
		ErrorLog: log.New(io.Discard, "", 0),
	}
	srv := httptest.NewServer(proxy)
	t.Cleanup(srv.Close)

	return srv.URL
}
