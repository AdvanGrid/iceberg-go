// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package rest_test

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	_ "github.com/apache/iceberg-go/io/gocloud/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type objectStore struct {
	srv  *httptest.Server
	hits atomic.Int32
}

func newObjectStore(t *testing.T) *objectStore {
	s := &objectStore{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		s.hits.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(s.srv.Close)

	return s
}

func newTableConfigServer(t *testing.T, storageEndpoint string) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"defaults": {}, "overrides": {}}`)
	})
	mux.HandleFunc("/v1/namespaces/ns/tables/tbl", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"metadata-location": "s3://warehouse/database/table/metadata.json", "metadata": %s,
			"config": {"s3.endpoint": %q, "s3.path-style-access": "true"}}`,
			exampleTableMetadataNoSnapshotV1, storageEndpoint)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return srv
}

func storageProps(endpoint string) iceberg.Properties {
	return iceberg.Properties{
		"s3.endpoint":          endpoint,
		"s3.region":            "us-east-1",
		"s3.access-key-id":     "key",
		"s3.secret-access-key": "secret",
	}
}

func openTableObject(t *testing.T, cat catalog.Catalog) {
	ctx := context.Background()
	tbl, err := cat.LoadTable(ctx, catalog.ToIdentifier("ns", "tbl"))
	require.NoError(t, err)

	fs, err := tbl.FS(ctx)
	require.NoError(t, err)
	_, _ = fs.Open("s3://warehouse/database/table/data/00000.parquet")
}

func TestLoadTableStorageEndpointPrecedence(t *testing.T) {
	loadWithProps := func(extra iceberg.Properties) func(*testing.T, string, string) catalog.Catalog {
		return func(t *testing.T, uri, endpoint string) catalog.Catalog {
			props := storageProps(endpoint)
			props["type"] = "rest"
			props["uri"] = uri
			maps.Copy(props, extra)
			cat, err := catalog.Load(context.Background(), "rest", props)
			require.NoError(t, err)

			return cat
		}
	}

	tests := []struct {
		name             string
		open             func(t *testing.T, uri, endpoint string) catalog.Catalog
		wantClientStore  bool
		wantCatalogStore bool
	}{
		{name: "table config wins by default", open: loadWithProps(nil), wantCatalogStore: true},
		{
			name:            "client properties win with the property",
			open:            loadWithProps(iceberg.Properties{"rest.prefer-client-config": "true"}),
			wantClientStore: true,
		},
		{
			name: "client properties win with the option",
			open: func(t *testing.T, uri, endpoint string) catalog.Catalog {
				cat, err := rest.NewCatalog(context.Background(), "rest", uri,
					rest.WithAdditionalProps(storageProps(endpoint)), rest.WithPreferClientConfig())
				require.NoError(t, err)

				return cat
			},
			wantClientStore: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientStore := newObjectStore(t)
			catalogStore := newObjectStore(t)
			srv := newTableConfigServer(t, catalogStore.srv.URL)

			openTableObject(t, tt.open(t, srv.URL, clientStore.srv.URL))

			assert.Equal(t, tt.wantClientStore, clientStore.hits.Load() > 0, "client store hit")
			assert.Equal(t, tt.wantCatalogStore, catalogStore.hits.Load() > 0, "catalog store hit")
		})
	}
}
