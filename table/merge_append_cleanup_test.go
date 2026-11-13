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

package table_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func referencedManifests(t *testing.T, fs iceio.IO, tbl *table.Table) map[string]struct{} {
	t.Helper()
	referenced := map[string]struct{}{}
	for _, snap := range tbl.Metadata().Snapshots() {
		manifests, err := snap.Manifests(fs)
		require.NoError(t, err)
		for _, m := range manifests {
			referenced[m.FilePath()] = struct{}{}
		}
	}

	return referenced
}

// TestMergeAppendLeavesNoUnreferencedManifests verifies that a merge append
// deletes the manifest it wrote for the new files once that manifest has been
// merged into a larger one, so no manifest in the table location is left
// unreferenced by every snapshot.
func TestMergeAppendLeavesNoUnreferencedManifests(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs := iceio.LocalFS{}

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, dir, iceberg.Properties{
			table.ManifestMergeEnabledKey:  "true",
			table.ManifestMinMergeCountKey: "2",
		})
	require.NoError(t, err)

	tbl := table.New(table.Identifier{"default", "merge_append_cleanup"}, meta, dir+"/metadata/00000.json",
		func(_ context.Context) (iceio.IO, error) { return fs, nil },
		&mergeCatalog{meta: meta},
	)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)
	for i := range 5 {
		bldr := array.NewInt32Builder(memory.DefaultAllocator)
		bldr.AppendValues([]int32{int32(i)}, nil)
		col := bldr.NewArray()
		bldr.Release()
		rec := array.NewRecordBatch(arrowSchema, []arrow.Array{col}, 1)
		col.Release()
		rdr, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{rec})
		require.NoError(t, err)
		rec.Release()

		tbl, err = tbl.Append(ctx, rdr, nil)
		rdr.Release()
		require.NoError(t, err, "append %d", i)
	}

	manifests, err := tbl.CurrentSnapshot().Manifests(fs)
	require.NoError(t, err)
	require.Len(t, manifests, 1, "the appends should have been merged")

	written, err := filepath.Glob(filepath.Join(dir, "metadata", "*-m*.avro"))
	require.NoError(t, err)
	referenced := referencedManifests(t, fs, tbl)
	for _, path := range written {
		_, ok := referenced[path]
		assert.True(t, ok, "manifest %s is not referenced by any snapshot", path)
	}
}
