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
	"fmt"
	"maps"
	"path/filepath"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// manifestReadCountingIO counts reads of the watched paths once watching
// starts.
type manifestReadCountingIO struct {
	iceio.LocalFS

	mu       sync.Mutex
	watching bool
	watched  map[string]struct{}
	reads    map[string]int
}

func (c *manifestReadCountingIO) record(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.watched[name]; c.watching && ok {
		c.reads[name]++
	}
}

func (c *manifestReadCountingIO) Open(name string) (iceio.File, error) {
	c.record(name)

	return c.LocalFS.Open(name)
}

func (c *manifestReadCountingIO) ReadFile(name string) ([]byte, error) {
	c.record(name)

	return c.LocalFS.ReadFile(name)
}

func (c *manifestReadCountingIO) watch(paths []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.watching = true
	for _, p := range paths {
		c.watched[p] = struct{}{}
	}
}

func (c *manifestReadCountingIO) readCounts() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return maps.Clone(c.reads)
}

// retryReadsScenario is a table of four data files, one manifest each, and a
// writer that overwrites the rows with id < 5 (all of the first file) while
// peers commit between its attempts.
type retryReadsScenario struct {
	t        *testing.T
	location string
	arrowSc  *arrow.Schema
	base     table.Metadata
	peers    int
}

func newRetryReadsScenario(t *testing.T) *retryReadsScenario {
	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "val", Type: iceberg.PrimitiveTypes.String},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion:        "2",
			table.CommitMinRetryWaitMsKey:      "0",
			table.CommitMaxRetryWaitMsKey:      "0",
			table.CommitTotalRetryTimeoutMsKey: "60000",
			table.CommitNumRetriesKey:          "5",
		})
	require.NoError(t, err)
	arrowSc, err := table.SchemaToArrowSchema(schema, nil, false, false)
	require.NoError(t, err)

	s := &retryReadsScenario{t: t, location: location, arrowSc: arrowSc, base: meta}
	for i := range 4 {
		s.base = s.peerAddFile(s.base, fmt.Sprintf(`[{"id":%d,"val":"a"},{"id":%d,"val":"b"}]`, 10*i+1, 10*i+3))
	}

	return s
}

func (s *retryReadsScenario) peerTable(current table.Metadata) (*table.Table, *occScenarioCatalog) {
	cat := &occScenarioCatalog{current: current, location: s.location}

	return table.New([]string{"default", "retry_reads"}, current, s.location+"/metadata/peer.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat), cat
}

// peerAddFile commits one data file holding rows on top of current.
func (s *retryReadsScenario) peerAddFile(current table.Metadata, rows string) table.Metadata {
	tbl, cat := s.peerTable(current)
	path := fmt.Sprintf("%s/data/peer-%d.parquet", s.location, s.peers)
	s.peers++
	writeParquetFile(s.t, path, s.arrowSc, rows)
	tx := tbl.NewTransaction()
	require.NoError(s.t, tx.AddFiles(s.t.Context(), []string{path}, nil, false))
	_, err := tx.Commit(s.t.Context())
	require.NoError(s.t, err)

	return cat.current
}

// peerDeleteOverwritten commits the removal of the rows the writer overwrites.
func (s *retryReadsScenario) peerDeleteOverwritten(current table.Metadata) table.Metadata {
	tbl, cat := s.peerTable(current)
	tx := tbl.NewTransaction()
	require.NoError(s.t, tx.Delete(s.t.Context(), overwrittenRows(), nil))
	_, err := tx.Commit(s.t.Context())
	require.NoError(s.t, err)

	return cat.current
}

func overwrittenRows() iceberg.BooleanExpression {
	return iceberg.LessThan(iceberg.Reference("id"), int64(5))
}

// overwrite overwrites the rows with id < 5 from the base snapshot. Each
// CommitTable call but the last is rejected after peers[i] advanced the
// table. Reads of the base snapshot's manifests are counted from the first
// rejection on.
func (s *retryReadsScenario) overwrite(peers ...func(table.Metadata) table.Metadata) (*occScenarioCatalog, map[string]int, error) {
	baseManifests, err := s.base.CurrentSnapshot().Manifests(iceio.LocalFS{})
	require.NoError(s.t, err)
	paths := make([]string, len(baseManifests))
	for i, m := range baseManifests {
		paths[i] = m.FilePath()
	}

	fs := &manifestReadCountingIO{watched: map[string]struct{}{}, reads: map[string]int{}}
	conflict := 0
	cat := &occScenarioCatalog{
		current:       s.base,
		conflictsLeft: len(peers),
		location:      s.location,
		onConflict: func(current table.Metadata) table.Metadata {
			fs.watch(paths)
			conflict++

			return peers[conflict-1](current)
		},
	}
	tbl := table.New([]string{"default", "retry_reads"}, s.base, s.location+"/metadata/writer.metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil }, cat)

	rows, err := array.TableFromJSON(memory.DefaultAllocator, s.arrowSc, []string{`[{"id":2,"val":"new"}]`})
	require.NoError(s.t, err)
	defer rows.Release()
	rdr := array.NewTableReader(rows, -1)
	defer rdr.Release()

	tx := tbl.NewTransaction()
	require.NoError(s.t, tx.Overwrite(s.t.Context(), rdr, nil, table.WithOverwriteFilter(overwrittenRows())))
	_, err = tx.Commit(s.t.Context())

	return cat, fs.readCounts(), err
}

func (s *retryReadsScenario) appendUnrelated(current table.Metadata) table.Metadata {
	return s.peerAddFile(current, fmt.Sprintf(`[{"id":%d,"val":"p"}]`, 100+s.peers))
}

// TestCommitRetryDoesNotRereadCheckedManifests checks that an overwrite that
// loses the commit race several times reads each manifest it already checked
// only once, as Java's ManifestFilterManager caches filtered manifests across
// attempts: the retries read the manifests the peers added, not the ones the
// writer's base snapshot already had.
func TestCommitRetryDoesNotRereadCheckedManifests(t *testing.T) {
	s := newRetryReadsScenario(t)

	cat, reads, err := s.overwrite(s.appendUnrelated, s.appendUnrelated, s.appendUnrelated)
	require.NoError(t, err)
	require.Equal(t, int32(4), cat.commitTableCalls.Load())
	require.Empty(t, reads, "retries re-read manifests of the base snapshot")

	tbl, _ := s.peerTable(cat.current)
	assertRowCount(t, tbl, 2+2+2+3+1)
}

// TestCommitRetryStillDetectsConcurrentRemoval checks that a retry after the
// overwritten file was removed by a peer fails, although an earlier attempt
// already found the file in a manifest.
func TestCommitRetryStillDetectsConcurrentRemoval(t *testing.T) {
	s := newRetryReadsScenario(t)

	_, _, err := s.overwrite(s.appendUnrelated, s.peerDeleteOverwritten)
	require.ErrorIs(t, err, table.ErrCommitDiverged)
}

// TestCommitRetryStillDetectsConflictingAppend checks that a retry fails when
// a peer appended rows matching the overwrite filter after an earlier attempt
// passed validation.
func TestCommitRetryStillDetectsConflictingAppend(t *testing.T) {
	s := newRetryReadsScenario(t)

	_, _, err := s.overwrite(s.appendUnrelated, func(current table.Metadata) table.Metadata {
		return s.peerAddFile(current, `[{"id":4,"val":"p"}]`)
	})
	require.ErrorIs(t, err, table.ErrConflictingDataFiles)
}
