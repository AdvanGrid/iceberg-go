package cases

import (
	"context"
	"maps"
	"slices"
	"testing"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// TestMergeAppendLeavesNoOrphans appends with manifest merge on, often enough
// for several merges, then checks that every object under the table location
// is referenced by the table and that all engines read the model.
func TestMergeAppendLeavesNoOrphans(t *testing.T) {
	for _, mode := range lane.DeleteModes {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			lk := lane.Open(t)
			tbl := lane.SetDeleteMode(t, lk.CreateTable(t, lane.Schema, catalog.WithProperties(iceberg.Properties{
				table.ManifestMergeEnabledKey:  "true",
				table.ManifestMinMergeCountKey: "3",
			})), mode)
			gen := lane.NewGen(10)
			model := lane.Model{}

			for day := range 12 {
				from, to := lane.Day(day)
				rows := gen.Rows(200, from, to)
				rdr := lane.Reader(rows, 256)
				var err error
				tbl, err = tbl.Append(ctx, rdr, nil)
				rdr.Release()
				require.NoError(t, err)
				model.Put(rows)
			}
			tbl = lk.Reload(t, tbl)

			referenced, err := lane.ReferencedFiles(ctx, tbl)
			require.NoError(t, err)
			objects, err := lk.Duck.ListObjects(ctx, tbl.Location())
			require.NoError(t, err)
			require.Equal(t, slices.Sorted(maps.Keys(referenced)), objects)
			lk.RequireChecksum(t, tbl, 0, model.Checksum())
		})
	}
}
