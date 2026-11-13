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

package table

import (
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSortComparerOrdersLikeTheDeclaredOrder(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "g", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "x", Type: iceberg.PrimitiveTypes.Float64},
	)
	arrowSchema, err := SchemaToArrowSchema(schema, nil, false, false)
	require.NoError(t, err)

	gNullsLastDescThenX, err := NewSortOrder(1, []SortField{
		{SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{}, Direction: SortDESC, NullOrder: NullsLast},
		{SourceIDs: []int{2}, Transform: iceberg.IdentityTransform{}, Direction: SortASC, NullOrder: NullsFirst},
	})
	require.NoError(t, err)
	cmp, err := newSortComparer(gNullsLastDescThenX, schema, arrowSchema)
	require.NoError(t, err)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer bldr.Release()
	g := bldr.Field(0).(*array.Int32Builder)
	x := bldr.Field(1).(*array.Float64Builder)
	appendRow := func(gv *int32, xv *float64) {
		if gv == nil {
			g.AppendNull()
		} else {
			g.Append(*gv)
		}
		if xv == nil {
			x.AppendNull()
		} else {
			x.Append(*xv)
		}
	}
	i32 := func(v int32) *int32 { return &v }
	f64 := func(v float64) *float64 { return &v }

	// Rows in the declared order: g descending with nulls last, then x
	// ascending with nulls first and NaN after every number.
	appendRow(i32(2), nil)
	appendRow(i32(2), f64(math.Inf(-1)))
	appendRow(i32(2), f64(0.5))
	appendRow(i32(2), f64(math.Inf(1)))
	appendRow(i32(2), f64(math.NaN()))
	appendRow(i32(1), f64(-3))
	appendRow(nil, nil)
	appendRow(nil, f64(1))
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	n := int(rec.NumRows())
	for i := range n {
		for j := range n {
			got := cmp.compare(rec, i, rec, j)
			switch {
			case i < j:
				assert.Negative(t, got, "row %d before row %d", i, j)
			case i > j:
				assert.Positive(t, got, "row %d after row %d", i, j)
			default:
				assert.Zero(t, got, "row %d equals itself", i)
			}
		}
	}
}

func TestSortComparerComparesStringsAndDecimals(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "s", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "d", Type: iceberg.DecimalTypeOf(12, 3)},
	)
	arrowSchema, err := SchemaToArrowSchema(schema, nil, false, false)
	require.NoError(t, err)
	order, err := NewSortOrder(1, []SortField{
		{SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{}, Direction: SortASC, NullOrder: NullsFirst},
		{SourceIDs: []int{2}, Transform: iceberg.IdentityTransform{}, Direction: SortASC, NullOrder: NullsFirst},
	})
	require.NoError(t, err)
	cmp, err := newSortComparer(order, schema, arrowSchema)
	require.NoError(t, err)

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSchema, strings.NewReader(
		`[{"s": "a", "d": "-1.500"}, {"s": "a", "d": "2.000"}, {"s": "ab", "d": "-9.000"}, {"s": "b", "d": "0.000"}]`))
	require.NoError(t, err)
	defer rec.Release()

	for i := 1; i < int(rec.NumRows()); i++ {
		assert.Negative(t, cmp.compare(rec, i-1, rec, i), "row %d before row %d", i-1, i)
	}
}

func TestSortComparerRefusesOrdersItCannotCompare(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "t", Type: iceberg.PrimitiveTypes.Timestamp},
		iceberg.NestedField{ID: 2, Name: "k", Type: iceberg.PrimitiveTypes.Int64},
	)
	arrowSchema, err := SchemaToArrowSchema(schema, nil, false, false)
	require.NoError(t, err)
	field := func(id int, tr iceberg.Transform) SortField {
		return SortField{SourceIDs: []int{id}, Transform: tr, Direction: SortASC, NullOrder: NullsFirst}
	}

	for name, fields := range map[string][]SortField{
		"order-preserving transform last": {field(2, iceberg.IdentityTransform{}), field(1, iceberg.DayTransform{})},
		"identity only":                   {field(1, iceberg.IdentityTransform{}), field(2, iceberg.IdentityTransform{})},
	} {
		order, err := NewSortOrder(1, fields)
		require.NoError(t, err)
		_, err = newSortComparer(order, schema, arrowSchema)
		assert.NoError(t, err, name)
	}

	for name, fields := range map[string][]SortField{
		"transform before the last field": {field(1, iceberg.DayTransform{}), field(2, iceberg.IdentityTransform{})},
		"bucket":                          {field(2, iceberg.BucketTransform{NumBuckets: 4})},
	} {
		order, err := NewSortOrder(1, fields)
		require.NoError(t, err)
		_, err = newSortComparer(order, schema, arrowSchema)
		assert.ErrorIs(t, err, ErrInvalidOperation, name)
	}
}
