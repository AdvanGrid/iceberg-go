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
	"bytes"
	"cmp"
	"container/heap"
	"context"
	"fmt"
	"iter"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
)

// sortComparer compares rows of record batches by a sort order, using the
// same ordering as the Arrow sort the writers apply: nulls placed by the
// null order regardless of direction, NaN after every other float before
// the direction is applied.
type sortComparer struct {
	keys []sortKeyComparer
}

type sortKeyComparer struct {
	column     int
	descending bool
	nullsFirst bool
	compare    func(a, b arrow.Array, i, j int) int
}

// checkMergeableSortOrder rejects sort orders that rows ordered by their
// source columns do not always satisfy: every field but the last must use
// the identity transform, and the last must preserve order.
func checkMergeableSortOrder(order SortOrder) error {
	if order.IsUnsorted() {
		return fmt.Errorf("%w: the table has no sort order", ErrInvalidOperation)
	}
	for i, field := range order.Fields() {
		_, identity := field.Transform.(iceberg.IdentityTransform)
		last := i == order.Len()-1
		if !identity && (!last || !field.Transform.PreservesOrder()) {
			return fmt.Errorf("%w: sort order %d: cannot order rows by %s, only the last field may use a transform other than identity, and it must preserve order",
				ErrInvalidOperation, order.OrderID(), field.Transform)
		}
	}

	return nil
}

// newSortComparer builds a comparer for batches of arrowSchema, whose
// columns are the top-level fields of schema.
func newSortComparer(order SortOrder, schema *iceberg.Schema, arrowSchema *arrow.Schema) (*sortComparer, error) {
	if err := checkMergeableSortOrder(order); err != nil {
		return nil, err
	}

	keys, err := resolveSortKeys(order, schema)
	if err != nil {
		return nil, err
	}
	c := &sortComparer{keys: make([]sortKeyComparer, len(keys))}
	for i, key := range keys {
		if key.ColumnIndex >= arrowSchema.NumFields() {
			return nil, fmt.Errorf("sort key column %d is not in the arrow schema %s", key.ColumnIndex, arrowSchema)
		}
		dt := arrowSchema.Field(key.ColumnIndex).Type
		compare, err := arrowValueComparer(dt)
		if err != nil {
			return nil, err
		}
		c.keys[i] = sortKeyComparer{
			column:     key.ColumnIndex,
			descending: key.Order == compute.SortOrderDescending,
			nullsFirst: key.NullPlacement == compute.SortNullsAtStart,
			compare:    compare,
		}
	}

	return c, nil
}

// compare returns a negative number when row i of a sorts before row j of
// b, a positive number when it sorts after, and zero when they tie.
func (c *sortComparer) compare(a arrow.RecordBatch, i int, b arrow.RecordBatch, j int) int {
	for _, key := range c.keys {
		x, y := a.Column(key.column), b.Column(key.column)
		xNull, yNull := x.IsNull(i), y.IsNull(j)
		switch {
		case xNull && yNull:
			continue
		case xNull != yNull:
			if xNull == key.nullsFirst {
				return -1
			}

			return 1
		}
		r := key.compare(x, y, i, j)
		if key.descending {
			r = -r
		}
		if r != 0 {
			return r
		}
	}

	return 0
}

// inOrder reports whether records are in the comparer's order. It stops at
// the first row out of order and releases every batch it reads.
func (c *sortComparer) inOrder(records iter.Seq2[arrow.RecordBatch, error]) (bool, error) {
	var prev arrow.RecordBatch
	defer func() {
		if prev != nil {
			prev.Release()
		}
	}()
	for rec, err := range records {
		if err != nil {
			return false, err
		}
		ordered := prev == nil || rec.NumRows() == 0 ||
			c.compare(prev, int(prev.NumRows())-1, rec, 0) <= 0
		for i := 1; ordered && i < int(rec.NumRows()); i++ {
			ordered = c.compare(rec, i-1, rec, i) <= 0
		}
		if !ordered {
			rec.Release()

			return false, nil
		}
		if rec.NumRows() == 0 {
			rec.Release()

			continue
		}
		if prev != nil {
			prev.Release()
		}
		prev = rec
	}

	return true, nil
}

type orderedValues[T any] interface {
	Value(int) T
}

func compareOrdered[T cmp.Ordered, A orderedValues[T]](a, b arrow.Array, i, j int) int {
	return cmp.Compare(a.(A).Value(i), b.(A).Value(j))
}

func compareFloat[T float32 | float64, A orderedValues[T]](a, b arrow.Array, i, j int) int {
	x, y := a.(A).Value(i), b.(A).Value(j)
	xNaN, yNaN := x != x, y != y
	switch {
	case xNaN && yNaN:
		return 0
	case xNaN:
		return 1
	case yNaN:
		return -1
	}

	return cmp.Compare(x, y)
}

func compareBytes[A orderedValues[[]byte]](a, b arrow.Array, i, j int) int {
	return bytes.Compare(a.(A).Value(i), b.(A).Value(j))
}

func compareBool(a, b arrow.Array, i, j int) int {
	x, y := a.(*array.Boolean).Value(i), b.(*array.Boolean).Value(j)
	switch {
	case x == y:
		return 0
	case y:
		return -1
	}

	return 1
}

func compareDecimal128(a, b arrow.Array, i, j int) int {
	return a.(*array.Decimal128).Value(i).Cmp(b.(*array.Decimal128).Value(j))
}

func compareDecimal256(a, b arrow.Array, i, j int) int {
	return a.(*array.Decimal256).Value(i).Cmp(b.(*array.Decimal256).Value(j))
}

// arrowValueComparer returns a three-way comparison of two non-null values
// of type dt.
func arrowValueComparer(dt arrow.DataType) (func(a, b arrow.Array, i, j int) int, error) {
	switch dt.ID() {
	case arrow.BOOL:
		return compareBool, nil
	case arrow.INT8:
		return compareOrdered[int8, *array.Int8], nil
	case arrow.INT16:
		return compareOrdered[int16, *array.Int16], nil
	case arrow.INT32:
		return compareOrdered[int32, *array.Int32], nil
	case arrow.INT64:
		return compareOrdered[int64, *array.Int64], nil
	case arrow.UINT8:
		return compareOrdered[uint8, *array.Uint8], nil
	case arrow.UINT16:
		return compareOrdered[uint16, *array.Uint16], nil
	case arrow.UINT32:
		return compareOrdered[uint32, *array.Uint32], nil
	case arrow.UINT64:
		return compareOrdered[uint64, *array.Uint64], nil
	case arrow.FLOAT32:
		return compareFloat[float32, *array.Float32], nil
	case arrow.FLOAT64:
		return compareFloat[float64, *array.Float64], nil
	case arrow.DATE32:
		return compareOrdered[arrow.Date32, *array.Date32], nil
	case arrow.DATE64:
		return compareOrdered[arrow.Date64, *array.Date64], nil
	case arrow.TIME32:
		return compareOrdered[arrow.Time32, *array.Time32], nil
	case arrow.TIME64:
		return compareOrdered[arrow.Time64, *array.Time64], nil
	case arrow.TIMESTAMP:
		return compareOrdered[arrow.Timestamp, *array.Timestamp], nil
	case arrow.DURATION:
		return compareOrdered[arrow.Duration, *array.Duration], nil
	case arrow.STRING:
		return compareOrdered[string, *array.String], nil
	case arrow.LARGE_STRING:
		return compareOrdered[string, *array.LargeString], nil
	case arrow.STRING_VIEW:
		return compareOrdered[string, *array.StringView], nil
	case arrow.BINARY:
		return compareBytes[*array.Binary], nil
	case arrow.LARGE_BINARY:
		return compareBytes[*array.LargeBinary], nil
	case arrow.BINARY_VIEW:
		return compareBytes[*array.BinaryView], nil
	case arrow.FIXED_SIZE_BINARY:
		return compareBytes[*array.FixedSizeBinary], nil
	case arrow.DECIMAL128:
		return compareDecimal128, nil
	case arrow.DECIMAL256:
		return compareDecimal256, nil
	case arrow.EXTENSION:
		storage, err := arrowValueComparer(dt.(arrow.ExtensionType).StorageType())
		if err != nil {
			return nil, err
		}

		return func(a, b arrow.Array, i, j int) int {
			return storage(a.(array.ExtensionArray).Storage(), b.(array.ExtensionArray).Storage(), i, j)
		}, nil
	}

	return nil, fmt.Errorf("%w: cannot compare sort key values of type %s", ErrInvalidOperation, dt)
}

// sortMergeRecords reads the tasks of a compaction group and returns their
// rows merged into the table's sort order. A task whose file is in order is
// streamed; one that is not is read and sorted in memory first. The tasks
// must not have delete files.
func sortMergeRecords(ctx context.Context, tbl *Table, tasks []FileScanTask, scanOpts []ScanOption, batchRows int) (*arrow.Schema, iter.Seq2[arrow.RecordBatch, error], error) {
	for _, task := range tasks {
		if len(task.DeleteFiles)+len(task.EqualityDeleteFiles)+len(task.DeletionVectorFiles) > 0 {
			return nil, nil, fmt.Errorf("%w: the sort strategy cannot rewrite %s, it has delete files",
				ErrInvalidOperation, task.File.FilePath())
		}
	}

	order := tbl.SortOrder()
	if err := checkMergeableSortOrder(order); err != nil {
		return nil, nil, err
	}
	scan := tbl.Scan(scanOpts...)
	projected, err := scan.Projection()
	if err != nil {
		return nil, nil, err
	}
	keyScan, err := sortKeyScan(tbl, order, scanOpts)
	if err != nil {
		return nil, nil, err
	}

	var (
		arrowSchema *arrow.Schema
		inputs      = make([]iter.Seq2[arrow.RecordBatch, error], 0, len(tasks))
	)
	for _, task := range tasks {
		ordered, err := taskInOrder(ctx, keyScan, order, task)
		if err != nil {
			return nil, nil, fmt.Errorf("check sort order of %s: %w", task.File.FilePath(), err)
		}
		sc, records, err := scan.ReadTasks(ctx, []FileScanTask{task})
		if err != nil {
			return nil, nil, fmt.Errorf("read %s: %w", task.File.FilePath(), err)
		}
		arrowSchema = sc
		if !ordered {
			records = sortedInMemory(ctx, order, projected, records, batchRows)
		}
		inputs = append(inputs, records)
	}

	cmp, err := newSortComparer(order, projected, arrowSchema)
	if err != nil {
		return nil, nil, err
	}

	return arrowSchema, mergeSorted(ctx, cmp, inputs, batchRows), nil
}

// sortKeyScan returns a scan that reads only the sort order's source columns.
func sortKeyScan(tbl *Table, order SortOrder, scanOpts []ScanOption) (*Scan, error) {
	names := make([]string, 0, order.Len())
	for _, field := range order.Fields() {
		name, ok := tbl.Schema().FindColumnName(field.SourceID())
		if !ok {
			return nil, fmt.Errorf("sort order %d: source id %d is not in the table schema", order.OrderID(), field.SourceID())
		}
		names = append(names, name)
	}

	return tbl.Scan(append(slices.Clone(scanOpts), WithSelectedFields(names...))...), nil
}

// taskInOrder reports whether the rows of task are in order, reading only
// the sort key columns.
func taskInOrder(ctx context.Context, keyScan *Scan, order SortOrder, task FileScanTask) (bool, error) {
	projected, err := keyScan.Projection()
	if err != nil {
		return false, err
	}
	arrowSchema, records, err := keyScan.ReadTasks(ctx, []FileScanTask{task})
	if err != nil {
		return false, err
	}
	cmp, err := newSortComparer(order, projected, arrowSchema)
	if err != nil {
		return false, err
	}

	return cmp.inOrder(records)
}

// sortedInMemory reads all of records and yields them sorted by order.
func sortedInMemory(ctx context.Context, order SortOrder, schema *iceberg.Schema, records iter.Seq2[arrow.RecordBatch, error], batchRows int) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		var batches []arrow.RecordBatch
		defer func() {
			for _, b := range batches {
				b.Release()
			}
		}()
		for rec, err := range records {
			if err != nil {
				yield(nil, err)

				return
			}
			batches = append(batches, rec)
		}
		if len(batches) == 0 {
			return
		}

		keys, err := resolveSortKeys(order, schema)
		if err != nil {
			yield(nil, err)

			return
		}
		unsorted := array.NewTableFromRecords(batches[0].Schema(), batches)
		defer unsorted.Release()
		sorted, err := compute.SortTable(ctx, unsorted, keys)
		if err != nil {
			yield(nil, err)

			return
		}
		defer sorted.Release()

		rdr := array.NewTableReader(sorted, int64(batchRows))
		defer rdr.Release()
		for rdr.Next() {
			rec := rdr.RecordBatch()
			rec.Retain()
			if !yield(rec, nil) {
				return
			}
		}
	}
}

type mergeCursor struct {
	input int
	next  func() (arrow.RecordBatch, error, bool)
	stop  func()
	batch arrow.RecordBatch
	row   int
}

// advance releases the current batch and moves to the next non-empty one.
// batch is nil once the input is exhausted.
func (c *mergeCursor) advance() error {
	if c.batch != nil {
		c.batch.Release()
		c.batch = nil
	}
	for {
		rec, err, ok := c.next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if rec.NumRows() > 0 {
			c.batch, c.row = rec, 0

			return nil
		}
		rec.Release()
	}
}

type mergeHeap struct {
	cmp     *sortComparer
	cursors []*mergeCursor
}

func (h *mergeHeap) Len() int { return len(h.cursors) }

func (h *mergeHeap) Less(i, j int) bool {
	a, b := h.cursors[i], h.cursors[j]
	if r := h.cmp.compare(a.batch, a.row, b.batch, b.row); r != 0 {
		return r < 0
	}

	return a.input < b.input
}

func (h *mergeHeap) Swap(i, j int) { h.cursors[i], h.cursors[j] = h.cursors[j], h.cursors[i] }

func (h *mergeHeap) Push(x any) { h.cursors = append(h.cursors, x.(*mergeCursor)) }

func (h *mergeHeap) Pop() any {
	last := h.cursors[len(h.cursors)-1]
	h.cursors = h.cursors[:len(h.cursors)-1]

	return last
}

// mergeSorted merges inputs, each in cmp's order, into one stream in cmp's
// order, of batches of at least batchRows rows except the last. It holds
// the current batch of every input; ties go to the earlier input.
func mergeSorted(ctx context.Context, cmp *sortComparer, inputs []iter.Seq2[arrow.RecordBatch, error], batchRows int) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		mem := compute.GetAllocator(ctx)
		h := &mergeHeap{cmp: cmp}
		cursors := make([]*mergeCursor, 0, len(inputs))
		var (
			pending     []arrow.RecordBatch
			pendingRows int
		)
		defer func() {
			for _, c := range cursors {
				if c.batch != nil {
					c.batch.Release()
				}
				c.stop()
			}
			for _, p := range pending {
				p.Release()
			}
		}()

		for i, input := range inputs {
			next, stop := iter.Pull2(input)
			c := &mergeCursor{input: i, next: next, stop: stop}
			cursors = append(cursors, c)
			if err := c.advance(); err != nil {
				yield(nil, err)

				return
			}
			if c.batch != nil {
				h.cursors = append(h.cursors, c)
			}
		}
		heap.Init(h)

		flush := func() bool {
			rec, err := concatRecords(mem, pending)
			for _, p := range pending {
				p.Release()
			}
			pending, pendingRows = pending[:0], 0
			if err != nil {
				yield(nil, err)

				return false
			}

			return yield(rec, nil)
		}

		for h.Len() > 0 {
			if err := ctx.Err(); err != nil {
				yield(nil, err)

				return
			}
			c := heap.Pop(h).(*mergeCursor)
			n := int(c.batch.NumRows())
			end := n
			if h.Len() > 0 {
				top := h.cursors[0]
				end = c.row + 1
				for end < n && cmp.compare(c.batch, end, top.batch, top.row) <= 0 {
					end++
				}
			}
			pending = append(pending, c.batch.NewSlice(int64(c.row), int64(end)))
			pendingRows += end - c.row
			c.row = end
			if c.row == n {
				if err := c.advance(); err != nil {
					yield(nil, err)

					return
				}
			}
			if c.batch != nil {
				heap.Push(h, c)
			}
			if pendingRows >= batchRows && !flush() {
				return
			}
		}
		if pendingRows > 0 {
			flush()
		}
	}
}

// concatRecords concatenates record batches of one schema into a new one.
func concatRecords(mem memory.Allocator, recs []arrow.RecordBatch) (arrow.RecordBatch, error) {
	schema := recs[0].Schema()
	cols := make([]arrow.Array, schema.NumFields())
	defer func() {
		for _, col := range cols {
			if col != nil {
				col.Release()
			}
		}
	}()
	parts := make([]arrow.Array, len(recs))
	var rows int64
	for _, rec := range recs {
		rows += rec.NumRows()
	}
	for i := range cols {
		for j, rec := range recs {
			parts[j] = rec.Column(i)
		}
		col, err := array.Concatenate(parts, mem)
		if err != nil {
			return nil, err
		}
		cols[i] = col
	}

	return array.NewRecordBatch(schema, cols, rows), nil
}
