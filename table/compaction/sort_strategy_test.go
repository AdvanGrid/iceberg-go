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

package compaction_test

import (
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanCompaction_SortStrategyRefusesDeleteFiles(t *testing.T) {
	cfg := compaction.DefaultConfig()
	cfg.MinInputFiles = 2
	cfg.Strategy = table.SortStrategy

	clean := []table.FileScanTask{
		makeTask(newDataFile("a.parquet", 1), 0, 0),
		makeTask(newDataFile("b.parquet", 1), 0, 0),
	}
	plan, err := cfg.PlanCompaction(clean)
	require.NoError(t, err)
	assert.Len(t, plan.Groups, 1)

	withDeletes := append(clean, makeTask(newDataFile("c.parquet", 1), 1, 0))
	_, err = cfg.PlanCompaction(withDeletes)
	require.ErrorIs(t, err, table.ErrInvalidOperation)
	assert.ErrorContains(t, err, "delete files")
}

func TestAnalyze_SortStrategyRefusesUnsortedTable(t *testing.T) {
	cfg := compaction.DefaultConfig()
	cfg.Strategy = table.SortStrategy

	_, err := compaction.Analyze(t.Context(), newAnalyzeTestTable(t), cfg)
	require.ErrorIs(t, err, table.ErrInvalidOperation)
	assert.ErrorContains(t, err, "sort order")
}
