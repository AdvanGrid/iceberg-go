# AdvanGrid fork of iceberg-go

This repository is AdvanGrid's fork of [apache/iceberg-go](https://github.com/apache/iceberg-go). AdvanGrid's lake writes and maintains its Iceberg tables with iceberg-go. The fork carries the fixes we need on our own schedule, each tested against Iceberg Java. Tracking issue: AdvanGrid/advangrid#2628.

## Using the fork

`go.mod` keeps `module github.com/apache/iceberg-go`, so imports never change. A consumer pins a fork tag with a `replace` directive:

```
replace github.com/apache/iceberg-go => github.com/AdvanGrid/iceberg-go v0.7.0-adv.1
```

## Branches and tags

- `advangrid` is the default branch. It was cut from upstream tag `v0.7.0`. Changes land through pull requests only, and the `unit`, `integration` and `differential` checks must pass.
- Releases are tags `v<upstream>-adv.<n>`, for example `v0.7.0-adv.1`. A tag is cut once all three checks and the nightly run pass on its commit.
- Every fix is a plain diff against upstream, so it can go upstream as it is. Whether to open an upstream pull request is decided by a person, never by default.

## Merging upstream

Upstream is merged into `advangrid`, never rebased:

1. `git fetch upstream --tags` and branch from `advangrid`.
2. `git merge v<next>` and resolve conflicts.
3. Adapt the harness in `advangrid/` to API changes.
4. Open the pull request, and run the nightly workflow on its branch before tagging.
5. Tag the merge as `v<next>-adv.1`.

## Tests

Each fix comes with two tests, both red before the fix:

- a unit test next to the code it fixes, in upstream's style;
- a differential case in `advangrid/` that compares what iceberg-go and DuckDB read with what Iceberg Java (Spark) reads.

CI (`.github/workflows/advangrid.yml`) runs three jobs on pull requests and pushes to `advangrid`:

| Job            | Runs                                                       |
| -------------- | ---------------------------------------------------------- |
| `unit`         | `go test ./...`                                            |
| `integration`  | upstream's `make integration-setup` and `make integration-test` |
| `differential` | `make advangrid-setup` and `make advangrid-test`           |

Upstream's own workflows are disabled in this repository. The `nightly` job runs on the daily schedule and on manual dispatch: `make advangrid-nightly` runs the randomized test over every layout with more steps (`LANE_NIGHTLY=1`), split into 8 shards (`LANE_SHARD=k/8`).

To run the upstream integration lane locally: `make integration-setup`, export the variables printed by `make integration-env`, then `make integration-test`.

## The differential lane

`make advangrid-setup` starts the services the lane needs from upstream's `internal/recipe` plus `advangrid/recipe/docker-compose.yml`: Garage v2.2.0, Lakekeeper v0.14.3 on Postgres, and a Spark oracle. Garage, Lakekeeper and the oracle share `spark-iceberg`'s network namespace, so the warehouse endpoint `http://localhost:3900` works for Lakekeeper, Spark and tests on the host. The script creates two warehouses: `lake`, and `split`, whose endpoint `http://spark-iceberg:3900` resolves only inside the stack. It also downloads the DuckDB v2.0.0 extensions and checks them against `advangrid/recipe/duckdb-extensions.sha256`.

The oracle (`advangrid/recipe/oracle.py`) is one Spark session (Spark 3.5, Iceberg Java 1.8.1) behind `http://localhost:8899/sql`. It is the reference reader.

`make advangrid-test` runs the Go module in `advangrid/`, which replaces `github.com/apache/iceberg-go` with the checkout. `advangrid/lane` holds the helpers, and `advangrid/cases` holds the cases. Every case compares a checksum read by iceberg-go, DuckDB and Spark: the row count, the exact sum of every integer and decimal column, and timestamps summed as epoch milliseconds.

`make advangrid-down` removes the stack. On rootless podman, run the targets with `DOCKER_HOST` set to the podman socket and `PODMAN_COMPOSE_PROVIDER` set to a docker-compose v2 binary.

## Fixes and upstream candidates

PR numbers refer to this repository. Every row's PR was red on its tests-only commit and green after the fix. Whether to propose a fix upstream is decided by a person. Known limits that are not fixed here: AdvanGrid/advangrid#2654 (each commit retry re-reads every manifest, so an overwrite can starve under steady contention).

Behaviour changes for callers:

- `WriteRecords` no longer releases the batches it consumes (finding 7). Code that retained batches before yielding them, as the old doc said, must stop retaining them.
- Any partition writer error now fails the whole write, including a cancelled context (#14).
- Finding 1: Spark (Iceberg Java 1.8.1) reads unsorted position delete files correctly. The spike's misread came from Trino 484's own reader, so the case checks the files' order with DuckDB.

| Finding | Fix | Unit test | Differential case | Upstream status |
| ------- | --- | --------- | ----------------- | --------------- |
| 1. Position delete files not sorted by `(file_path, pos)` | #6: scan tasks sorted by path, one routing worker in the delete fanout | `table/pos_delete_order_test.go` `TestMergeOnReadDeleteWritesSortedPositionDeletes` | `advangrid/cases/posdelete_order_test.go` `TestPositionDeletesAcrossFilesAreSorted` | Not proposed |
| 2. Copy-on-write rewrite drops existing merge-on-read deletes | #5: rewrite plans and applies the file's positional, equality and DV deletes | `table/cow_rewrite_deletes_test.go` `TestCopyOnWriteRewriteKeepsMergeOnReadDeletes` | `advangrid/cases/cow_keeps_deletes_test.go` `TestOverwriteKeepsMergeOnReadDeletes` | Not proposed |
| 3. Bin-pack compaction loses the declared sort order | #7: `table.SortStrategy`, a streaming k-way merge by the sort order | `table/rewrite_data_files_sort_test.go`, `table/sort_merge_internal_test.go`, `table/compaction/sort_strategy_test.go` | `advangrid/cases/sort_compaction_test.go` `TestSortCompactionKeepsOrder` | Not proposed |
| 4. Fanout writer reorders a partition's batches | #8: workers add a batch's partitions in input order | `table/partitioned_fanout_writer_order_test.go` `TestAppendKeepsBatchOrderWithinPartition` | `advangrid/cases/fanout_batch_order_test.go` `TestAppendKeepsSortedBatchOrder` | Not proposed |
| 5. Data files never carry `sort_order_id` | #10: the rolling writer claims the id per file and drops it at the first order break | `table/rolling_data_writer_test.go` `TestWriterStampsSortOrderIDOnlyForFilesInOrder` | `advangrid/cases/sort_order_id_test.go` `TestSortOrderIDOnFilesInOrder` | Not proposed |
| 6. `RollingDataWriter.Add` hides the real write error | #12: the first stopping error is kept and returned | `table/rolling_data_writer_test.go` `TestRollingDataWriter/TestAddAfterStreamErrorReturnsCause` | `advangrid/cases/writer_error_test.go` `TestWriteRecordsReportsWriteCause` | Not proposed |
| 7. `WriteRecords` releases the caller's batches | #3: usual Arrow contract; internal callers release what they own | `table/write_records_test.go` `TestWriteRecords/TestCallerKeepsBatchOwnership` | `advangrid/cases/write_records_ownership_test.go` `TestWriteRecordsKeepsCallerBatches` | Not proposed |
| 8. REST table `config` overrides the client's storage endpoint | #4: `rest.WithPreferClientConfig()` / `rest.prefer-client-config=true` | `catalog/rest/table_config_test.go` `TestLoadTableStorageEndpointPrecedence` | `advangrid/cases/client_storage_wins_test.go` `TestClientStorageEndpointWins` | Not proposed |
| 9. `file_format` matched case-sensitively | #13: `DataFile.FileFormat()` upper-cases the stored value | `manifest_test.go` `TestManifests/TestReadManifestNormalizesLowerCaseFileFormat` | `advangrid/cases/file_format_case_test.go` `TestCompactsDuckDBDataFiles` | Not proposed |
| 10. Merge on append leaves its intermediate manifest | #9: the merge append deletes manifests the committed snapshot does not reference | `table/merge_append_cleanup_test.go` `TestMergeAppendLeavesNoUnreferencedManifests` | `advangrid/cases/merge_append_manifests_test.go` `TestMergeAppendLeavesNoOrphans` | Not proposed |
| AdvanGrid/advangrid#2653. A failed partition writer drops rows silently | #14: the writer factory keeps the first writer error, refuses new writers after it, and closing returns it | `table/rolling_data_writer_test.go` `TestRollingDataWriter/TestPartitionWriterFailureIsNotLost` | `advangrid/cases/partition_writer_loss_test.go` `TestAppendReportsPartitionWriterFailure` | Not proposed |
