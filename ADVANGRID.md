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

Upstream's own workflows are disabled in this repository.

To run the upstream integration lane locally: `make integration-setup`, export the variables printed by `make integration-env`, then `make integration-test`.

## Fixes and upstream candidates

| Finding | Fix | Unit test | Differential case | Upstream status |
| ------- | --- | --------- | ----------------- | --------------- |
