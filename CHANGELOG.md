# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0]

Initial release.

### Added

- `initialize`, `test_connection`, `ping` connection lifecycle, with
  `local_datacenter`, `consistency_level`, `request_timeout_ms`, and
  `scylla_shard_aware` applied from the manifest's `settings`
- Schema browsing: `get_databases` (keyspaces), `get_tables`, `get_columns`,
  `get_indexes`
- `execute_query` with CQL-native forward paging (see the "Query paging &
  `total_count`" section of the README for how this differs from
  offset-based paging)
- Row editing: `insert_record`, `update_record`, `delete_record` for tables
  with a single-column primary key
- CI (`ci.yml`): builds, lints (`cargo fmt`, `cargo clippy -D warnings`), and
  runs a full functional smoke test against a real Cassandra container on
  every push
- Release automation (`release.yml`): tagged pushes build per-platform
  binaries (Linux x86_64/aarch64, macOS x86_64/aarch64, Windows x86_64) and
  publish them, plus a per-platform `.tabularium`, as GitHub Release assets
- Verified end-to-end against both a real Cassandra container and a real
  ScyllaDB container

### Known limitations

- No TLS support yet (`ssl_mode` is accepted but not acted on)
- No shard-aware/tablet-aware ScyllaDB routing yet (`scylla_shard_aware` is
  currently informational only)
- `update_record`/`delete_record` require a single-column primary key;
  composite primary keys must be edited via CQL through the query editor
- Row *writes* cover a subset of CQL scalar types (reads support the full
  range) - see the README's "Known limitations" for the exact list

[Unreleased]: https://github.com/TabularisDB/tabularis-cassandra-plugin/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/TabularisDB/tabularis-cassandra-plugin/releases/tag/v0.1.0
