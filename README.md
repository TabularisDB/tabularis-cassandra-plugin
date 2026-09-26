# tabularis-cassandra-plugin (Rust)

A [Tabularis](https://tabularis.dev) driver plugin for **Apache Cassandra**
and **ScyllaDB**, written in Rust against ScyllaDB's own
[`scylla-rust-driver`](https://github.com/scylladb/scylla-rust-driver). Built
against the [Cassandra/ScyllaDB plugin bounty](https://tabularis.dev/plugins/bounties):
keyspaces, tables, paged CQL queries, and row editing.

## Status

Implements:

- **Connection**: `initialize` (applies `local_datacenter`/`consistency_level`/
  `request_timeout_ms`/`scylla_shard_aware` from the manifest's `settings`),
  `test_connection`, `ping`
- **Schema browsing**: `get_databases` (keyspaces), `get_tables`, `get_columns`,
  `get_indexes`. `get_views`, `get_routines`, `get_triggers`, and
  `get_foreign_keys` are implemented as no-ops returning an empty list,
  since CQL has none of these concepts - see "Known limitations" for why
  they're implemented at all rather than just declared unsupported
- **Querying**: `execute_query` with CQL-native forward paging (see below)
- **Row editing**: `insert_record`, `update_record`, `delete_record` (single-
  column primary keys only - see "Known limitations")
- **CI and release automation**: every push runs a full functional smoke test
  against a real Cassandra service container; tagged releases build and
  publish per-platform binaries (Linux x86_64/aarch64, macOS x86_64/aarch64,
  Windows x86_64) via GitHub Actions
- **Verified against real ScyllaDB**, not just Cassandra: the full protocol
  surface has been exercised end-to-end against an actual ScyllaDB container,
  in addition to Cassandra

Not yet implemented: TLS. See "Known limitations" and "Roadmap" below.

## Why Cassandra and ScyllaDB share one plugin

ScyllaDB is wire-compatible with Cassandra's CQL native protocol, so a
`scylla-rust-driver`-based client talks to either without any protocol-level
branching - this mirrors the bounty's own recommendation to build the
Cassandra path first and validate ScyllaDB on top of it rather than
duplicating a second plugin.

**Cassandra vs ScyllaDB, concretely:**

- Connection settings, schema discovery, querying, and row editing all work
  identically against both - confirmed directly against real Cassandra and
  real ScyllaDB containers, not just assumed from protocol compatibility.
- ScyllaDB additionally supports *shard-aware* and *tablet-aware* routing for
  lower tail latency under load. This plugin does not implement shard-aware
  routing yet - the `scylla_shard_aware` connection setting is currently
  informational only (recorded, not acted on). Both databases work correctly
  without it; it's purely a performance optimization for high-throughput
  ScyllaDB workloads. Tracked as follow-up work.

## Building

Requires the [Rust toolchain](https://rustup.rs/) (stable). On Windows, you
also need a linker - install Visual Studio Build Tools' "Desktop development
with C++" workload (or `winget install Microsoft.VisualStudio.2022.BuildTools
--silent --override "--wait --quiet --add
Microsoft.VisualStudio.Workload.VCTools --includeRecommended"`), since Rust's
default Windows target expects MSVC's `link.exe`.

```bash
cargo build              # debug build -> target/debug/tabularis-cassandra-plugin
cargo build --release    # release build -> target/release/tabularis-cassandra-plugin
cargo test                # unit tests (none yet - this currently just confirms the harness runs)
cargo fmt --all -- --check   # formatting check, matches CI
cargo clippy --all-targets --all-features -- -D warnings   # lint, matches CI
```

CI (`.github/workflows/ci.yml`) runs all of the above plus a full functional
smoke test against a real Cassandra service container on every push.
Tagged pushes (`vX.Y.Z`) additionally trigger `.github/workflows/release.yml`,
which cross-compiles a binary for each supported platform and publishes them,
plus a per-platform `.tabularium`, as GitHub Release assets.

## Installing a release build

Download the zip for your platform from the
[latest release](https://github.com/TabularisDB/tabularis-cassandra-plugin/releases/latest),
which contains the built binary and a matching `.tabularium`. Extract both
into Tabularis's plugin directory and restart Tabularis (or reload plugins
from Settings).

## Installing locally for development

Copy the built binary and manifest into Tabularis's plugin directory, then
restart Tabularis or reload plugins from Settings:

```bash
# Linux/macOS
mkdir -p ~/.local/share/tabularis/plugins/cassandra
cp target/release/tabularis-cassandra-plugin ~/.local/share/tabularis/plugins/cassandra/
cp .tabularium ~/.local/share/tabularis/plugins/cassandra/
```

```powershell
# Windows
mkdir "$env:APPDATA\tabularis\plugins\cassandra"
copy target\release\tabularis-cassandra-plugin.exe "$env:APPDATA\tabularis\plugins\cassandra\"
copy .tabularium "$env:APPDATA\tabularis\plugins\cassandra\"
```

## Manual protocol testing

`scripts/exercise-plugin.sh` pipes a full sequence of JSON-RPC requests
(connect, browse schema, page a query, insert/update/delete a row) into the
built binary against a real Cassandra/Scylla, exactly as Tabularis itself
would, and fails if any response comes back with a JSON-RPC error:

```bash
docker run --rm -d --name smoke-cassandra -p 9042:9042 cassandra:5.0
# wait for it to come up (cqlsh 127.0.0.1 9042 -e "DESCRIBE KEYSPACES"), then:
cargo build
./scripts/exercise-plugin.sh            # against the debug build
./scripts/exercise-plugin.sh --release  # against a release build (cargo build --release first)
```

The same script has also been run unmodified against a real ScyllaDB
container (`docker run --name scylla -p 9042:9042 -d scylladb/scylla --smp 1
--memory 750M --overprovisioned 1 --broadcast-rpc-address 127.0.0.1`).

You can also drive a single request by hand:

```bash
echo '{"jsonrpc":"2.0","method":"test_connection","params":{"params":{"host":"127.0.0.1","port":9042,"database":"my_keyspace"}},"id":1}' \
  | ./target/debug/tabularis-cassandra-plugin
```

## Query paging & `total_count`

CQL has no `OFFSET`/row-number-based paging - a page is only reachable via
the opaque `PagingState` token the server returns alongside the previous
page. The plugin caches that token per query (keyed by the raw query text) so
paging forward one page at a time - the common UI pattern - resumes exactly
where it left off, without replaying earlier pages. A page requested "out of
order" (the cache doesn't already have the token leading to it) falls back to
replaying forward from the start, which is correct but costs more requests;
in practice this only happens after a process restart or a UI jump straight
to an unvisited page.

Similarly, CQL has no cheap `COUNT(*)` for an arbitrary statement - it's a
full coordinator-side scan, which this plugin deliberately never issues just
to populate a total. `total_count` is therefore the number of rows actually
counted so far, across every page fetched for that query in this process's
lifetime - accurate as "how many rows have we seen," not a claim about the
true total until the query is actually exhausted.

## Known limitations

- **Views, routines, triggers, and foreign keys don't exist in CQL**, so
  `get_views`/`get_routines`/`get_triggers`/`get_foreign_keys` always return
  an empty list. They're implemented (not just declared `false` in
  `.tabularium`) because Tabularis's schema-tree UI calls some of these
  unconditionally - in the same batched request as calls this plugin *does*
  need to succeed (`get_tables`, or `get_columns`/`get_indexes` when
  expanding a table). A driver that returns "Method not found" for any one
  call in that batch fails the whole batch, silently discarding the good
  results too (with nothing shown in the UI - Tabularis only logs it to the
  browser console). Concretely: leaving `get_views` unimplemented meant no
  table ever appeared under any keyspace, and leaving `get_foreign_keys`
  unimplemented meant expanding a table never showed its columns. Returning
  an empty list for each keeps those batches alive.
- **Composite primary keys and row editing.** Tabularis's `update_record`/
  `delete_record` protocol identifies a row with a single `pk_col`/`pk_val`
  pair. CQL primary keys are frequently composite (partition key plus
  clustering columns), which that shape can't express. Tables with a
  composite primary key are therefore browsable and queryable, but not
  editable through the grid - edit them with CQL via the query editor
  instead. `insert_record` is unaffected, since it supplies every column
  explicitly.
- **Row editing only covers a subset of CQL scalar types.** `insert_record`/
  `update_record` currently accept text/ascii, boolean, int, bigint,
  smallint, tinyint, float, double, blob (as hex), and uuid. Decimal,
  duration, date/time/timestamp, varint, counter, timeuuid, and any
  collection/tuple/user-defined-type column can't be written through the
  grid yet - write those via CQL through the query editor. All of these
  types, plus collections/tuples/UDTs, display correctly when reading
  (`execute_query`); only the write side is narrower.
- **No TLS.** The `ssl_mode` connection field is accepted but not yet acted
  on - `.tabularium` accordingly declares `supports_ssl: false`.
- **No shard-aware ScyllaDB routing** - see "Why Cassandra and ScyllaDB share
  one plugin" above.
- **`request_timeout_ms` only governs per-statement timeouts**, not the
  initial connection handshake - the driver's own default applies there.
- **Secondary index columns** are reported using CQL's raw index target
  expression (e.g. `values(tags)`) rather than a parsed column list, since
  CQL index targets are expressions, not always plain columns.

## Configuration (`.tabularium` settings)

| Setting | Default | Notes |
|---|---|---|
| `local_datacenter` | `datacenter1` | Required by the driver's default load-balancing policy; must match a real datacenter name in your cluster. |
| `consistency_level` | `LOCAL_QUORUM` | Any standard CQL consistency level; an unrecognized value falls back to `LOCAL_QUORUM` rather than failing the connection. |
| `request_timeout_ms` | `10000` | Applies to per-request timeouts only - see "Known limitations". |
| `scylla_shard_aware` | `false` | Informational only for now - see "Known limitations". |

## Roadmap

- TLS support (`rustls`, most likely).
- Wider read/write type coverage (see "Known limitations").
- Shard-aware ScyllaDB routing.

## Contributing

Bug reports and pull requests are welcome - please open an issue first for
anything beyond a small fix. See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
for community expectations and [CHANGELOG.md](CHANGELOG.md) for release
history.

## License

Apache License 2.0 - see [LICENSE](LICENSE).
