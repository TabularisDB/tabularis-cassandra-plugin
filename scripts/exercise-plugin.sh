#!/usr/bin/env bash
# Exercises every JSON-RPC method the plugin implements against a real
# Cassandra/ScyllaDB cluster, by piping newline-delimited JSON-RPC requests
# into the built binary via stdin, exactly as Tabularis itself would.
#
# Ported from the Java v1 plugin's scripts/exercise-plugin.sh, adapted for
# two real differences from that version:
#   - Rust has no separate "interpreted vs. native" split the way the Java
#     JAR/native-image build did - a build is always a native binary. This
#     script instead runs against either the debug build (default) or the
#     release build (--release), matching CI's two build passes.
#   - The write path (json_to_cql_value, see code-logic-and-architecture.md)
#     doesn't support every CQL type yet - notably no `timestamp`. The seed
#     table below still has a `created_at timestamp` column (seeded only via
#     cqlsh, exercising the *read* path, which does support it) but
#     insert_record's request below deliberately omits it, unlike the Java
#     version's request, which could write it.
#
# Unlike the Java version, this script also verifies the responses: it fails
# if any JSON-RPC response comes back with an "error" field, or if the
# binary produced fewer responses than requests sent (a sign it crashed
# partway through).
#
# Requires: a reachable Cassandra/Scylla at $CASSANDRA_HOST:$CASSANDRA_PORT
# (defaults 127.0.0.1:9042) and cqlsh on PATH to seed a scratch keyspace.
set -euo pipefail

CASSANDRA_HOST="${CASSANDRA_HOST:-127.0.0.1}"
CASSANDRA_PORT="${CASSANDRA_PORT:-9042}"
KEYSPACE="${KEYSPACE:-tabularis_smoke}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RELEASE=false
for arg in "$@"; do
  case "$arg" in
    --release) RELEASE=true ;;
    *) echo "Unknown argument: $arg" >&2; exit 1 ;;
  esac
done

if [ "$RELEASE" = true ]; then
  BIN="$ROOT_DIR/target/release/tabularis-cassandra-plugin"
  BUILD_HINT="cargo build --release"
else
  BIN="$ROOT_DIR/target/debug/tabularis-cassandra-plugin"
  BUILD_HINT="cargo build"
fi
[ -x "$BIN" ] || { echo "Binary not found at $BIN - run '$BUILD_HINT' first" >&2; exit 1; }

echo "==> Seeding scratch keyspace \"$KEYSPACE\" on $CASSANDRA_HOST:$CASSANDRA_PORT"
cqlsh "$CASSANDRA_HOST" "$CASSANDRA_PORT" -e \
  "CREATE KEYSPACE IF NOT EXISTS $KEYSPACE WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};"
cqlsh "$CASSANDRA_HOST" "$CASSANDRA_PORT" -e \
  "CREATE TABLE IF NOT EXISTS $KEYSPACE.widgets (id uuid PRIMARY KEY, name text, weight double, tags set<text>, created_at timestamp) WITH comment = 'smoke-test table';"
cqlsh "$CASSANDRA_HOST" "$CASSANDRA_PORT" -e \
  "CREATE INDEX IF NOT EXISTS ON $KEYSPACE.widgets (name);"
cqlsh "$CASSANDRA_HOST" "$CASSANDRA_PORT" -e \
  "INSERT INTO $KEYSPACE.widgets (id, name, weight, tags, created_at) VALUES (uuid(), 'seed-row', 1.5, {'a','b'}, toTimestamp(now()));"

REQUESTS_FILE="$(mktemp)"
OUTPUT_FILE="$(mktemp)"
trap 'rm -f "$REQUESTS_FILE" "$OUTPUT_FILE"' EXIT

# Fixed id (rather than a freshly generated uuid) so insert/update/delete
# below all target the same, known row.
ROW_ID="11111111-1111-1111-1111-111111111111"

cat > "$REQUESTS_FILE" <<JSONRPC
{"jsonrpc":"2.0","method":"initialize","params":{"settings":{"local_datacenter":"datacenter1"}},"id":1}
{"jsonrpc":"2.0","method":"test_connection","params":{"params":{"host":"$CASSANDRA_HOST","port":$CASSANDRA_PORT,"database":"$KEYSPACE"}},"id":2}
{"jsonrpc":"2.0","method":"get_databases","params":{"params":{"host":"$CASSANDRA_HOST","port":$CASSANDRA_PORT,"database":"$KEYSPACE"}},"id":3}
{"jsonrpc":"2.0","method":"get_tables","params":{"params":{"host":"$CASSANDRA_HOST","port":$CASSANDRA_PORT,"database":"$KEYSPACE"},"schema":null},"id":4}
{"jsonrpc":"2.0","method":"get_columns","params":{"params":{"host":"$CASSANDRA_HOST","port":$CASSANDRA_PORT,"database":"$KEYSPACE"},"schema":null,"table":"widgets"},"id":5}
{"jsonrpc":"2.0","method":"get_indexes","params":{"params":{"host":"$CASSANDRA_HOST","port":$CASSANDRA_PORT,"database":"$KEYSPACE"},"schema":null,"table":"widgets"},"id":6}
{"jsonrpc":"2.0","method":"insert_record","params":{"params":{"host":"$CASSANDRA_HOST","port":$CASSANDRA_PORT,"database":"$KEYSPACE"},"schema":null,"table":"widgets","data":{"id":"$ROW_ID","name":"ci-row","weight":2.5}},"id":7}
{"jsonrpc":"2.0","method":"execute_query","params":{"params":{"host":"$CASSANDRA_HOST","port":$CASSANDRA_PORT,"database":"$KEYSPACE"},"query":"SELECT * FROM widgets","page":1,"page_size":1},"id":8}
{"jsonrpc":"2.0","method":"execute_query","params":{"params":{"host":"$CASSANDRA_HOST","port":$CASSANDRA_PORT,"database":"$KEYSPACE"},"query":"SELECT * FROM widgets","page":2,"page_size":1},"id":9}
{"jsonrpc":"2.0","method":"update_record","params":{"params":{"host":"$CASSANDRA_HOST","port":$CASSANDRA_PORT,"database":"$KEYSPACE"},"schema":null,"table":"widgets","pk_col":"id","pk_val":"$ROW_ID","col_name":"weight","new_val":3.5},"id":10}
{"jsonrpc":"2.0","method":"delete_record","params":{"params":{"host":"$CASSANDRA_HOST","port":$CASSANDRA_PORT,"database":"$KEYSPACE"},"schema":null,"table":"widgets","pk_col":"id","pk_val":"$ROW_ID"},"id":11}
{"jsonrpc":"2.0","method":"ping","params":{"params":{"host":"$CASSANDRA_HOST","port":$CASSANDRA_PORT,"database":"$KEYSPACE"}},"id":12}
JSONRPC

REQUEST_COUNT="$(wc -l < "$REQUESTS_FILE")"
echo "==> Running the plugin against $REQUEST_COUNT requests ($([ "$RELEASE" = true ] && echo release || echo debug) build)"

"$BIN" < "$REQUESTS_FILE" > "$OUTPUT_FILE"
cat "$OUTPUT_FILE"

RESPONSE_COUNT="$(wc -l < "$OUTPUT_FILE")"
if [ "$RESPONSE_COUNT" -ne "$REQUEST_COUNT" ]; then
  echo "==> FAILED: sent $REQUEST_COUNT requests but got $RESPONSE_COUNT responses - the binary may have crashed partway through" >&2
  exit 1
fi

if grep -q '"error"' "$OUTPUT_FILE"; then
  echo "==> FAILED: at least one response contained a JSON-RPC error:" >&2
  grep '"error"' "$OUTPUT_FILE" >&2
  exit 1
fi

echo "==> All $RESPONSE_COUNT responses succeeded with no JSON-RPC errors"
