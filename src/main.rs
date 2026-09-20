use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::cluster::metadata::{Column, ColumnKind, ColumnType, NativeType, Table};
use scylla::cluster::ClusterState;
use scylla::deserialize::row::ColumnIterator;
use scylla::deserialize::value::DeserializeValue;
use scylla::policies::load_balancing::DefaultPolicy;
use scylla::response::PagingState;
use scylla::statement::unprepared::Statement;
use scylla::statement::Consistency;
use scylla::value::CqlValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Deserialize, Debug)]
struct RpcRequest {
    #[allow(dead_code)]
    jsonrpc: String,
    id: serde_json::Value,
    method: String,
    #[serde(default)]
    params: serde_json::Value,
}

#[derive(Serialize)]
struct RpcResponse {
    jsonrpc: &'static str,
    id: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError>,
}

#[derive(Serialize)]
struct RpcError {
    code: i32,
    message: String,
}

#[derive(Deserialize, Debug)]
struct ConnectionParams {
    #[allow(dead_code)]
    driver: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    database: Option<String>,
    username: Option<String>,
    password: Option<String>,
    #[allow(dead_code)]
    ssl_mode: Option<String>,
}

// Values delivered by the host via the optional "initialize" RPC call (see
// the "settings" array in .tabularium). Ported from the Java plugin's
// PluginSettings: an immutable-per-connection snapshot that a fresh
// "initialize" call simply replaces (see AppState::apply_settings below).
//
// We build this by hand from serde_json::Value (extract_plugin_settings,
// further down) rather than #[derive(Deserialize)] like ConnectionParams -
// each field needs its own independent default when missing or the wrong
// JSON type, which is exactly what the Java version's
// `settingsNode.path("x").asText(DEFAULTS.x)` idiom does. A derive would
// need a `#[serde(default = "...")]` function per field to get the same
// effect, which ends up less readable than just writing it out.
#[derive(Debug, Clone)]
struct PluginSettings {
    local_datacenter: String,
    consistency_level: String,
    request_timeout_ms: u64,
    // Hints that the cluster is ScyllaDB and the driver should prefer the
    // shard-aware native port if the caller connects through one. Kept as a
    // documented no-op for now, same as the Java plugin - shard-aware
    // routing isn't implemented yet.
    #[allow(dead_code)]
    scylla_shard_aware: bool,
}

impl Default for PluginSettings {
    fn default() -> Self {
        PluginSettings {
            local_datacenter: "datacenter1".to_string(),
            consistency_level: "LOCAL_QUORUM".to_string(),
            request_timeout_ms: 10_000,
            scylla_shard_aware: false,
        }
    }
}

// Tracks how far we've gotten through one query's result set. CQL paging
// is forward-only: the driver hands back an opaque `PagingState` token
// after each page that means "resume exactly here" - there's no
// "jump to row N" the way SQL's OFFSET pretends to offer. So the cache
// buys us cheap *sequential* paging (the common case - a user clicking
// "next page" again and again) while staying honest that going backwards
// or skipping ahead means replaying from the start.
struct QueryProgress {
    next_page: u32,
    paging_state: PagingState,
    rows_seen: i64,
    exhausted: bool,
}

impl QueryProgress {
    fn fresh() -> Self {
        QueryProgress {
            next_page: 1,
            paging_state: PagingState::start(),
            rows_seen: 0,
            exhausted: false,
        }
    }
}

// One connection per process (see lesson 5), so a single Session slot was
// enough. Queries are different: a user can have several queries open in
// Tabularis's UI at once, each paging independently, so this really does
// need a cache keyed by something per-query - the raw query text is a
// simple (if imperfect - two *different* uses of identical query text
// would share a cache slot) key to start with.
struct AppState {
    session: Mutex<Option<Arc<Session>>>,
    queries: Mutex<HashMap<String, QueryProgress>>,
    settings: Mutex<PluginSettings>,
}

impl AppState {
    fn new() -> Self {
        AppState {
            session: Mutex::new(None),
            queries: Mutex::new(HashMap::new()),
            settings: Mutex::new(PluginSettings::default()),
        }
    }

    // Called from the "initialize" handler. A later "initialize" call (the
    // host may send one again, e.g. if the user edits the connection's
    // settings) simply replaces the snapshot - it does NOT reopen an
    // already-cached session, matching the Java plugin's behavior (settings
    // only take effect for sessions opened *after* they're applied).
    async fn apply_settings(&self, settings: PluginSettings) {
        *self.settings.lock().await = settings;
    }

    async fn get_or_connect(&self, params: &ConnectionParams) -> Result<Arc<Session>, RpcError> {
        let mut guard = self.session.lock().await;
        if let Some(session) = guard.as_ref() {
            return Ok(Arc::clone(session));
        }
        let settings = self.settings.lock().await.clone();
        let session = Arc::new(connect(params, &settings).await?);
        *guard = Some(Arc::clone(&session));
        Ok(session)
    }
}

#[tokio::main]
async fn main() {
    let stdin = io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    let mut stdout = io::stdout();
    let state = AppState::new();

    loop {
        let line = match lines.next_line().await {
            Ok(Some(text)) => text,
            Ok(None) => break,
            Err(e) => {
                eprintln!("error reading stdin: {e}");
                continue;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        let response = match serde_json::from_str::<RpcRequest>(&line) {
            Ok(request) => {
                let id = request.id.clone();
                let outcome = handle(&state, request).await;
                build_response(id, outcome)
            }
            Err(e) => {
                eprintln!("parse error: {e}");
                build_response(
                    serde_json::Value::Null,
                    Err(RpcError {
                        code: -32700,
                        message: "Parse error".to_string(),
                    }),
                )
            }
        };

        if let Err(e) = write_response(&mut stdout, &response).await {
            eprintln!("error writing stdout: {e}");
        }
    }
}

async fn handle(state: &AppState, request: RpcRequest) -> Result<serde_json::Value, RpcError> {
    match request.method.as_str() {
        "initialize" => {
            let settings = extract_plugin_settings(&request.params);
            state.apply_settings(settings).await;
            Ok(serde_json::Value::Null)
        }

        "test_connection" => {
            let params = extract_connection_params(&request.params)?;
            state.get_or_connect(&params).await?;
            Ok(serde_json::json!({ "success": true }))
        }
        "ping" => {
            let params = extract_connection_params(&request.params)?;
            state.get_or_connect(&params).await?;
            Ok(serde_json::Value::Null)
        }

        "get_databases" => {
            let params = extract_connection_params(&request.params)?;
            let session = state.get_or_connect(&params).await?;
            let names: Vec<String> = session
                .get_cluster_state()
                .keyspaces_iter()
                .map(|(name, _)| name.to_string())
                .collect();
            Ok(serde_json::json!(names))
        }

        "get_tables" => {
            let params = extract_connection_params(&request.params)?;
            let schema_filter = extract_schema_filter(&request.params);
            let session = state.get_or_connect(&params).await?;
            let tables = list_tables(session.as_ref(), schema_filter.as_deref());
            Ok(serde_json::json!(tables))
        }

        "get_columns" => {
            let params = extract_connection_params(&request.params)?;
            let schema_filter = extract_schema_filter(&request.params);
            let table_name = extract_table_name(&request.params)?;
            let session = state.get_or_connect(&params).await?;
            let columns = list_columns(session.as_ref(), schema_filter.as_deref(), &table_name)?;
            Ok(serde_json::json!(columns))
        }

        "get_indexes" => {
            let params = extract_connection_params(&request.params)?;
            let schema_filter = extract_schema_filter(&request.params);
            let table_name = extract_table_name(&request.params)?;
            let session = state.get_or_connect(&params).await?;

            let keyspace = schema_filter.or_else(|| params.database.clone()).ok_or_else(|| RpcError {
                code: -32602,
                message: "get_indexes needs a keyspace: pass a schema, or connect with a default database".to_string(),
            })?;

            let indexes = list_indexes(session.as_ref(), &keyspace, &table_name).await?;
            Ok(serde_json::json!(indexes))
        }

        "execute_query" => {
            let params = extract_connection_params(&request.params)?;
            let (query, page, page_size) = extract_execute_query_params(&request.params)?;
            let session = state.get_or_connect(&params).await?;
            execute_query(state, session.as_ref(), &query, page, page_size).await
        }

        "insert_record" => {
            let params = extract_connection_params(&request.params)?;
            let schema_filter = extract_schema_filter(&request.params);
            let table_name = extract_table_name(&request.params)?;
            let data = extract_data(&request.params)?;
            let session = state.get_or_connect(&params).await?;
            insert_record(
                session.as_ref(),
                schema_filter.as_deref(),
                &table_name,
                &data,
            )
            .await
        }

        "update_record" => {
            let params = extract_connection_params(&request.params)?;
            let schema_filter = extract_schema_filter(&request.params);
            let table_name = extract_table_name(&request.params)?;
            let (pk_col, pk_val) = extract_pk(&request.params)?;
            let (col_name, new_val) = extract_update_fields(&request.params)?;
            let session = state.get_or_connect(&params).await?;
            update_record(
                session.as_ref(),
                schema_filter.as_deref(),
                &table_name,
                &pk_col,
                &pk_val,
                &col_name,
                &new_val,
            )
            .await
        }

        "delete_record" => {
            let params = extract_connection_params(&request.params)?;
            let schema_filter = extract_schema_filter(&request.params);
            let table_name = extract_table_name(&request.params)?;
            let (pk_col, pk_val) = extract_pk(&request.params)?;
            let session = state.get_or_connect(&params).await?;
            delete_record(
                session.as_ref(),
                schema_filter.as_deref(),
                &table_name,
                &pk_col,
                &pk_val,
            )
            .await
        }

        other => Err(RpcError {
            code: -32601,
            message: format!("Method not found: {other}"),
        }),
    }
}

fn extract_connection_params(params: &serde_json::Value) -> Result<ConnectionParams, RpcError> {
    let inner = params.get("params").ok_or_else(|| RpcError {
        code: -32602,
        message: "Missing params.params".to_string(),
    })?;
    serde_json::from_value(inner.clone()).map_err(|e| RpcError {
        code: -32602,
        message: format!("Invalid connection params: {e}"),
    })
}

// "initialize"'s params look like { "settings": { "local_datacenter": ...,
// ... } } - a sibling of the "params" key that extract_connection_params
// reads for every other method. Every field is optional and independently
// defaulted, same as the Java plugin: a host that only sends
// { "settings": { "consistency_level": "ONE" } } still gets sane defaults
// for local_datacenter/request_timeout_ms/scylla_shard_aware.
fn extract_plugin_settings(params: &serde_json::Value) -> PluginSettings {
    let defaults = PluginSettings::default();
    let settings = match params.get("settings") {
        Some(s) if s.is_object() => s,
        _ => return defaults,
    };
    PluginSettings {
        local_datacenter: settings
            .get("local_datacenter")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or(defaults.local_datacenter),
        consistency_level: settings
            .get("consistency_level")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or(defaults.consistency_level),
        request_timeout_ms: settings
            .get("request_timeout_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(defaults.request_timeout_ms),
        scylla_shard_aware: settings
            .get("scylla_shard_aware")
            .and_then(|v| v.as_bool())
            .unwrap_or(defaults.scylla_shard_aware),
    }
}

// Mirrors the Java plugin's resolveConsistency: an unrecognized or missing
// value doesn't fail the connection, it just falls back to LOCAL_QUORUM -
// the same default .tabularium advertises for the setting itself.
fn parse_consistency(name: &str) -> Consistency {
    match name.trim().to_uppercase().as_str() {
        "ANY" => Consistency::Any,
        "ONE" => Consistency::One,
        "TWO" => Consistency::Two,
        "THREE" => Consistency::Three,
        "QUORUM" => Consistency::Quorum,
        "ALL" => Consistency::All,
        "LOCAL_QUORUM" => Consistency::LocalQuorum,
        "EACH_QUORUM" => Consistency::EachQuorum,
        "LOCAL_ONE" => Consistency::LocalOne,
        _ => Consistency::LocalQuorum,
    }
}

fn extract_schema_filter(params: &serde_json::Value) -> Option<String> {
    params
        .get("schema")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn extract_table_name(params: &serde_json::Value) -> Result<String, RpcError> {
    params
        .get("table")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| RpcError {
            code: -32602,
            message: "Missing required param: table".to_string(),
        })
}

fn extract_execute_query_params(
    params: &serde_json::Value,
) -> Result<(String, u32, i32), RpcError> {
    let query = params
        .get("query")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| RpcError {
            code: -32602,
            message: "Missing required param: query".to_string(),
        })?;
    let page = params
        .get("page")
        .and_then(|v| v.as_u64())
        .unwrap_or(1)
        .max(1) as u32;
    let page_size = params
        .get("page_size")
        .and_then(|v| v.as_i64())
        .unwrap_or(100)
        .max(1) as i32;
    Ok((query, page, page_size))
}

fn extract_data(
    params: &serde_json::Value,
) -> Result<serde_json::Map<String, serde_json::Value>, RpcError> {
    let data = params
        .get("data")
        .and_then(|v| v.as_object())
        .ok_or_else(|| RpcError {
            code: -32602,
            message: "\"data\" must be an object of column -> value".to_string(),
        })?;
    if data.is_empty() {
        return Err(RpcError {
            code: -32602,
            message: "\"data\" must be a non-empty object of column -> value".to_string(),
        });
    }
    Ok(data.clone())
}

// Shared by update_record and delete_record - both identify a row the same
// way, via a single primary-key column/value pair (see the composite-key
// note on require_single_column_primary_key below).
fn extract_pk(params: &serde_json::Value) -> Result<(String, serde_json::Value), RpcError> {
    let pk_col = params
        .get("pk_col")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| RpcError {
            code: -32602,
            message: "Missing required param: pk_col".to_string(),
        })?;
    let pk_val = params.get("pk_val").cloned().ok_or_else(|| RpcError {
        code: -32602,
        message: "Missing required param: pk_val".to_string(),
    })?;
    Ok((pk_col, pk_val))
}

fn extract_update_fields(
    params: &serde_json::Value,
) -> Result<(String, serde_json::Value), RpcError> {
    let col_name = params
        .get("col_name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| RpcError {
            code: -32602,
            message: "Missing required param: col_name".to_string(),
        })?;
    let new_val = params.get("new_val").cloned().ok_or_else(|| RpcError {
        code: -32602,
        message: "Missing required param: new_val".to_string(),
    })?;
    Ok((col_name, new_val))
}

async fn connect(
    params: &ConnectionParams,
    settings: &PluginSettings,
) -> Result<Session, RpcError> {
    let host = params.host.as_deref().unwrap_or("127.0.0.1");
    let port = params.port.unwrap_or(9042);

    // local_datacenter: the default load-balancing policy needs to know
    // which DC is "local" so it prefers replicas there - same reason the
    // Java plugin's CqlSessionBuilder calls withLocalDatacenter. Without
    // this, DefaultPolicy still works but picks a preferred DC on its own,
    // which is not necessarily the one the user configured.
    let policy = DefaultPolicy::builder()
        .prefer_datacenter(settings.local_datacenter.clone())
        .build();

    // consistency_level / request_timeout_ms: both configured per-session
    // via an ExecutionProfile, then installed as the session's *default*
    // profile handle - individual statements can still override either one
    // later by building their own profile, same as the Java driver's
    // per-statement consistency override.
    //
    // Note one gap versus the Java version: CqlSessionBuilder sets both
    // REQUEST_TIMEOUT and CONNECTION_CONNECT_TIMEOUT from request_timeout_ms.
    // The Rust driver's ExecutionProfile only exposes a request timeout
    // (applies to statement execution) - there's no separate connect-timeout
    // knob on the profile, so the driver's own default governs how long
    // initial connection setup can take. Worth a closer look later; not a
    // blocker for now.
    let profile = ExecutionProfile::builder()
        .load_balancing_policy(policy)
        .consistency(parse_consistency(&settings.consistency_level))
        .request_timeout(Some(Duration::from_millis(settings.request_timeout_ms)))
        .build();

    let mut builder = SessionBuilder::new()
        .known_node(format!("{host}:{port}"))
        .default_execution_profile_handle(profile.into_handle());
    if let (Some(user), Some(pass)) = (&params.username, &params.password) {
        builder = builder.user(user, pass);
    }

    let session = builder.build().await.map_err(|e| RpcError {
        code: -32603,
        message: format!("Connection failed: {e}"),
    })?;

    if let Some(keyspace) = &params.database {
        if !keyspace.is_empty() {
            session
                .use_keyspace(keyspace, false)
                .await
                .map_err(|e| RpcError {
                    code: -32603,
                    message: format!("Connection failed: could not use keyspace: {e}"),
                })?;
        }
    }

    Ok(session)
}

fn list_tables(session: &Session, schema_filter: Option<&str>) -> Vec<serde_json::Value> {
    let cluster = session.get_cluster_state();
    let mut tables = Vec::new();

    for (keyspace_name, keyspace) in cluster.keyspaces_iter() {
        if let Some(filter) = schema_filter {
            if keyspace_name != filter {
                continue;
            }
        }
        for table_name in keyspace.tables.keys() {
            tables.push(serde_json::json!({
                "name": table_name,
                "schema": keyspace_name,
                "comment": serde_json::Value::Null,
            }));
        }
    }

    tables
}

fn find_table<'a>(
    cluster: &'a ClusterState,
    schema_filter: Option<&str>,
    table_name: &str,
) -> Option<&'a Table> {
    for (keyspace_name, keyspace) in cluster.keyspaces_iter() {
        if let Some(filter) = schema_filter {
            if keyspace_name != filter {
                continue;
            }
        }
        if let Some(table) = keyspace.tables.get(table_name) {
            return Some(table);
        }
    }
    None
}

// Like find_table, but for mutations rather than reads - and stricter about
// it. get_columns/get_tables are fine silently returning the first match if
// a table name happens to exist in more than one keyspace with no schema
// given (worst case you see an extra row you didn't expect). Silently
// writing to the wrong keyspace's table because of that same ambiguity is a
// much worse failure mode, so this version treats "found in more than one
// keyspace" as an error rather than picking one arbitrarily.
fn resolve_table<'a>(
    cluster: &'a ClusterState,
    schema_filter: Option<&str>,
    table_name: &str,
) -> Result<(&'a str, &'a Table), RpcError> {
    let mut found: Option<(&'a str, &'a Table)> = None;
    for (keyspace_name, keyspace) in cluster.keyspaces_iter() {
        if let Some(filter) = schema_filter {
            if keyspace_name != filter {
                continue;
            }
        }
        if let Some(table) = keyspace.tables.get(table_name) {
            if found.is_some() {
                return Err(RpcError {
                    code: -32602,
                    message: format!(
                        "\"{table_name}\" exists in more than one keyspace; pass \"schema\" to disambiguate"
                    ),
                });
            }
            found = Some((keyspace_name, table));
        }
    }
    found.ok_or_else(|| RpcError {
        code: -32602,
        message: format!("Table not found: {table_name}"),
    })
}

fn find_column<'a>(table: &'a Table, name: &str) -> Result<&'a Column, RpcError> {
    table.columns.get(name).ok_or_else(|| RpcError {
        code: -32602,
        message: format!("Column \"{name}\" not found"),
    })
}

fn primary_key_columns(table: &Table) -> Vec<&String> {
    table
        .partition_key
        .iter()
        .chain(table.clustering_key.iter())
        .collect()
}

fn is_primary_key_column(table: &Table, name: &str) -> bool {
    primary_key_columns(table)
        .iter()
        .any(|c| c.as_str() == name)
}

// The protocol identifies the row to update/delete with a single
// pk_col/pk_val pair (see plugins/PLUGIN_GUIDE.md) - it has no way to
// express a composite CQL primary key (partition key + clustering columns).
// The Java v1 implementation hit this exact same host-protocol limitation
// and made the same call: refuse cleanly on composite-key tables rather
// than guess which clustering columns the caller meant, and point at
// execute_query (raw CQL) as the escape hatch. insert_record doesn't have
// this problem - it's given every column explicitly, composite key or not.
fn require_single_column_primary_key<'a>(
    table: &'a Table,
    pk_col: &str,
    table_name: &str,
    operation: &str,
) -> Result<&'a str, RpcError> {
    let pk_columns = primary_key_columns(table);
    if pk_columns.len() != 1 {
        return Err(RpcError {
            code: -32602,
            message: format!(
                "Table \"{table_name}\" has a composite primary key ({} columns); row {operation} isn't supported for it yet - use execute_query with CQL instead",
                pk_columns.len()
            ),
        });
    }
    let actual = pk_columns[0].as_str();
    if actual != pk_col {
        return Err(RpcError {
            code: -32602,
            message: format!(
                "\"{pk_col}\" is not the primary key of \"{table_name}\" (it's \"{actual}\")"
            ),
        });
    }
    Ok(actual)
}

fn list_columns(
    session: &Session,
    schema_filter: Option<&str>,
    table_name: &str,
) -> Result<Vec<serde_json::Value>, RpcError> {
    let cluster = session.get_cluster_state();
    let table = find_table(&cluster, schema_filter, table_name).ok_or_else(|| RpcError {
        code: -32602,
        message: format!("Table not found: {table_name}"),
    })?;

    let mut columns: Vec<serde_json::Value> = table
        .columns
        .iter()
        .map(|(name, column)| {
            let is_key = matches!(
                column.kind,
                ColumnKind::PartitionKey | ColumnKind::Clustering
            );
            serde_json::json!({
                "name": name,
                "data_type": cql_type_name(&column.typ),
                "is_nullable": !is_key,
                "default_value": serde_json::Value::Null,
                "is_pk": is_key,
                "is_auto_increment": false,
                "comment": serde_json::Value::Null,
            })
        })
        .collect();

    columns.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));

    Ok(columns)
}

// Real secondary-index listing, now that we have query execution: Cassandra
// keeps index definitions in a system table rather than in the same
// metadata snapshot as tables/columns, so this is a normal CQL SELECT
// rather than a get_cluster_state() lookup - a nice contrast with the
// hand-rolled paging loop in execute_query below. `query_unpaged` is the
// right call here (not query_single_page): system_schema.indexes for one
// table is always a handful of rows, never worth paging.
async fn list_indexes(
    session: &Session,
    keyspace: &str,
    table_name: &str,
) -> Result<Vec<serde_json::Value>, RpcError> {
    let statement = Statement::new(
        "SELECT index_name, options FROM system_schema.indexes WHERE keyspace_name = ? AND table_name = ?",
    );

    let query_result = session
        .query_unpaged(statement, (keyspace, table_name))
        .await
        .map_err(|e| RpcError {
            code: -32603,
            message: format!("Index lookup failed: {e}"),
        })?;

    let rows_result = query_result.into_rows_result().map_err(|e| RpcError {
        code: -32603,
        message: format!("Index lookup did not return rows: {e}"),
    })?;

    let typed_rows = rows_result
        .rows::<(String, Option<HashMap<String, String>>)>()
        .map_err(|e| RpcError {
            code: -32603,
            message: format!("Failed to read index rows: {e}"),
        })?;

    let mut indexes = Vec::new();
    for row in typed_rows {
        let (index_name, options) = row.map_err(|e| RpcError {
            code: -32603,
            message: format!("Failed to read index row: {e}"),
        })?;
        // The "target" option holds the indexed column name for a plain
        // secondary index (collection indexes use forms like
        // "values(tags)" - left as-is here rather than parsed further).
        let target = options
            .and_then(|o| o.get("target").cloned())
            .unwrap_or_default();
        indexes.push(serde_json::json!({
            "index_name": index_name,
            "columns": [target],
            // Cassandra secondary indexes are never unique or primary-key
            // constraints - that concept doesn't exist for them.
            "is_unique": false,
            "is_primary": false,
        }));
    }

    Ok(indexes)
}

// The core of this lesson. `page` is 1-indexed and forward-only under the
// hood: to serve page N we need the PagingState that resulted from
// fetching page N-1. The cache stores exactly one checkpoint per query -
// "as of page X, here's the state to fetch page X+1" - so repeatedly
// asking for the next page (the common case) is cheap, while asking for a
// page we've gone past means starting over and replaying forward, which is
// the only honest option CQL paging leaves us.
async fn execute_query(
    state: &AppState,
    session: &Session,
    query: &str,
    page: u32,
    page_size: i32,
) -> Result<serde_json::Value, RpcError> {
    let mut cache = state.queries.lock().await;
    let mut progress = cache.remove(query).unwrap_or_else(QueryProgress::fresh);

    if page < progress.next_page {
        // Asked for a page we've already passed - CQL paging can't go
        // backwards, so the only honest option is to start over.
        progress = QueryProgress::fresh();
    }

    if progress.exhausted {
        // We already learned, from a previous call, that this query has
        // fewer pages than what's being asked for now - nothing to fetch,
        // this is just an empty page past the end of the data. Returning
        // early here also means we never touch `progress.paging_state`,
        // so there's no stale/expired state to worry about reusing.
        let rows_seen = progress.rows_seen;
        cache.insert(query.to_string(), progress);
        return Ok(serde_json::json!({
            "columns": Vec::<String>::new(),
            "rows": Vec::<Vec<serde_json::Value>>::new(),
            "total_count": rows_seen,
            "execution_time_ms": 0,
        }));
    }

    // Destructuring into plain local variables (rather than keeping this as
    // one struct) sidesteps a partial-move headache: `paging_state` gets
    // moved into query_single_page() below and reassigned each loop turn,
    // which is awkward to do cleanly through a struct field but is exactly
    // what a handful of separate `mut` locals are for.
    let QueryProgress {
        mut next_page,
        mut paging_state,
        mut rows_seen,
        mut exhausted,
    } = progress;

    let statement = Statement::new(query).with_page_size(page_size);
    let mut page_result: Option<(Vec<String>, Vec<Vec<serde_json::Value>>)> = None;

    // `loop` + an explicit `break` here instead of `while next_page <= page
    // && !exhausted` - which is what I actually wrote first, and which
    // rustc rejected. The reason is worth understanding: `paging_state` is
    // *moved* into query_single_page() each pass, and only the Continue
    // arm below puts a new value back. A `while` condition checking a
    // `bool` flag doesn't prove anything to the borrow checker about
    // whether the loop body runs again - it just sees a flag that could
    // still be false next time round, and refuses to assume otherwise. An
    // explicit `break` *inside* the arm that stops reassigning
    // `paging_state` is what actually proves "this value is never touched
    // again" - structurally, not by trusting a runtime flag.
    loop {
        if next_page > page {
            break;
        }

        let (query_result, paging_state_response) = session
            .query_single_page(statement.clone(), &[], paging_state)
            .await
            .map_err(|e| RpcError {
                code: -32603,
                message: format!("Query failed: {e}"),
            })?;

        let rows_result = query_result.into_rows_result().map_err(|e| RpcError {
            code: -32603,
            message: format!("Query did not return rows: {e}"),
        })?;

        rows_seen += rows_result.rows_num() as i64;

        if next_page == page {
            page_result = Some(rows_result_to_json(&rows_result)?);
        }

        match paging_state_response.into_paging_control_flow() {
            ControlFlow::Break(()) => {
                exhausted = true;
                next_page += 1;
                // Reassigned even though it'll never be read again while
                // exhausted stays true (the early-return above short-
                // circuits before ever reaching query_single_page) -
                // needed so every path through this loop leaves
                // `paging_state` initialized, which is what satisfies the
                // borrow checker per the comment above the loop.
                paging_state = PagingState::start();
                break;
            }
            ControlFlow::Continue(new_state) => {
                paging_state = new_state;
                next_page += 1;
            }
        }
    }

    cache.insert(
        query.to_string(),
        QueryProgress {
            next_page,
            paging_state,
            rows_seen,
            exhausted,
        },
    );

    // Asking for a page past the end of the data (e.g. page 5 of a
    // 2-page result) is a well-formed request, not an error - it's just
    // empty. `total_count` is honest rather than an estimate: it's the
    // number of rows we've actually counted while paging through so far,
    // not a full pre-scan - Cassandra has no cheap way to know the true
    // total without one.
    let (columns, rows) = page_result.unwrap_or_default();
    Ok(serde_json::json!({
        "columns": columns,
        "rows": rows,
        "total_count": rows_seen,
        "execution_time_ms": 0,
    }))
}

// insert_record supplies every column explicitly, so - unlike update/delete
// below - it works fine even on tables with a composite primary key: there's
// no existing row to identify, just a new one to write.
//
// The values here are a *dynamic*, mixed-type set (however many columns the
// caller sent, whatever their CQL types are) - a fundamentally different
// shape from every query so far, which have all bound a fixed, known-at-
// compile-time tuple (`(keyspace, table_name)` in list_indexes, `&[]` in
// execute_query). Verified against the driver's own docs rather than
// guessed: `HashMap<String, T>` implements the driver's SerializeRow trait
// directly when T: SerializeValue, and CqlValue itself implements
// SerializeValue (it's used for both directions - reading rows back out,
// as in rows_result_to_json below, and sending values in, here). CQL's
// named bind markers (`:col_name`, instead of positional `?`s) are what let
// a HashMap's unordered keys line up with the right placeholder.
async fn insert_record(
    session: &Session,
    schema_filter: Option<&str>,
    table_name: &str,
    data: &serde_json::Map<String, serde_json::Value>,
) -> Result<serde_json::Value, RpcError> {
    let cluster = session.get_cluster_state();
    let (keyspace, table) = resolve_table(&cluster, schema_filter, table_name)?;

    let mut bindings: HashMap<String, Option<CqlValue>> = HashMap::new();
    let mut column_names = Vec::new();
    for (column_name, json_value) in data {
        let column = find_column(table, column_name)?;
        let cql_value = json_to_cql_value(json_value, &column.typ)?;
        column_names.push(column_name.clone());
        bindings.insert(column_name.clone(), cql_value);
    }

    let placeholders: Vec<String> = column_names.iter().map(|c| format!(":{c}")).collect();
    // Column/table/keyspace names go straight into the CQL text rather than
    // as bound values - CQL has no way to parameterize an identifier, only
    // a value. That's normally the classic string-building-a-query danger
    // sign, but every identifier landing here has already been confirmed,
    // above, to be a real column that exists on a real table resolved from
    // the driver's own cluster metadata - not raw, unchecked input.
    let cql = format!(
        "INSERT INTO {keyspace}.{table_name} ({}) VALUES ({})",
        column_names.join(", "),
        placeholders.join(", "),
    );

    let statement = Statement::new(cql);
    session
        .query_unpaged(statement, bindings)
        .await
        .map_err(|e| RpcError {
            code: -32603,
            message: format!("Insert failed: {e}"),
        })?;

    Ok(serde_json::Value::Null)
}

async fn update_record(
    session: &Session,
    schema_filter: Option<&str>,
    table_name: &str,
    pk_col: &str,
    pk_val: &serde_json::Value,
    col_name: &str,
    new_val: &serde_json::Value,
) -> Result<serde_json::Value, RpcError> {
    let cluster = session.get_cluster_state();
    let (keyspace, table) = resolve_table(&cluster, schema_filter, table_name)?;
    let pk_col = require_single_column_primary_key(table, pk_col, table_name, "update")?;

    if is_primary_key_column(table, col_name) {
        return Err(RpcError {
            code: -32602,
            message: format!(
                "\"{col_name}\" is part of the primary key and cannot be changed in place; delete and re-insert the row instead"
            ),
        });
    }

    let target_column = find_column(table, col_name)?;
    let new_cql_value = json_to_cql_value(new_val, &target_column.typ)?;
    let pk_column = find_column(table, pk_col)?;
    let pk_cql_value = json_to_cql_value(pk_val, &pk_column.typ)?;

    let mut bindings: HashMap<String, Option<CqlValue>> = HashMap::new();
    bindings.insert("new_val".to_string(), new_cql_value);
    bindings.insert("pk_val".to_string(), pk_cql_value);

    let cql = format!(
        "UPDATE {keyspace}.{table_name} SET {col_name} = :new_val WHERE {pk_col} = :pk_val"
    );
    let statement = Statement::new(cql);
    session
        .query_unpaged(statement, bindings)
        .await
        .map_err(|e| RpcError {
            code: -32603,
            message: format!("Update failed: {e}"),
        })?;

    // CQL UPDATE is an upsert, and the native protocol reports no
    // affected-row count outside of lightweight transactions (IF EXISTS /
    // IF conditions, which we're not using) - a successful execute means
    // exactly the one targeted row was written. Same reasoning the Java v1
    // implementation used for this exact same gap in what CQL can tell you.
    Ok(serde_json::json!(1))
}

async fn delete_record(
    session: &Session,
    schema_filter: Option<&str>,
    table_name: &str,
    pk_col: &str,
    pk_val: &serde_json::Value,
) -> Result<serde_json::Value, RpcError> {
    let cluster = session.get_cluster_state();
    let (keyspace, table) = resolve_table(&cluster, schema_filter, table_name)?;
    let pk_col = require_single_column_primary_key(table, pk_col, table_name, "delete")?;

    let pk_column = find_column(table, pk_col)?;
    let pk_cql_value = json_to_cql_value(pk_val, &pk_column.typ)?;

    let mut bindings: HashMap<String, Option<CqlValue>> = HashMap::new();
    bindings.insert("pk_val".to_string(), pk_cql_value);

    let cql = format!("DELETE FROM {keyspace}.{table_name} WHERE {pk_col} = :pk_val");
    let statement = Statement::new(cql);
    session
        .query_unpaged(statement, bindings)
        .await
        .map_err(|e| RpcError {
            code: -32603,
            message: format!("Delete failed: {e}"),
        })?;

    Ok(serde_json::json!(1))
}

// There's no ready-made "untyped Row" type in this driver version (my
// first attempt reached for one - `scylla::deserialize::row::Row` - and
// it doesn't exist; the compiler was right to reject it). What the driver
// gives us instead is lower-level but just as workable: `ColumnIterator`
// (a type that implements DeserializeRow, meaning it's a valid stand-in
// for "I don't know this row's shape, give me raw access to each column")
// yields one `RawColumn` per column, each carrying its own `spec` (name +
// CQL type) and raw byte `slice`. `Option<CqlValue>::deserialize(typ,
// slice)` - going through Option rather than bare CqlValue - is what
// turns those raw bytes into an actual value, with `None` meaning a SQL
// NULL rather than an error.
fn rows_result_to_json(
    rows_result: &scylla::response::query_result::QueryRowsResult,
) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>), RpcError> {
    let columns: Vec<String> = rows_result
        .column_specs()
        .iter()
        .map(|spec| spec.name().to_string())
        .collect();

    let typed_rows = rows_result.rows::<ColumnIterator>().map_err(|e| RpcError {
        code: -32603,
        message: format!("Failed to read rows: {e}"),
    })?;

    let mut rows = Vec::new();
    for row in typed_rows {
        let column_iter = row.map_err(|e| RpcError {
            code: -32603,
            message: format!("Failed to read row: {e}"),
        })?;

        let mut values = Vec::new();
        for raw_column in column_iter {
            let raw_column = raw_column.map_err(|e| RpcError {
                code: -32603,
                message: format!("Failed to read column: {e}"),
            })?;
            let value = Option::<CqlValue>::deserialize(raw_column.spec.typ(), raw_column.slice)
                .map_err(|e| RpcError {
                    code: -32603,
                    message: format!(
                        "Failed to deserialize column {}: {e}",
                        raw_column.spec.name()
                    ),
                })?;
            values.push(
                value
                    .map(cql_value_to_json)
                    .unwrap_or(serde_json::Value::Null),
            );
        }
        rows.push(values);
    }

    Ok((columns, rows))
}

// Covers the common scalar and collection types explicitly. Anything not
// listed (Decimal, Duration, Date/Time/Timestamp, Varint, Counter, Tuple,
// user-defined types, and whatever CqlValue - itself non-exhaustive -
// grows in the future) falls back to a debug-formatted string rather than
// silently dropping data. A fine thing to sharpen once real columns of
// those types come up.
fn cql_value_to_json(value: CqlValue) -> serde_json::Value {
    match value {
        CqlValue::Text(s) | CqlValue::Ascii(s) => serde_json::Value::String(s),
        CqlValue::Boolean(b) => serde_json::Value::Bool(b),
        CqlValue::Int(i) => serde_json::json!(i),
        CqlValue::BigInt(i) => serde_json::json!(i),
        CqlValue::SmallInt(i) => serde_json::json!(i),
        CqlValue::TinyInt(i) => serde_json::json!(i),
        CqlValue::Float(f) => serde_json::json!(f),
        CqlValue::Double(f) => serde_json::json!(f),
        CqlValue::Uuid(u) => serde_json::Value::String(u.to_string()),
        CqlValue::Timeuuid(u) => serde_json::Value::String(u.to_string()),
        CqlValue::Inet(ip) => serde_json::Value::String(ip.to_string()),
        CqlValue::Blob(bytes) => serde_json::Value::String(hex_encode(&bytes)),
        CqlValue::List(items) | CqlValue::Set(items) | CqlValue::Vector(items) => {
            serde_json::Value::Array(items.into_iter().map(cql_value_to_json).collect())
        }
        CqlValue::Map(pairs) => map_to_json(pairs),
        CqlValue::Empty => serde_json::Value::Null,
        other => serde_json::Value::String(format!("{other:?}")),
    }
}

fn map_to_json(pairs: Vec<(CqlValue, CqlValue)>) -> serde_json::Value {
    let all_string_keys = pairs
        .iter()
        .all(|(k, _)| matches!(k, CqlValue::Text(_) | CqlValue::Ascii(_)));

    if !all_string_keys {
        // JSON objects require string keys; a CQL map with, say, int keys
        // has no lossless JSON shape without picking a convention, so this
        // stays a readable fallback rather than silently coercing keys.
        return serde_json::Value::String(format!("{pairs:?}"));
    }

    let mut obj = serde_json::Map::new();
    for (key, value) in pairs {
        let key = match key {
            CqlValue::Text(s) | CqlValue::Ascii(s) => s,
            _ => unreachable!("checked above"),
        };
        obj.insert(key, cql_value_to_json(value));
    }
    serde_json::Value::Object(obj)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hex_decode(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) {
        return Err("hex string must have an even number of digits".to_string());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

// The reverse of cql_value_to_json - and necessarily less forgiving than
// it. Reading can always fall back to "stringify whatever this is" for a
// type we don't specially handle; writing can't do that, because producing
// the *wrong* CqlValue variant would silently corrupt the write rather than
// just look a bit ugly in a response. So unsupported column types are a
// hard error here (Decimal, Duration, Date/Time/Timestamp, Varint, Counter,
// Timeuuid, collections, tuples, UDTs) - a documented gap in the opposite
// direction from cql_value_to_json's, worth sharpening once real columns of
// those types come up. `Ok(None)` specifically means "write CQL NULL for
// this column" (a JSON `null`, or an explicitly nullable field) - see
// insert_record/update_record for how HashMap<String, Option<CqlValue>>
// lets NULL and a real value share one binding.
fn json_to_cql_value(
    value: &serde_json::Value,
    typ: &ColumnType,
) -> Result<Option<CqlValue>, RpcError> {
    if value.is_null() {
        return Ok(None);
    }

    let native = match typ {
        ColumnType::Native(native) => native,
        other => {
            return Err(RpcError {
                code: -32602,
                message: format!("Writing column type {other:?} isn't supported yet"),
            })
        }
    };

    let cql_value = match native {
        NativeType::Text | NativeType::Ascii => CqlValue::Text(
            value
                .as_str()
                .ok_or_else(|| type_mismatch("a string", value))?
                .to_string(),
        ),
        NativeType::Boolean => CqlValue::Boolean(
            value
                .as_bool()
                .ok_or_else(|| type_mismatch("a boolean", value))?,
        ),
        NativeType::Int => CqlValue::Int(
            value
                .as_i64()
                .ok_or_else(|| type_mismatch("a number", value))? as i32,
        ),
        NativeType::BigInt => CqlValue::BigInt(
            value
                .as_i64()
                .ok_or_else(|| type_mismatch("a number", value))?,
        ),
        NativeType::SmallInt => CqlValue::SmallInt(
            value
                .as_i64()
                .ok_or_else(|| type_mismatch("a number", value))? as i16,
        ),
        NativeType::TinyInt => CqlValue::TinyInt(
            value
                .as_i64()
                .ok_or_else(|| type_mismatch("a number", value))? as i8,
        ),
        NativeType::Float => CqlValue::Float(
            value
                .as_f64()
                .ok_or_else(|| type_mismatch("a number", value))? as f32,
        ),
        NativeType::Double => CqlValue::Double(
            value
                .as_f64()
                .ok_or_else(|| type_mismatch("a number", value))?,
        ),
        NativeType::Blob => {
            let s = value
                .as_str()
                .ok_or_else(|| type_mismatch("a hex string", value))?;
            CqlValue::Blob(hex_decode(s).map_err(|e| RpcError {
                code: -32602,
                message: format!("Invalid hex blob: {e}"),
            })?)
        }
        NativeType::Uuid => {
            let s = value
                .as_str()
                .ok_or_else(|| type_mismatch("a UUID string", value))?;
            let uuid = Uuid::parse_str(s).map_err(|e| RpcError {
                code: -32602,
                message: format!("Invalid UUID \"{s}\": {e}"),
            })?;
            CqlValue::Uuid(uuid)
        }
        other => {
            return Err(RpcError {
                code: -32602,
                message: format!(
                    "Writing column type \"{}\" isn't supported yet",
                    native_type_name(other)
                ),
            })
        }
    };
    Ok(Some(cql_value))
}

fn type_mismatch(expected: &str, value: &serde_json::Value) -> RpcError {
    RpcError {
        code: -32602,
        message: format!("Expected {expected}, got {value}"),
    }
}

fn cql_type_name(typ: &ColumnType) -> String {
    match typ {
        ColumnType::Native(native) => native_type_name(native).to_string(),
        other => format!("{other:?}"),
    }
}

fn native_type_name(native: &NativeType) -> &'static str {
    match native {
        NativeType::Ascii => "ascii",
        NativeType::BigInt => "bigint",
        NativeType::Blob => "blob",
        NativeType::Boolean => "boolean",
        NativeType::Counter => "counter",
        NativeType::Date => "date",
        NativeType::Decimal => "decimal",
        NativeType::Double => "double",
        NativeType::Duration => "duration",
        NativeType::Float => "float",
        NativeType::Int => "int",
        NativeType::Inet => "inet",
        NativeType::SmallInt => "smallint",
        NativeType::Text => "text",
        NativeType::Time => "time",
        NativeType::Timestamp => "timestamp",
        NativeType::Timeuuid => "timeuuid",
        NativeType::TinyInt => "tinyint",
        NativeType::Uuid => "uuid",
        NativeType::Varint => "varint",
        _ => "unknown",
    }
}

fn build_response(
    id: serde_json::Value,
    outcome: Result<serde_json::Value, RpcError>,
) -> RpcResponse {
    match outcome {
        Ok(result) => RpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        },
        Err(error) => RpcResponse {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(error),
        },
    }
}

async fn write_response(stdout: &mut io::Stdout, response: &RpcResponse) -> io::Result<()> {
    let text = serde_json::to_string(response).expect("RpcResponse always serializes");
    stdout.write_all(text.as_bytes()).await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await
}
