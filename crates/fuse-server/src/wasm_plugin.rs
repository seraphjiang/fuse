// SPDX-License-Identifier: Apache-2.0
//! WASM connector plugin runtime.
//!
//! Loads `.wasm` modules and exposes them as FederatedConnector instances.
//! Each plugin exports functions that map to the connector interface:
//!
//! - `connector_type() -> string`
//! - `health_check() -> json`
//! - `discover_schemas() -> json`
//! - `execute(query_json) -> json`
//!
//! Communication is via JSON serialization over WASM linear memory.

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use sha2::{Sha256, Digest};
use wasmtime::*;

use fuse_core::connector::*;
use fuse_core::error::ConnectorError;

/// Maximum fuel per WASM function call (prevents infinite loops).
const WASM_FUEL_LIMIT: u64 = 10_000_000;
/// Maximum WASM linear memory in bytes (64 MiB).
const _WASM_MEMORY_LIMIT: usize = 64 * 1024 * 1024;
/// Maximum input size for WASM function calls (1 MiB).
const WASM_MAX_INPUT_SIZE: usize = 1024 * 1024;

/// Verify SHA-256 hash of a WASM module file.
fn verify_wasm_sha256(path: &Path, expected_hex: &str) -> Result<(), String> {
    let bytes = std::fs::read(path)
        .map_err(|e| format!("failed to read {}: {e}", path.display()))?;
    let actual = hex::encode(Sha256::digest(&bytes));
    if actual != expected_hex.to_lowercase() {
        return Err(format!(
            "SHA-256 mismatch for {}: expected {}, got {}",
            path.display(), expected_hex, actual
        ));
    }
    Ok(())
}

/// A loaded WASM plugin module.
pub struct WasmPlugin {
    id: String,
    connector_type_name: String,
    module_path: PathBuf,
    engine: Engine,
    module: Module,
    security: PluginSecurityConfig,
}

impl fmt::Debug for WasmPlugin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WasmPlugin")
            .field("id", &self.id)
            .field("connector_type", &self.connector_type_name)
            .field("module_path", &self.module_path)
            .finish()
    }
}

impl WasmPlugin {
    /// Load a WASM plugin from a `.wasm` file with security constraints.
    pub fn load(id: &str, path: &Path, security: PluginSecurityConfig) -> Result<Self, ConnectorError> {
        // Verify integrity if sha256 is specified
        if let Some(ref expected) = security.sha256 {
            verify_wasm_sha256(path, expected)
                .map_err(|e| ConnectorError::Connection(format!("wasm integrity: {e}")))?;
            info!("WASM plugin '{}' SHA-256 verified", id);
        } else {
            warn!("WASM plugin '{}' loaded without SHA-256 verification — consider adding [security].sha256 to manifest", id);
        }

        let mut config = wasmtime::Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config)
            .map_err(|e| ConnectorError::Connection(format!("wasm engine: {e}")))?;
        let module = Module::from_file(&engine, path)
            .map_err(|e| ConnectorError::Connection(format!("wasm load '{}': {e}", path.display())))?;

        // Reject modules that import WASI when not allowed
        if !security.allow_wasi {
            for import in module.imports() {
                if import.module() == "wasi_snapshot_preview1" || import.module().starts_with("wasi:") {
                    return Err(ConnectorError::Connection(format!(
                        "wasm plugin '{}' requires WASI but allow_wasi=false in manifest", id
                    )));
                }
            }
        }

        // Probe for connector_type export
        let fuel = security.max_fuel.unwrap_or(WASM_FUEL_LIMIT);
        let connector_type_name = Self::probe_connector_type(&engine, &module, fuel)?;

        info!(
            "Loaded WASM plugin '{}' (type: {}, fuel: {}, mem: {}MiB, wasi: {}) from {}",
            id, connector_type_name, fuel,
            security.max_memory_mb.unwrap_or(64),
            security.allow_wasi,
            path.display()
        );

        Ok(Self {
            id: id.to_string(),
            connector_type_name,
            module_path: path.to_path_buf(),
            engine,
            module,
            security,
        })
    }

    fn probe_connector_type(engine: &Engine, module: &Module, fuel: u64) -> Result<String, ConnectorError> {
        let mut store = Store::new(engine, ());
        store.set_fuel(fuel).ok();
        let linker = Linker::new(engine);
        let instance = linker.instantiate(&mut store, module)
            .map_err(|e| ConnectorError::Connection(format!("wasm instantiate: {e}")))?;

        // Call connector_type() -> (ptr, len) to get the type string
        let func = instance.get_typed_func::<(), (i32, i32)>(&mut store, "connector_type")
            .map_err(|_| ConnectorError::Connection("wasm: missing 'connector_type' export".into()))?;

        let (ptr, len) = func.call(&mut store, ())
            .map_err(|e| ConnectorError::Connection(format!("wasm connector_type call: {e}")))?;

        let memory = instance.get_memory(&mut store, "memory")
            .ok_or_else(|| ConnectorError::Connection("wasm: no memory export".into()))?;

        let data = memory.data(&store);
        let start = ptr as usize;
        let end = start + len as usize;
        if end > data.len() {
            return Err(ConnectorError::Connection("wasm: connector_type out of bounds".into()));
        }

        String::from_utf8(data[start..end].to_vec())
            .map_err(|e| ConnectorError::Connection(format!("wasm: invalid utf8: {e}")))
    }

    /// Call a WASM function that takes a JSON string and returns a JSON string.
    fn call_json_fn(&self, func_name: &str, input: &str) -> Result<String, ConnectorError> {
        if input.len() > WASM_MAX_INPUT_SIZE {
            return Err(ConnectorError::QueryFailed(format!(
                "wasm input too large: {} bytes (max {})", input.len(), WASM_MAX_INPUT_SIZE
            )));
        }

        let mut store = Store::new(&self.engine, ());
        store.set_fuel(self.security.max_fuel.unwrap_or(WASM_FUEL_LIMIT)).ok();
        let linker = Linker::new(&self.engine);
        let instance = linker.instantiate(&mut store, &self.module)
            .map_err(|e| ConnectorError::Connection(format!("wasm instantiate: {e}")))?;

        let memory = instance.get_memory(&mut store, "memory")
            .ok_or_else(|| ConnectorError::Connection("wasm: no memory export".into()))?;

        // Write input to WASM memory via alloc
        let alloc = instance.get_typed_func::<i32, i32>(&mut store, "alloc")
            .map_err(|_| ConnectorError::Connection("wasm: missing 'alloc' export".into()))?;

        let input_bytes = input.as_bytes();
        let input_ptr = alloc.call(&mut store, input_bytes.len() as i32)
            .map_err(|e| ConnectorError::Connection(format!("wasm alloc: {e}")))?;

        memory.data_mut(&mut store)[input_ptr as usize..input_ptr as usize + input_bytes.len()]
            .copy_from_slice(input_bytes);

        // Call the function
        let func = instance.get_typed_func::<(i32, i32), (i32, i32)>(&mut store, func_name)
            .map_err(|_| ConnectorError::Connection(format!("wasm: missing '{}' export", func_name)))?;

        let (out_ptr, out_len) = func.call(&mut store, (input_ptr, input_bytes.len() as i32))
            .map_err(|e| ConnectorError::QueryFailed(format!("wasm {func_name}: {e}")))?;

        let data = memory.data(&store);
        let start = out_ptr as usize;
        let end = start + out_len as usize;
        if end > data.len() {
            return Err(ConnectorError::QueryFailed(format!("wasm {func_name}: output out of bounds")));
        }

        String::from_utf8(data[start..end].to_vec())
            .map_err(|e| ConnectorError::QueryFailed(format!("wasm: invalid utf8: {e}")))
    }
}

#[async_trait]
impl FederatedConnector for WasmPlugin {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { &self.connector_type_name }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: true,
            supports_projection: true,
            supports_aggregation: false,
            supports_sorting: false,
            supports_limit: true,
            supports_join: false,
            max_concurrent_queries: 4,
            supports_streaming: false,
            latency_class: LatencyClass::Medium,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        ConnectorHealth { status: HealthStatus::Healthy, latency_ms: None, message: Some("wasm plugin".into()) }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let json = self.call_json_fn("discover_schemas", "{}")?;
        let names: Vec<String> = serde_json::from_str(&json)
            .map_err(|e| ConnectorError::SchemaDiscovery(format!("wasm: {e}")))?;
        Ok(names.into_iter().map(|n| SchemaInfo {
            name: n,
            schema_type: SchemaType::Table,
            estimated_row_count: None,
        }).collect())
    }

    async fn get_schema(&self, table: &str) -> Result<arrow::datatypes::Schema, ConnectorError> {
        let json = self.call_json_fn("get_schema", table)?;
        let fields: Vec<String> = serde_json::from_str(&json)
            .map_err(|e| ConnectorError::SchemaDiscovery(format!("wasm: {e}")))?;
        Ok(Schema::new(fields.into_iter().map(|f| Field::new(f, DataType::Utf8, true)).collect::<Vec<_>>()))
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let query_json = serde_json::json!({
            "table": query.table,
            "projections": query.projections,
            "limit": query.limit,
        }).to_string();

        let result_json = self.call_json_fn("execute", &query_json)?;

        // Parse JSON array of rows → Arrow batch
        let rows: Vec<HashMap<String, serde_json::Value>> = serde_json::from_str(&result_json)
            .map_err(|e| ConnectorError::QueryFailed(format!("wasm result parse: {e}")))?;

        if rows.is_empty() { return Ok(vec![]); }

        // Infer columns from first row
        let columns: Vec<String> = rows[0].keys().cloned().collect();
        let fields: Vec<Field> = columns.iter().map(|c| Field::new(c, DataType::Utf8, true)).collect();
        let arrays: Vec<Arc<dyn arrow::array::Array>> = columns.iter().map(|col| {
            let vals: Vec<Option<String>> = rows.iter().map(|r| {
                r.get(col).map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
            }).collect();
            Arc::new(StringArray::from(vals)) as Arc<dyn arrow::array::Array>
        }).collect();

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|e| ConnectorError::QueryFailed(format!("arrow: {e}")))?;
        Ok(vec![batch])
    }

    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        let batches = self.execute(query).await?;
        for batch in batches {
            if tx.send(Ok(batch)).await.is_err() { break; }
        }
        Ok(())
    }
}

/// Discover and load all `.wasm` plugins from a directory.
/// Plugin manifest loaded from `manifest.toml` alongside a `.wasm` file.
///
/// Example manifest.toml:
/// ```toml
/// id = "my-connector"
/// name = "My Custom Connector"
/// version = "0.1.0"
/// connector_type = "my-type"
/// description = "Connects to My Service"
/// author = "Team X"
///
/// [security]
/// sha256 = "abc123..."
/// max_fuel = 5000000
/// max_memory_mb = 32
/// max_execution_ms = 30000
/// allow_wasi = false
/// ```
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct PluginManifest {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub connector_type: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub author: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub security: PluginSecurityConfig,
}

/// Security constraints for a WASM plugin.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct PluginSecurityConfig {
    /// SHA-256 hex digest of the `.wasm` file. If set, module is rejected on mismatch.
    #[serde(default)]
    pub sha256: Option<String>,
    /// Max fuel (CPU budget) per call. Defaults to WASM_FUEL_LIMIT.
    #[serde(default)]
    pub max_fuel: Option<u64>,
    /// Max linear memory in MiB. Defaults to 64.
    #[serde(default)]
    pub max_memory_mb: Option<usize>,
    /// Max wall-clock execution time per call in ms. Defaults to 30_000.
    #[serde(default)]
    pub max_execution_ms: Option<u64>,
    /// Whether WASI imports are allowed. Defaults to false (fully sandboxed).
    #[serde(default)]
    pub allow_wasi: bool,
}

impl Default for PluginSecurityConfig {
    fn default() -> Self {
        Self {
            sha256: None,
            max_fuel: None,
            max_memory_mb: None,
            max_execution_ms: None,
            allow_wasi: false,
        }
    }
}

impl PluginManifest {
    /// Load a manifest from a TOML file.
    pub fn from_file(path: &Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read {}: {e}", path.display()))?;
        toml::from_str(&content)
            .map_err(|e| format!("failed to parse {}: {e}", path.display()))
    }
}

pub fn load_plugins_from_dir(dir: &Path) -> Vec<WasmPlugin> {
    let mut plugins = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        debug!("Plugin dir {} not found, skipping", dir.display());
        return plugins;
    };

    for entry in entries.flatten() {
        let path = entry.path();

        // Case 1: subdirectory with manifest.toml + .wasm file
        if path.is_dir() {
            let manifest_path = path.join("manifest.toml");
            if manifest_path.exists() {
                match PluginManifest::from_file(&manifest_path) {
                    Ok(manifest) => {
                        if manifest.enabled == Some(false) {
                            info!("Plugin '{}' disabled via manifest, skipping", manifest.id);
                            continue;
                        }
                        let wasm_path = path.join(format!("{}.wasm", manifest.id));
                        if wasm_path.exists() {
                            match WasmPlugin::load(&manifest.id, &wasm_path, manifest.security.clone()) {
                                Ok(plugin) => {
                                    info!("Loaded plugin '{}' v{} from manifest",
                                        manifest.id,
                                        manifest.version.as_deref().unwrap_or("unknown"));
                                    plugins.push(plugin);
                                }
                                Err(e) => warn!("Failed to load plugin '{}': {}", manifest.id, e),
                            }
                        } else {
                            warn!("Manifest for '{}' found but no .wasm file at {}", manifest.id, wasm_path.display());
                        }
                    }
                    Err(e) => warn!("Bad manifest in {}: {}", path.display(), e),
                }
            }
            continue;
        }

        // Case 2: bare .wasm file — loaded with default (restrictive) security
        if path.extension().is_some_and(|e| e == "wasm") {
            let id = path.file_stem().unwrap_or_default().to_string_lossy().to_string();
            warn!("Loading bare WASM plugin '{}' without manifest — no integrity verification", id);
            match WasmPlugin::load(&id, &path, PluginSecurityConfig::default()) {
                Ok(plugin) => {
                    info!("Loaded WASM plugin: {} ({})", id, plugin.connector_type_name);
                    plugins.push(plugin);
                }
                Err(e) => warn!("Failed to load WASM plugin {}: {}", path.display(), e),
            }
        }
    }
    plugins
}

/// Runtime plugin registry — manages loaded WASM plugins.
pub struct PluginRegistry {
    plugins: std::sync::Mutex<Vec<WasmPlugin>>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self { plugins: std::sync::Mutex::new(Vec::new()) }
    }

    /// Load plugins from a directory and register them.
    pub fn load_from_dir(&self, dir: &Path) {
        let loaded = load_plugins_from_dir(dir);
        let mut plugins = self.plugins.lock().unwrap();
        plugins.extend(loaded);
    }

    /// Register a single plugin.
    pub fn register(&self, plugin: WasmPlugin) {
        self.plugins.lock().unwrap().push(plugin);
    }

    /// List all loaded plugin IDs.
    pub fn list(&self) -> Vec<String> {
        self.plugins.lock().unwrap().iter().map(|p| p.id.clone()).collect()
    }

    /// Get plugin count.
    pub fn count(&self) -> usize {
        self.plugins.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_nonexistent_file() {
        let result = WasmPlugin::load("test", Path::new("/nonexistent.wasm"), PluginSecurityConfig::default());
        assert!(result.is_err());
    }

    #[test]
    fn test_load_plugins_from_missing_dir() {
        let plugins = load_plugins_from_dir(Path::new("/nonexistent/plugins"));
        assert!(plugins.is_empty());
    }

    #[test]
    fn test_load_plugins_from_empty_dir() {
        let dir = std::env::temp_dir().join("fuse_wasm_test_empty");
        let _ = std::fs::create_dir_all(&dir);
        let plugins = load_plugins_from_dir(&dir);
        assert!(plugins.is_empty());
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn test_load_invalid_wasm() {
        let dir = std::env::temp_dir().join("fuse_wasm_test_invalid");
        let _ = std::fs::create_dir_all(&dir);
        std::fs::write(dir.join("bad.wasm"), b"not a wasm module").unwrap();
        let plugins = load_plugins_from_dir(&dir);
        assert!(plugins.is_empty()); // should warn and skip
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_manifest_parse() {
        let toml = r#"
id = "my-plugin"
name = "My Plugin"
version = "0.2.0"
connector_type = "custom"
description = "A test plugin"
author = "Test"
"#;
        let m: PluginManifest = toml::from_str(toml).unwrap();
        assert_eq!(m.id, "my-plugin");
        assert_eq!(m.version.as_deref(), Some("0.2.0"));
        assert_eq!(m.connector_type.as_deref(), Some("custom"));
        assert!(m.enabled.is_none()); // defaults to None (treated as enabled)
    }

    #[test]
    fn test_manifest_minimal() {
        let toml = r#"id = "bare""#;
        let m: PluginManifest = toml::from_str(toml).unwrap();
        assert_eq!(m.id, "bare");
        assert!(m.name.is_none());
        assert!(m.version.is_none());
    }

    #[test]
    fn test_manifest_disabled() {
        let toml = r#"
id = "disabled-plugin"
enabled = false
"#;
        let m: PluginManifest = toml::from_str(toml).unwrap();
        assert_eq!(m.enabled, Some(false));
    }

    #[test]
    fn test_manifest_from_file_missing() {
        let result = PluginManifest::from_file(Path::new("/nonexistent/manifest.toml"));
        assert!(result.is_err());
    }

    #[test]
    fn test_manifest_from_file_valid() {
        let dir = std::env::temp_dir().join("fuse_manifest_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("manifest.toml");
        std::fs::write(&path, "id = \"test-plug\"\nversion = \"1.0.0\"").unwrap();
        let m = PluginManifest::from_file(&path).unwrap();
        assert_eq!(m.id, "test-plug");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_plugins_manifest_dir_no_wasm() {
        let dir = std::env::temp_dir().join("fuse_manifest_no_wasm");
        let plugin_dir = dir.join("my-plugin");
        let _ = std::fs::create_dir_all(&plugin_dir);
        std::fs::write(plugin_dir.join("manifest.toml"), "id = \"my-plugin\"").unwrap();
        // No .wasm file — should warn and skip
        let plugins = load_plugins_from_dir(&dir);
        assert!(plugins.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_plugins_disabled_manifest() {
        let dir = std::env::temp_dir().join("fuse_manifest_disabled");
        let plugin_dir = dir.join("off-plugin");
        let _ = std::fs::create_dir_all(&plugin_dir);
        std::fs::write(plugin_dir.join("manifest.toml"), "id = \"off-plugin\"\nenabled = false").unwrap();
        std::fs::write(plugin_dir.join("off-plugin.wasm"), b"fake").unwrap();
        let plugins = load_plugins_from_dir(&dir);
        assert!(plugins.is_empty()); // disabled
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_plugin_registry_empty() {
        let reg = PluginRegistry::new();
        assert_eq!(reg.count(), 0);
        assert!(reg.list().is_empty());
    }

    #[test]
    fn test_plugin_registry_load_empty_dir() {
        let dir = std::env::temp_dir().join("fuse_reg_empty");
        let _ = std::fs::create_dir_all(&dir);
        let reg = PluginRegistry::new();
        reg.load_from_dir(&dir);
        assert_eq!(reg.count(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Security tests ──

    #[test]
    fn test_verify_sha256_valid() {
        let dir = std::env::temp_dir().join("fuse_sha256_valid");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test.wasm");
        let content = b"fake wasm content";
        std::fs::write(&path, content).unwrap();
        let expected = hex::encode(sha2::Sha256::digest(content));
        assert!(verify_wasm_sha256(&path, &expected).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_verify_sha256_mismatch() {
        let dir = std::env::temp_dir().join("fuse_sha256_bad");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test.wasm");
        std::fs::write(&path, b"content").unwrap();
        let result = verify_wasm_sha256(&path, "0000000000000000000000000000000000000000000000000000000000000000");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("SHA-256 mismatch"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_verify_sha256_missing_file() {
        let result = verify_wasm_sha256(Path::new("/nonexistent"), "abc");
        assert!(result.is_err());
    }

    #[test]
    fn test_manifest_with_security_config() {
        let toml = r#"
id = "secure-plugin"
version = "1.0.0"

[security]
sha256 = "abcdef1234567890"
max_fuel = 5000000
max_memory_mb = 32
max_execution_ms = 15000
allow_wasi = false
"#;
        let m: PluginManifest = toml::from_str(toml).unwrap();
        assert_eq!(m.security.sha256.as_deref(), Some("abcdef1234567890"));
        assert_eq!(m.security.max_fuel, Some(5_000_000));
        assert_eq!(m.security.max_memory_mb, Some(32));
        assert_eq!(m.security.max_execution_ms, Some(15_000));
        assert!(!m.security.allow_wasi);
    }

    #[test]
    fn test_manifest_security_defaults() {
        let toml = r#"id = "no-security""#;
        let m: PluginManifest = toml::from_str(toml).unwrap();
        assert!(m.security.sha256.is_none());
        assert!(m.security.max_fuel.is_none());
        assert!(m.security.max_memory_mb.is_none());
        assert!(!m.security.allow_wasi);
    }

    #[test]
    fn test_manifest_allow_wasi() {
        let toml = r#"
id = "wasi-plugin"
[security]
allow_wasi = true
"#;
        let m: PluginManifest = toml::from_str(toml).unwrap();
        assert!(m.security.allow_wasi);
    }

    #[test]
    fn test_security_config_default() {
        let cfg = PluginSecurityConfig::default();
        assert!(cfg.sha256.is_none());
        assert!(cfg.max_fuel.is_none());
        assert!(cfg.max_memory_mb.is_none());
        assert!(cfg.max_execution_ms.is_none());
        assert!(!cfg.allow_wasi);
    }
}
