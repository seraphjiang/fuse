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
use wasmtime::*;

use fuse_core::connector::*;
use fuse_core::error::ConnectorError;

/// Maximum fuel per WASM function call (prevents infinite loops).
const WASM_FUEL_LIMIT: u64 = 10_000_000;
/// Maximum WASM linear memory in bytes (64 MiB).
const _WASM_MEMORY_LIMIT: usize = 64 * 1024 * 1024;
/// Maximum input size for WASM function calls (1 MiB).
const WASM_MAX_INPUT_SIZE: usize = 1024 * 1024;

/// A loaded WASM plugin module.
pub struct WasmPlugin {
    id: String,
    connector_type_name: String,
    module_path: PathBuf,
    engine: Engine,
    module: Module,
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
    /// Load a WASM plugin from a `.wasm` file.
    pub fn load(id: &str, path: &Path) -> Result<Self, ConnectorError> {
        let mut config = wasmtime::Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config)
            .map_err(|e| ConnectorError::Connection(format!("wasm engine: {e}")))?;
        let module = Module::from_file(&engine, path)
            .map_err(|e| ConnectorError::Connection(format!("wasm load '{}': {e}", path.display())))?;

        // Probe for connector_type export
        let connector_type_name = Self::probe_connector_type(&engine, &module)?;

        info!("Loaded WASM plugin '{}' (type: {}) from {}", id, connector_type_name, path.display());

        Ok(Self {
            id: id.to_string(),
            connector_type_name,
            module_path: path.to_path_buf(),
            engine,
            module,
        })
    }

    fn probe_connector_type(engine: &Engine, module: &Module) -> Result<String, ConnectorError> {
        let mut store = Store::new(engine, ());
        store.set_fuel(WASM_FUEL_LIMIT).ok();
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
        store.set_fuel(WASM_FUEL_LIMIT).ok();
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
                            match WasmPlugin::load(&manifest.id, &wasm_path) {
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

        // Case 2: bare .wasm file (original behavior)
        if path.extension().is_some_and(|e| e == "wasm") {
            let id = path.file_stem().unwrap_or_default().to_string_lossy().to_string();
            match WasmPlugin::load(&id, &path) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_nonexistent_file() {
        let result = WasmPlugin::load("test", Path::new("/nonexistent.wasm"));
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
}
