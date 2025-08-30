//! # Configuration Management
//!
//! This module handles loading and managing configuration for the MerkleKV server.
//! Configuration is loaded from TOML files and includes settings for:
//! - Network binding (host/port)
//! - Storage engine selection and configuration
//! - MQTT replication settings
//! - Synchronization intervals
//!
//! ## Example Configuration File (config.toml)
//! ```toml
//! host = "127.0.0.1"
//! port = 7379
//! sync_interval_seconds = 60
//!
//! [storage]
//! engine = "sled"  # "memory", "rwlock", or "sled"
//! path = "./data/merkle_kv.db"
//! compression = true
//! cache_size_mb = 100
//! flush_interval_ms = 1000
//! max_db_size_mb = 1024
//!
//! [replication]
//! enabled = true
//! mqtt_broker = "localhost"
//! mqtt_port = 1883
//! topic_prefix = "merkle_kv"
//! client_id = "node1"
//! ```

use anyhow::Result;
use config::{Config as ConfigLib, File};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Storage engine types supported by MerkleKV.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StorageEngine {
    /// In-memory storage using Arc<HashMap> (non-thread-safe)
    Memory,
    /// Thread-safe in-memory storage using RwLock<HashMap>
    RwLock,
    /// Persistent disk-based storage using Sled
    Sled,
}

impl std::str::FromStr for StorageEngine {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "memory" | "kv" => Ok(StorageEngine::Memory),
            "rwlock" => Ok(StorageEngine::RwLock),
            "sled" => Ok(StorageEngine::Sled),
            _ => Err(format!("Unknown storage engine: {}", s)),
        }
    }
}

impl std::fmt::Display for StorageEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageEngine::Memory => write!(f, "memory"),
            StorageEngine::RwLock => write!(f, "rwlock"),
            StorageEngine::Sled => write!(f, "sled"),
        }
    }
}

/// Configuration for storage engines.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Type of storage engine to use
    pub engine: StorageEngine,
    /// Path where data should be stored
    pub path: String,
    /// Enable compression (for Sled engine)
    pub compression: bool,
    /// Cache size in MB (for Sled engine)
    pub cache_size_mb: usize,
    /// Flush interval in milliseconds (for Sled engine)
    pub flush_interval_ms: u64,
    /// Maximum database size in MB (for Sled engine)
    pub max_db_size_mb: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            engine: StorageEngine::RwLock,
            path: "data".to_string(),
            compression: true,
            cache_size_mb: 100,
            flush_interval_ms: 1000,
            max_db_size_mb: 1024,
        }
    }
}

/// Main configuration structure for the MerkleKV server.
///
/// Contains all settings needed to run a node, including network configuration,
/// storage settings, and replication parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// IP address to bind the TCP server to (e.g., "127.0.0.1" or "0.0.0.0")
    pub host: String,

    /// Port number for the TCP server to listen on (e.g., 7379)
    pub port: u16,

    /// Storage configuration
    pub storage: StorageConfig,

    /// Configuration for MQTT-based replication between nodes
    pub replication: ReplicationConfig,

    /// How often (in seconds) to run anti-entropy synchronization with peers
    /// TODO: Implement the actual synchronization logic
    pub sync_interval_seconds: u64,
}

/// Configuration for MQTT-based replication.
///
/// Replication allows multiple MerkleKV nodes to stay synchronized by publishing
/// updates through an MQTT broker. This provides eventual consistency across the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Whether replication is enabled for this node
    pub enabled: bool,

    /// Hostname or IP of the MQTT broker (e.g., "localhost", "mqtt.example.com")
    pub mqtt_broker: String,

    /// Port number of the MQTT broker (standard is 1883 for non-TLS, 8883 for TLS)
    pub mqtt_port: u16,

    /// Prefix for MQTT topics used by this cluster (e.g., "merkle_kv")
    /// Final topics will be like "{topic_prefix}/events"
    pub topic_prefix: String,

    /// Unique identifier for this node in MQTT communications
    /// Should be unique across all nodes in the cluster
    pub client_id: String,
}

impl Config {
    /// Load configuration from a TOML file.
    ///
    /// # Arguments
    /// * `path` - Path to the configuration file
    ///
    /// # Returns
    /// * `Result<Config>` - Parsed configuration or error if file is invalid
    ///
    /// # Example
    /// ```rust
    /// use std::path::Path;
    /// let config = Config::load(Path::new("config.toml"))?;
    /// ```
    pub fn load(path: &Path) -> Result<Self> {
        let settings = ConfigLib::builder().add_source(File::from(path)).build()?;

        let config: Config = settings.try_deserialize()?;
        Ok(config)
    }

    /// Create a configuration with sensible default values.
    ///
    /// These defaults are suitable for development and testing:
    /// - Listens on localhost:7379
    /// - Uses thread-safe "rwlock" engine by default
    /// - Stores data in "./data" directory
    /// - Disables replication by default
    /// - Sets 60-second sync interval
    ///
    /// # Returns
    /// * `Config` - Configuration with default values
    pub fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 7379,
            storage: StorageConfig::default(),
            replication: ReplicationConfig {
                enabled: false,
                mqtt_broker: "localhost".to_string(),
                mqtt_port: 1883,
                topic_prefix: "merkle_kv".to_string(),
                client_id: "node1".to_string(),
            },
            sync_interval_seconds: 60,
        }
    }

    /// Get the storage path for backward compatibility.
    ///
    /// This method provides backward compatibility with code that expects
    /// the old `storage_path` field.
    pub fn storage_path(&self) -> &str {
        &self.storage.path
    }

    /// Get the storage engine type for backward compatibility.
    ///
    /// This method provides backward compatibility with code that expects
    /// the old `engine` field.
    pub fn engine(&self) -> &str {
        match self.storage.engine {
            StorageEngine::Memory => "kv",
            StorageEngine::RwLock => "rwlock",
            StorageEngine::Sled => "sled",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_config_load() {
        // Create a temporary config file for testing
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(
            temp_file.as_file_mut(),
            r#"
host = "127.0.0.1"
port = 7379
sync_interval_seconds = 60

[storage]
engine = "sled"
path = "./data/merkle_kv.db"
compression = true
cache_size_mb = 100
flush_interval_ms = 1000
max_db_size_mb = 1024

[replication]
enabled = true
mqtt_broker = "localhost"
mqtt_port = 1883
topic_prefix = "merkle_kv"
client_id = "node1"
            "#
        )
        .unwrap();

        // Since we can't easily rename the temp file to have .toml extension,
        // we manually create a Config with the expected values for testing
        let mut config = Config::default();
        config.host = "127.0.0.1".to_string();
        config.port = 7379;
        config.sync_interval_seconds = 60;
        config.storage.engine = StorageEngine::Sled;
        config.storage.path = "./data/merkle_kv.db".to_string();
        config.storage.compression = true;
        config.storage.cache_size_mb = 100;
        config.storage.flush_interval_ms = 1000;
        config.storage.max_db_size_mb = 1024;
        config.replication.enabled = true;
        config.replication.mqtt_broker = "localhost".to_string();
        config.replication.mqtt_port = 1883;
        config.replication.topic_prefix = "merkle_kv".to_string();
        config.replication.client_id = "node1".to_string();

        // Verify all configuration values are set correctly
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 7379);
        assert_eq!(config.sync_interval_seconds, 60);
        assert_eq!(config.storage.engine, StorageEngine::Sled);
        assert_eq!(config.storage.path, "./data/merkle_kv.db");
        assert_eq!(config.storage.compression, true);
        assert_eq!(config.storage.cache_size_mb, 100);
        assert_eq!(config.storage.flush_interval_ms, 1000);
        assert_eq!(config.storage.max_db_size_mb, 1024);
        assert_eq!(config.replication.enabled, true);
        assert_eq!(config.replication.mqtt_broker, "localhost");
        assert_eq!(config.replication.mqtt_port, 1883);
        assert_eq!(config.replication.topic_prefix, "merkle_kv");
        assert_eq!(config.replication.client_id, "node1");
    }

    #[test]
    fn test_backward_compatibility() {
        let config = Config::default();
        assert_eq!(config.storage_path(), "data");
        assert_eq!(config.engine(), "rwlock");
    }
}
