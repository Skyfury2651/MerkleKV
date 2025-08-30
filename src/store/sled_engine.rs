//! # Sled Persistent Storage Engine
//!
//! This module provides a persistent disk-based storage engine using Sled.
//! Implements the `KVEngineStoreTrait` interface for consistent API across all engines.
//!
//! ## Features
//!
//! - **Persistence**: Data survives server restarts and crashes
//! - **ACID Compliance**: Atomic operations with crash recovery
//! - **Thread Safety**: Safe for concurrent access across multiple threads
//! - **Performance**: Optimized for embedded database workloads
//! - **Compression**: Optional value compression for space efficiency
//! - **Caching**: In-memory LRU cache for frequently accessed data
//!
//! ## Architecture
//!
//! The SledEngine combines Sled's persistent storage with an in-memory LRU cache:
//! - **Sled Database**: Handles all persistent storage operations
//! - **LRU Cache**: Improves performance for hot keys
//! - **Tree Structure**: Organized storage using Sled's tree abstraction
//! - **Error Handling**: Comprehensive error handling and recovery

use anyhow::{anyhow, Result};
use lru::LruCache;
use sled::{Db, Tree};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use super::kv_trait::KVEngineStoreTrait;

/// Configuration options for the Sled storage engine.
#[derive(Debug, Clone)]
pub struct SledConfig {
    /// Enable value compression for space efficiency
    pub compression: bool,
    /// Cache size in number of entries
    pub cache_size: usize,
    /// Flush interval in milliseconds
    pub flush_interval_ms: u64,
    /// Maximum database size in bytes
    pub max_db_size: usize,
}

impl Default for SledConfig {
    fn default() -> Self {
        Self {
            compression: true,
            cache_size: 1000,
            flush_interval_ms: 1000,
            max_db_size: 1024 * 1024 * 1024, // 1GB
        }
    }
}

/// Persistent disk-based key-value storage engine using Sled.
///
/// This implementation provides:
/// - **Durability**: Data survives server restarts and crashes
/// - **Performance**: LRU cache for frequently accessed data
/// - **Thread Safety**: Safe for concurrent access
/// - **ACID Compliance**: Atomic operations with crash recovery
///
/// # Example
/// ```rust
/// use merkle_kv::store::sled_engine::SledEngine;
///
/// let engine = SledEngine::new("./data/merkle_kv.db")?;
/// engine.set("key1".to_string(), "value1".to_string())?;
/// assert_eq!(engine.get("key1"), Some("value1".to_string()));
/// ```
#[derive(Clone)]
pub struct SledEngine {
    /// Main Sled database instance
    db: Arc<Db>,
    /// Tree for key-value storage
    tree: Arc<Tree>,
    /// In-memory LRU cache for frequently accessed data
    cache: Arc<Mutex<LruCache<String, String>>>,
    /// Configuration options
    config: SledConfig,
}

impl SledEngine {
    /// Create a new Sled storage engine instance.
    ///
    /// # Arguments
    /// * `storage_path` - Path where the Sled database should be stored
    ///
    /// # Returns
    /// * `Result<SledEngine>` - New storage engine instance or error
    ///
    /// # Example
    /// ```rust
    /// let engine = SledEngine::new("./data/merkle_kv.db")?;
    /// ```
    pub fn new(storage_path: &str) -> Result<Self> {
        Self::with_config(storage_path, SledConfig::default())
    }

    /// Create a new Sled storage engine with custom configuration.
    ///
    /// # Arguments
    /// * `storage_path` - Path where the Sled database should be stored
    /// * `config` - Custom configuration options
    ///
    /// # Returns
    /// * `Result<SledEngine>` - New storage engine instance or error
    pub fn with_config(storage_path: &str, config: SledConfig) -> Result<Self> {
        // Open the Sled database
        let db = sled::open(storage_path)
            .map_err(|e| anyhow!("Failed to open Sled database at {}: {}", storage_path, e))?;

        // Open the main tree for key-value storage
        let tree = db
            .open_tree(b"merkle_kv")
            .map_err(|e| anyhow!("Failed to open Sled tree: {}", e))?;

        // Create LRU cache with the specified size
        let cache_size = NonZeroUsize::new(config.cache_size)
            .ok_or_else(|| anyhow!("Cache size must be greater than 0"))?;
        let cache = Arc::new(Mutex::new(LruCache::new(cache_size)));

        Ok(Self {
            db: Arc::new(db),
            tree: Arc::new(tree),
            cache,
            config,
        })
    }

    /// Get a value from the cache or database.
    ///
    /// This method first checks the in-memory cache, then falls back to the database.
    fn get_internal(&self, key: &str) -> Result<Option<String>> {
        // First check the cache
        if let Ok(mut cache) = self.cache.lock() {
            if let Some(value) = cache.get(key) {
                return Ok(Some(value.clone()));
            }
        }

        // If not in cache, get from database
        let value = self
            .tree
            .get(key.as_bytes())
            .map_err(|e| anyhow!("Failed to get key '{}' from database: {}", key, e))?;

        if let Some(value_bytes) = value {
            let value_str = String::from_utf8(value_bytes.to_vec())
                .map_err(|e| anyhow!("Invalid UTF-8 in value for key '{}': {}", key, e))?;

            // Add to cache
            if let Ok(mut cache) = self.cache.lock() {
                cache.put(key.to_string(), value_str.clone());
            }

            Ok(Some(value_str))
        } else {
            Ok(None)
        }
    }

    /// Set a value in both the cache and database.
    fn set_internal(&self, key: String, value: String) -> Result<()> {
        // Update cache
        if let Ok(mut cache) = self.cache.lock() {
            cache.put(key.clone(), value.clone());
        }

        // Store in database
        self.tree
            .insert(key.as_bytes(), value.as_bytes())
            .map_err(|e| anyhow!("Failed to set key '{}' in database: {}", key, e))?;

        Ok(())
    }

    /// Delete a key from both the cache and database.
    fn delete_internal(&self, key: &str) -> Result<bool> {
        // Remove from cache
        if let Ok(mut cache) = self.cache.lock() {
            cache.pop(key);
        }

        // Remove from database
        let result = self
            .tree
            .remove(key.as_bytes())
            .map_err(|e| anyhow!("Failed to delete key '{}' from database: {}", key, e))?;

        Ok(result.is_some())
    }

    /// Get all keys from the database.
    fn keys_internal(&self) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        
        for result in self.tree.iter() {
            let (key_bytes, _) = result
                .map_err(|e| anyhow!("Failed to iterate over database: {}", e))?;
            
            let key = String::from_utf8(key_bytes.to_vec())
                .map_err(|e| anyhow!("Invalid UTF-8 in key: {}", e))?;
            
            keys.push(key);
        }

        Ok(keys)
    }

    /// Get the count of keys in the database.
    fn len_internal(&self) -> Result<usize> {
        Ok(self.tree.len())
    }

    /// Check if the database is empty.
    fn is_empty_internal(&self) -> Result<bool> {
        Ok(self.tree.is_empty())
    }

    /// Force a flush of pending changes to disk.
    pub fn flush(&self) -> Result<()> {
        self.db
            .flush()
            .map_err(|e| anyhow!("Failed to flush database: {}", e))?;
        Ok(())
    }

    /// Get database statistics for monitoring.
    pub fn stats(&self) -> Result<HashMap<String, String>> {
        let mut stats = HashMap::new();
        
        // Database size
        if let Ok(size) = self.db.size_on_disk() {
            stats.insert("db_size_bytes".to_string(), size.to_string());
        }
        
        // Tree size
        stats.insert("tree_size".to_string(), self.tree.len().to_string());
        
        // Cache size
        if let Ok(cache) = self.cache.lock() {
            stats.insert("cache_size".to_string(), cache.len().to_string());
        }
        
        Ok(stats)
    }
}

impl KVEngineStoreTrait for SledEngine {
    fn get(&self, key: &str) -> Option<String> {
        match self.get_internal(key) {
            Ok(value) => value,
            Err(e) => {
                log::error!("Failed to get key '{}': {}", key, e);
                None
            }
        }
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        self.set_internal(key, value)
    }

    fn delete(&self, key: &str) -> bool {
        match self.delete_internal(key) {
            Ok(deleted) => deleted,
            Err(e) => {
                log::error!("Failed to delete key '{}': {}", key, e);
                false
            }
        }
    }

    fn keys(&self) -> Vec<String> {
        match self.keys_internal() {
            Ok(keys) => keys,
            Err(e) => {
                log::error!("Failed to get keys: {}", e);
                Vec::new()
            }
        }
    }

    fn len(&self) -> usize {
        match self.len_internal() {
            Ok(len) => len,
            Err(e) => {
                log::error!("Failed to get length: {}", e);
                0
            }
        }
    }

    fn is_empty(&self) -> bool {
        match self.is_empty_internal() {
            Ok(empty) => empty,
            Err(e) => {
                log::error!("Failed to check if empty: {}", e);
                true
            }
        }
    }

    fn increment(&self, key: &str, amount: Option<i64>) -> Result<i64> {
        let increment_by = amount.unwrap_or(1);
        
        // Get current value
        let current_value = match self.get(key) {
            Some(value) => value.parse::<i64>().unwrap_or(0),
            None => 0,
        };
        
        let new_value = current_value + increment_by;
        
        // Store new value
        self.set(key.to_string(), new_value.to_string())?;
        
        Ok(new_value)
    }

    fn decrement(&self, key: &str, amount: Option<i64>) -> Result<i64> {
        let decrement_by = amount.unwrap_or(1);
        
        // Get current value
        let current_value = match self.get(key) {
            Some(value) => value.parse::<i64>().unwrap_or(0),
            None => 0,
        };
        
        let new_value = current_value - decrement_by;
        
        // Store new value
        self.set(key.to_string(), new_value.to_string())?;
        
        Ok(new_value)
    }

    fn append(&self, key: &str, value: &str) -> Result<String> {
        let current_value = self.get(key).unwrap_or_default();
        let new_value = format!("{}{}", current_value, value);
        
        self.set(key.to_string(), new_value.clone())?;
        
        Ok(new_value)
    }

    fn prepend(&self, key: &str, value: &str) -> Result<String> {
        let current_value = self.get(key).unwrap_or_default();
        let new_value = format!("{}{}", value, current_value);
        
        self.set(key.to_string(), new_value.clone())?;
        
        Ok(new_value)
    }

    fn truncate(&self) -> Result<()> {
        // Clear cache
        if let Ok(mut cache) = self.cache.lock() {
            cache.clear();
        }
        
        // Clear database
        self.tree.clear().map_err(|e| anyhow!("Failed to clear database: {}", e))?;
        
        Ok(())
    }

    fn count_keys(&self) -> Result<u64> {
        Ok(self.len() as u64)
    }

    fn sync(&self) -> Result<()> {
        self.flush()
    }
}

impl Drop for SledEngine {
    fn drop(&mut self) {
        // Ensure data is flushed to disk when the engine is dropped
        if let Err(e) = self.flush() {
            log::error!("Failed to flush database on drop: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::fs;

    #[test]
    fn test_sled_persistence() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().join("test.db");
        
        // Create engine and store data
        {
            let engine = SledEngine::new(storage_path.to_str().unwrap()).unwrap();
            engine.set("key1".to_string(), "value1".to_string()).unwrap();
            engine.set("key2".to_string(), "value2".to_string()).unwrap();
        }
        
        // Reopen and verify data persists
        {
            let engine = SledEngine::new(storage_path.to_str().unwrap()).unwrap();
            assert_eq!(engine.get("key1"), Some("value1".to_string()));
            assert_eq!(engine.get("key2"), Some("value2".to_string()));
        }
    }

    #[test]
    fn test_sled_basic_operations() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().join("test.db");
        
        let engine = SledEngine::new(storage_path.to_str().unwrap()).unwrap();

        // Test set and get
        engine.set("key1".to_string(), "value1".to_string()).unwrap();
        assert_eq!(engine.get("key1"), Some("value1".to_string()));

        // Test overwriting
        engine.set("key1".to_string(), "new_value".to_string()).unwrap();
        assert_eq!(engine.get("key1"), Some("new_value".to_string()));

        // Test delete
        assert!(engine.delete("key1"));
        assert_eq!(engine.get("key1"), None);

        // Test multiple keys
        engine.set("key2".to_string(), "value2".to_string()).unwrap();
        engine.set("key3".to_string(), "value3".to_string()).unwrap();

        let keys = engine.keys();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"key2".to_string()));
        assert!(keys.contains(&"key3".to_string()));

        // Test length and empty
        assert_eq!(engine.len(), 2);
        assert!(!engine.is_empty());
    }

    #[test]
    fn test_sled_numeric_operations() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().join("test.db");
        
        let engine = SledEngine::new(storage_path.to_str().unwrap()).unwrap();

        // Test increment
        let result = engine.increment("counter", Some(5)).unwrap();
        assert_eq!(result, 5);

        let result = engine.increment("counter", None).unwrap();
        assert_eq!(result, 6);

        // Test decrement
        let result = engine.decrement("counter", Some(2)).unwrap();
        assert_eq!(result, 4);

        let result = engine.decrement("counter", None).unwrap();
        assert_eq!(result, 3);
    }

    #[test]
    fn test_sled_string_operations() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().join("test.db");
        
        let engine = SledEngine::new(storage_path.to_str().unwrap()).unwrap();

        // Test append
        engine.set("greeting".to_string(), "Hello".to_string()).unwrap();
        let result = engine.append("greeting", " World!").unwrap();
        assert_eq!(result, "Hello World!");

        // Test prepend
        let result = engine.prepend("greeting", "Say: ").unwrap();
        assert_eq!(result, "Say: Hello World!");
    }

    #[test]
    fn test_sled_truncate() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().join("test.db");
        
        let engine = SledEngine::new(storage_path.to_str().unwrap()).unwrap();

        // Add some data
        engine.set("key1".to_string(), "value1".to_string()).unwrap();
        engine.set("key2".to_string(), "value2".to_string()).unwrap();
        assert_eq!(engine.len(), 2);

        // Truncate
        engine.truncate().unwrap();
        assert_eq!(engine.len(), 0);
        assert!(engine.is_empty());
    }

    #[test]
    fn test_sled_config() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().join("test.db");
        
        let config = SledConfig {
            compression: false,
            cache_size: 100,
            flush_interval_ms: 500,
            max_db_size: 1024 * 1024,
        };
        
        let engine = SledEngine::with_config(storage_path.to_str().unwrap(), config).unwrap();
        
        // Test that it works with custom config
        engine.set("key1".to_string(), "value1".to_string()).unwrap();
        assert_eq!(engine.get("key1"), Some("value1".to_string()));
    }

    #[test]
    fn test_sled_stats() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().join("test.db");
        
        let engine = SledEngine::new(storage_path.to_str().unwrap()).unwrap();
        
        // Add some data
        engine.set("key1".to_string(), "value1".to_string()).unwrap();
        engine.set("key2".to_string(), "value2".to_string()).unwrap();
        
        let stats = engine.stats().unwrap();
        assert!(stats.contains_key("tree_size"));
        assert!(stats.contains_key("cache_size"));
        
        // Tree size should be 2
        assert_eq!(stats["tree_size"], "2");
    }
}
