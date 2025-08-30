//! # KV Storage Engine Trait
//!
//! This module defines the common interface for all key-value storage engines
//! in MerkleKV. This allows for easy swapping between different storage implementations
//! without changing the rest of the codebase.
//!
//! ## Implementations
//!
//! - `RwLockEngine`: Thread-safe in-memory storage using RwLock<HashMap>
//! - `KvEngine`: Non-thread-safe in-memory storage using Arc<HashMap>
//! - Future: Persistent storage engines (RocksDB, Sled, etc.)

use anyhow::Result;

/// Common interface for all key-value storage engines.
///
/// This trait defines the core operations that any storage engine must implement.
/// All engines should be safe to share across multiple threads (Send + Sync).
/// 
/// The trait includes basic operations (get, set, delete), numeric operations
/// (increment, decrement), string operations (append, prepend), and bulk operations
/// (truncate, count_keys).
pub trait KVEngineStoreTrait: Send + Sync {
    /// Retrieve a value by its key.
    ///
    /// # Arguments
    /// * `key` - The key to look up
    ///
    /// # Returns
    /// * `Option<String>` - The value if found, None otherwise
    fn get(&self, key: &str) -> Option<String>;

    /// Store a key-value pair.
    ///
    /// # Arguments
    /// * `key` - The key to store
    /// * `value` - The value to associate with the key
    ///
    /// # Returns
    /// * `Result<()>` - Success or error
    fn set(&self, key: String, value: String) -> Result<()>;

    /// Delete a key-value pair.
    ///
    /// # Arguments
    /// * `key` - The key to delete
    ///
    /// # Returns
    /// * `bool` - True if the key existed and was deleted, false otherwise
    fn delete(&self, key: &str) -> bool;

    /// Get all keys currently stored in the engine.
    ///
    /// # Returns
    /// * `Vec<String>` - Vector of all keys in the store
    fn keys(&self) -> Vec<String>;

    /// Get the number of key-value pairs in the store.
    ///
    /// # Returns
    /// * `usize` - Number of key-value pairs
    fn len(&self) -> usize;

    /// Check if the store is empty.
    ///
    /// # Returns
    /// * `bool` - True if the store is empty, false otherwise
    fn is_empty(&self) -> bool;
    
    /// Increment a numeric value.
    ///
    /// # Arguments
    /// * `key` - The key to increment
    /// * `amount` - The amount to increment by (default: 1)
    ///
    /// # Returns
    /// * `Result<i64>` - The new value after incrementing, or error if not a valid number
    fn increment(&self, key: &str, amount: Option<i64>) -> Result<i64>;
    
    /// Decrement a numeric value.
    ///
    /// # Arguments
    /// * `key` - The key to decrement
    /// * `amount` - The amount to decrement by (default: 1)
    ///
    /// # Returns
    /// * `Result<i64>` - The new value after decrementing, or error if not a valid number
    fn decrement(&self, key: &str, amount: Option<i64>) -> Result<i64>;
    
    /// Append a value to an existing string.
    ///
    /// If the key doesn't exist, it will be created with the value.
    ///
    /// # Arguments
    /// * `key` - The key to append to
    /// * `value` - The value to append
    ///
    /// # Returns
    /// * `Result<String>` - The new value after appending
    fn append(&self, key: &str, value: &str) -> Result<String>;
    
    /// Prepend a value to an existing string.
    ///
    /// If the key doesn't exist, it will be created with the value.
    ///
    /// # Arguments
    /// * `key` - The key to prepend to
    /// * `value` - The value to prepend
    ///
    /// # Returns
    /// * `Result<String>` - The new value after prepending
    fn prepend(&self, key: &str, value: &str) -> Result<String>;
    
    /// Clear all keys/values in the store.
    ///
    /// # Returns
    /// * `Result<()>` - Success or error
    fn truncate(&self) -> Result<()>;
    
    /// Get the number of key-value pairs in the store.
    ///
    /// # Returns
    /// * `Result<u64>` - Number of key-value pairs or error
    fn count_keys(&self) -> Result<u64>;
    
    /// Force synchronization of pending changes to persistent storage.
    /// For in-memory engines, this is a no-op.
    ///
    /// # Returns
    /// * `Result<()>` - Success or error
    fn sync(&self) -> Result<()>;
}
