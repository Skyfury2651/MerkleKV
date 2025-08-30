//! # Key-Value Storage Engine
//!
//! This module provides the core storage functionality for MerkleKV.
//! Currently implements an in-memory storage engine using HashMap.
//!
//! ## Current Implementation
//!
//! The current implementation is a simple in-memory store that:
//! - Uses `Arc<HashMap<String, String>>` for thread-safe access
//! - Creates new HashMap instances on every write (copy-on-write pattern)
//! - Provides basic get/set/delete operations
//! - Supports numeric operations (increment/decrement)
//! - Supports string operations (append/prepend)
//! - Returns all keys for iteration
//!
//! ## Future Implementation Plans
//!
//! This is a placeholder implementation. A production version should:
//! - Use a persistent storage engine (e.g., RocksDB, Sled)
//! - Support transactions and atomic operations
//! - Implement Write-Ahead Logging (WAL)
//! - Add compression and efficient serialization
//! - Support range queries and iteration
//! - Implement proper error handling for I/O operations

use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;

use super::kv_trait::KVEngineStoreTrait;

/// In-memory key-value storage engine.
///
/// This is a simplified storage implementation that keeps all data in memory.
/// The `Arc<HashMap>` allows for efficient cloning of the engine while sharing
/// the underlying data until a write operation occurs.
///
/// **Note**: This implementation is not persistent! All data is lost when
/// the process terminates.
#[derive(Clone)]
pub struct KvEngine {
    /// Shared reference to the key-value data
    /// Using Arc allows multiple readers while writes create new instances
    data: Arc<HashMap<String, String>>,
    // TODO: Add persistent storage implementation
    // In a real implementation, this would use a persistent storage engine like Sled:
    // storage_path: PathBuf,
    // sled_db: sled::Db,
}

impl KvEngine {
    /// Create a new storage engine instance.
    ///
    /// # Arguments
    /// * `_storage_path` - Path where data should be stored (currently unused)
    ///
    /// # Returns
    /// * `Result<KvEngine>` - New storage engine instance or error
    ///
    /// # Current Behavior
    /// Creates an empty in-memory HashMap. The storage_path is ignored.
    ///
    /// # Future Implementation
    /// Should initialize or open a persistent storage engine at the given path.
    pub fn new(_storage_path: &str) -> Result<Self> {
        // TODO: Initialize persistent storage engine here
        // For example, with Sled:
        // let db = sled::open(storage_path)?;
        // Ok(Self { storage_path: storage_path.into(), sled_db: db })

        Ok(Self {
            data: Arc::new(HashMap::new()),
        })
    }

    /// Retrieve a value by its key.
    ///
    /// # Arguments
    /// * `key` - The key to look up
    ///
    /// # Returns
    /// * `Option<String>` - The value if found, None otherwise
    ///
    /// # Example
    /// ```rust
    /// let engine = KvEngine::new("./data")?;
    /// if let Some(value) = engine.get("user:123") {
    ///     println!("Found user: {}", value);
    /// }
    /// ```
    pub fn get(&self, key: &str) -> Option<String> {
        self.data.get(key).cloned()
    }

    /// Store a key-value pair.
    ///
    /// This operation creates a new HashMap with the updated data due to the
    /// immutable nature of the `Arc<HashMap>` design. This is inefficient but
    /// simple for the current prototype.
    ///
    /// # Arguments
    /// * `key` - The key to store
    /// * `value` - The value to associate with the key
    ///
    /// # Example
    /// ```rust
    /// let mut engine = KvEngine::new("./data")?;
    /// engine.set("user:123".to_string(), "john_doe".to_string());
    /// ```
    ///
    /// # Performance Note
    /// This implementation is O(n) in the number of keys due to HashMap cloning.
    /// A persistent storage engine would be much more efficient.
    pub fn set(&mut self, key: String, value: String) {
        // Create a new HashMap with the updated value
        // TODO: Replace with efficient persistent storage writes
        let mut new_data = (*self.data).clone();
        new_data.insert(key, value);
        self.data = Arc::new(new_data);
    }

    /// Delete a key-value pair.
    ///
    /// Like `set`, this creates a new HashMap without the deleted key.
    ///
    /// # Arguments
    /// * `key` - The key to delete
    ///
    /// # Example
    /// ```rust
    /// let mut engine = KvEngine::new("./data")?;
    /// engine.delete("user:123");
    /// ```
    pub fn delete(&mut self, key: &str) {
        // Create a new HashMap without the deleted key
        // TODO: Replace with efficient persistent storage deletes
        let mut new_data = (*self.data).clone();
        new_data.remove(key);
        self.data = Arc::new(new_data);
    }

    /// Get all keys currently stored in the engine.
    ///
    /// This is primarily used by the Merkle tree to rebuild its state
    /// and for debugging purposes.
    ///
    /// # Returns
    /// * `Vec<String>` - Vector of all keys in the store
    ///
    /// # Performance Note
    /// This operation is O(n) and creates a new vector. In a production
    /// system, this should be replaced with an iterator-based approach.
    pub fn keys(&self) -> Vec<String> {
        self.data.keys().cloned().collect()
    }

    /// Increment a numeric value stored at the given key.
    /// 
    /// If the key doesn't exist, it will be created with the increment amount.
    /// If the key exists but doesn't contain a valid number, an error message is returned.
    /// 
    /// # Arguments
    /// * `key` - The key to increment
    /// * `amount` - The amount to increment by (defaults to 1 if None)
    /// 
    /// # Returns
    /// * `Result<i64, String>` - The new value after incrementing, or an error message
    /// 
    /// # Example
    /// ```rust
    /// let mut engine = KvEngine::new("./data")?;
    /// match engine.increment("counter", Some(5)) {
    ///     Ok(new_value) => println!("New counter value: {}", new_value),
    ///     Err(e) => println!("Error: {}", e),
    /// }
    /// ```
    pub fn increment(&mut self, key: &str, amount: Option<i64>) -> Result<i64, String> {
        let increment_by = amount.unwrap_or(1);
        let mut new_data = (*self.data).clone();
        
        let new_value = match new_data.get(key) {
            Some(value) => {
                // Try to parse the existing value as a number
                match value.parse::<i64>() {
                    Ok(num) => num + increment_by,
                    Err(_) => return Err(format!("Value for key '{}' is not a valid number", key)),
                }
            }
            None => increment_by, // Key doesn't exist, start with the increment amount
        };
        
        // Store the new value
        new_data.insert(key.to_string(), new_value.to_string());
        self.data = Arc::new(new_data);
        
        Ok(new_value)
    }

    /// Decrement a numeric value stored at the given key.
    /// 
    /// If the key doesn't exist, it will be created with the negative of the decrement amount.
    /// If the key exists but doesn't contain a valid number, an error message is returned.
    /// 
    /// # Arguments
    /// * `key` - The key to decrement
    /// * `amount` - The amount to decrement by (defaults to 1 if None)
    /// 
    /// # Returns
    /// * `Result<i64, String>` - The new value after decrementing, or an error message
    /// 
    /// # Example
    /// ```rust
    /// let mut engine = KvEngine::new("./data")?;
    /// match engine.decrement("counter", Some(3)) {
    ///     Ok(new_value) => println!("New counter value: {}", new_value),
    ///     Err(e) => println!("Error: {}", e),
    /// }
    /// ```
    pub fn decrement(&mut self, key: &str, amount: Option<i64>) -> Result<i64, String> {
        let decrement_by = amount.unwrap_or(1);
        // Decrement is just a negative increment
        self.increment(key, Some(-decrement_by))
    }

    /// Append a value to an existing string.
    /// 
    /// If the key doesn't exist, it will be created with the value.
    /// 
    /// # Arguments
    /// * `key` - The key to append to
    /// * `value` - The value to append
    /// 
    /// # Returns
    /// * `String` - The new value after appending
    /// 
    /// # Example
    /// ```rust
    /// let mut engine = KvEngine::new("./data")?;
    /// let new_value = engine.append("greeting", " World!");
    /// println!("New value: {}", new_value); // e.g., "Hello World!"
    /// ```
    pub fn append(&mut self, key: &str, value: &str) -> String {
        let mut new_data = (*self.data).clone();
        
        let new_value = match new_data.get(key) {
            Some(existing) => {
                let mut result = existing.clone();
                result.push_str(value);
                result
            }
            None => value.to_string(), // Key doesn't exist, use the value as is
        };
        
        // Store the new value
        new_data.insert(key.to_string(), new_value.clone());
        self.data = Arc::new(new_data);
        
        new_value
    }

    /// Prepend a value to an existing string.
    /// 
    /// If the key doesn't exist, it will be created with the value.
    /// 
    /// # Arguments
    /// * `key` - The key to prepend to
    /// * `value` - The value to prepend
    /// 
    /// # Returns
    /// * `String` - The new value after prepending
    /// 
    /// # Example
    /// ```rust
    /// let mut engine = KvEngine::new("./data")?;
    /// let new_value = engine.prepend("greeting", "Hello, ");
    /// println!("New value: {}", new_value); // e.g., "Hello, World!"
    /// ```
    pub fn prepend(&mut self, key: &str, value: &str) -> String {
        let mut new_data = (*self.data).clone();
        
        let new_value = match new_data.get(key) {
            Some(existing) => {
                let mut result = value.to_string();
                result.push_str(existing);
                result
            }
            None => value.to_string(), // Key doesn't exist, use the value as is
        };
        
        // Store the new value
        new_data.insert(key.to_string(), new_value.clone());
        self.data = Arc::new(new_data);
        
        new_value
    }

    /// Clear all keys/values in the store.
    /// 
    /// This operation removes all data from the store, effectively resetting it
    /// to an empty state.
    /// 
    /// # Example
    /// ```rust
    /// let mut engine = KvEngine::new("./data")?;
    /// engine.truncate();
    /// assert_eq!(engine.keys().len(), 0);
    /// ```
    pub fn truncate(&mut self) {
        // Create a new empty HashMap
        self.data = Arc::new(HashMap::new());
    }
}

impl KVEngineStoreTrait for KvEngine {
    /// Retrieve a value by its key.
    ///
    /// # Arguments
    /// * `key` - The key to look up
    ///
    /// # Returns
    /// * `Option<String>` - The value if found, None otherwise
    fn get(&self, key: &str) -> Option<String> {
        self.data.get(key).cloned()
    }

    /// Store a key-value pair.
    ///
    /// ⚠️ **WARNING**: This method is NOT thread-safe!
    ///
    /// This operation creates a new HashMap with the updated data due to the
    /// immutable nature of the `Arc<HashMap>` design. This is inefficient but
    /// simple for the current prototype.
    ///
    /// # Arguments
    /// * `key` - The key to store
    /// * `value` - The value to associate with the key
    ///
    /// # Returns
    /// * `Result<()>` - Success or error
    ///
    /// # Thread Safety
    /// ⚠️ This method is NOT safe for concurrent access!
    /// Concurrent writes can lead to data corruption or lost updates.
    fn set(&self, key: String, value: String) -> Result<()> {
        // This is unsafe for concurrent access!
        // We need to clone the HashMap, modify it, and create a new Arc
        let mut new_data = HashMap::clone(&self.data);
        new_data.insert(key, value);
        // This is a race condition if multiple threads do this simultaneously
        unsafe {
            let arc_ptr = Arc::into_raw(self.data.clone());
            let mutex_ptr = arc_ptr as *mut HashMap<String, String>;
            *mutex_ptr = new_data;
            let _ = Arc::from_raw(arc_ptr);
        }
        Ok(())
    }

    /// Delete a key-value pair.
    ///
    /// ⚠️ **WARNING**: This method is NOT thread-safe!
    ///
    /// Like `set`, this creates a new HashMap without the deleted key.
    ///
    /// # Arguments
    /// * `key` - The key to delete
    ///
    /// # Returns
    /// * `bool` - True if the key existed and was deleted, false otherwise
    ///
    /// # Thread Safety
    /// ⚠️ This method is NOT safe for concurrent access!
    fn delete(&self, key: &str) -> bool {
        // This is unsafe for concurrent access!
        let mut new_data = HashMap::clone(&self.data);
        let existed = new_data.remove(key).is_some();
        if existed {
            unsafe {
                let arc_ptr = Arc::into_raw(self.data.clone());
                let mutex_ptr = arc_ptr as *mut HashMap<String, String>;
                *mutex_ptr = new_data;
                let _ = Arc::from_raw(arc_ptr);
            }
        }
        existed
    }

    /// Get all keys currently stored in the engine.
    ///
    /// # Returns
    /// * `Vec<String>` - Vector of all keys in the store
    fn keys(&self) -> Vec<String> {
        self.data.keys().cloned().collect()
    }

    /// Get the number of key-value pairs in the store.
    ///
    /// # Returns
    /// * `usize` - Number of key-value pairs
    fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the store is empty.
    ///
    /// # Returns
    /// * `bool` - True if the store is empty, false otherwise
    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    
    /// Increment a numeric value.
    ///
    /// # Arguments
    /// * `key` - The key to increment
    /// * `amount` - The amount to increment by (default: 1)
    ///
    /// # Returns
    /// * `Result<i64>` - The new value after incrementing, or error if not a valid number
    fn increment(&self, key: &str, amount: Option<i64>) -> Result<i64> {
        // This is unsafe for concurrent access!
        let mut new_data = HashMap::clone(&self.data);
        
        // Default increment amount is 1
        let increment_by = amount.unwrap_or(1);
        
        // Get the current value or initialize to 0
        let current_value = match new_data.get(key) {
            Some(value) => {
                // Try to parse the current value as a number
                value.parse::<i64>().map_err(|_| {
                    anyhow::anyhow!("Value for key '{}' is not a valid number", key)
                })?
            }
            None => 0, // Key doesn't exist, start from 0
        };
        
        // Calculate the new value
        let new_value = current_value + increment_by;
        
        // Store the new value
        new_data.insert(key.to_string(), new_value.to_string());
        
        unsafe {
            let arc_ptr = Arc::into_raw(self.data.clone());
            let mutex_ptr = arc_ptr as *mut HashMap<String, String>;
            *mutex_ptr = new_data;
            let _ = Arc::from_raw(arc_ptr);
        }
        
        Ok(new_value)
    }
    
    /// Decrement a numeric value.
    ///
    /// # Arguments
    /// * `key` - The key to decrement
    /// * `amount` - The amount to decrement by (default: 1)
    ///
    /// # Returns
    /// * `Result<i64>` - The new value after decrementing, or error if not a valid number
    fn decrement(&self, key: &str, amount: Option<i64>) -> Result<i64> {
        // Decrement is just a negative increment
        let decrement_by = amount.unwrap_or(1);
        self.increment(key, Some(-decrement_by))
    }
    
    /// Append a value to an existing string.
    ///
    /// # Arguments
    /// * `key` - The key to append to
    /// * `value` - The value to append
    ///
    /// # Returns
    /// * `Result<String>` - The new value after appending
    fn append(&self, key: &str, value: &str) -> Result<String> {
        // This is unsafe for concurrent access!
        let mut new_data = HashMap::clone(&self.data);
        
        // Check if the key exists
        if let Some(current_value) = new_data.get(key) {
            // Append the new value
            let new_value = format!("{}{}", current_value, value);
            
            // Store the new value
            new_data.insert(key.to_string(), new_value.clone());
            
            unsafe {
                let arc_ptr = Arc::into_raw(self.data.clone());
                let mutex_ptr = arc_ptr as *mut HashMap<String, String>;
                *mutex_ptr = new_data;
                let _ = Arc::from_raw(arc_ptr);
            }
            
            Ok(new_value)
        } else {
            // Key doesn't exist, create it with the value
            new_data.insert(key.to_string(), value.to_string());
            
            unsafe {
                let arc_ptr = Arc::into_raw(self.data.clone());
                let mutex_ptr = arc_ptr as *mut HashMap<String, String>;
                *mutex_ptr = new_data;
                let _ = Arc::from_raw(arc_ptr);
            }
            
            Ok(value.to_string())
        }
    }
    
    /// Prepend a value to an existing string.
    ///
    /// # Arguments
    /// * `key` - The key to prepend to
    /// * `value` - The value to prepend
    ///
    /// # Returns
    /// * `Result<String>` - The new value after prepending
    fn prepend(&self, key: &str, value: &str) -> Result<String> {
        // This is unsafe for concurrent access!
        let mut new_data = HashMap::clone(&self.data);
        
        // Check if the key exists
        if let Some(current_value) = new_data.get(key) {
            // Prepend the new value
            let new_value = format!("{}{}", value, current_value);
            
            // Store the new value
            new_data.insert(key.to_string(), new_value.clone());
            
            unsafe {
                let arc_ptr = Arc::into_raw(self.data.clone());
                let mutex_ptr = arc_ptr as *mut HashMap<String, String>;
                *mutex_ptr = new_data;
                let _ = Arc::from_raw(arc_ptr);
            }
            
            Ok(new_value)
        } else {
            // Key doesn't exist, create it with the value
            new_data.insert(key.to_string(), value.to_string());
            
            unsafe {
                let arc_ptr = Arc::into_raw(self.data.clone());
                let mutex_ptr = arc_ptr as *mut HashMap<String, String>;
                *mutex_ptr = new_data;
                let _ = Arc::from_raw(arc_ptr);
            }
            
            Ok(value.to_string())
        }
    }
    
    /// Clear all keys/values in the store.
    ///
    /// # Returns
    /// * `Result<()>` - Success or error
    fn truncate(&self) -> Result<()> {
        // This is unsafe for concurrent access!
        unsafe {
            let arc_ptr = Arc::into_raw(self.data.clone());
            let mutex_ptr = arc_ptr as *mut HashMap<String, String>;
            *mutex_ptr = HashMap::new();
            let _ = Arc::from_raw(arc_ptr);
        }
        
        Ok(())
    }
    
    /// Get the number of key-value pairs in the store.
    ///
    /// # Returns
    /// * `Result<u64>` - Number of key-value pairs or error
    fn count_keys(&self) -> Result<u64> {
        Ok(self.data.len() as u64)
    }
    
    /// Force synchronization of pending changes to persistent storage.
    /// For this in-memory engine, this is a no-op.
    ///
    /// # Returns
    /// * `Result<()>` - Always returns Ok(())
    fn sync(&self) -> Result<()> {
        // This is an in-memory engine, so there's nothing to sync
        // In a persistent storage engine, this would flush data to disk
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_kv_operations() {
        // Use a temporary directory for testing (even though it's not used yet)
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().to_str().unwrap();

        let mut engine = KvEngine::new(storage_path).unwrap();

        // Test basic set and get operations
        engine.set("key1".to_string(), "value1".to_string());
        assert_eq!(engine.get("key1"), Some("value1".to_string()));

        // Test overwriting an existing key
        engine.set("key1".to_string(), "new_value".to_string());
        assert_eq!(engine.get("key1"), Some("new_value".to_string()));

        // Test delete operation
        engine.delete("key1");
        assert_eq!(engine.get("key1"), None);

        // Test keys() method with multiple entries
        engine.set("key2".to_string(), "value2".to_string());
        engine.set("key3".to_string(), "value3".to_string());

        let keys = engine.keys();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"key2".to_string()));
        assert!(keys.contains(&"key3".to_string()));
    }
    
    #[test]
    fn test_increment_operations() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().to_str().unwrap();
        let mut engine = KvEngine::new(storage_path).unwrap();
        
        // Test incrementing a non-existent key (should create with value 1)
        let result = engine.increment("counter1", None).unwrap();
        assert_eq!(result, 1);
        assert_eq!(engine.get("counter1"), Some("1".to_string()));
        
        // Test incrementing with a specific amount
        let result = engine.increment("counter1", Some(5)).unwrap();
        assert_eq!(result, 6);
        assert_eq!(engine.get("counter1"), Some("6".to_string()));
        
        // Test incrementing with a negative amount
        let result = engine.increment("counter1", Some(-2)).unwrap();
        assert_eq!(result, 4);
        assert_eq!(engine.get("counter1"), Some("4".to_string()));
        
        // Test incrementing a key with non-numeric value
        engine.set("text".to_string(), "hello".to_string());
        let result = engine.increment("text", None);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_decrement_operations() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().to_str().unwrap();
        let mut engine = KvEngine::new(storage_path).unwrap();
        
        // Test decrementing a non-existent key (should create with value -1)
        let result = engine.decrement("counter1", None).unwrap();
        assert_eq!(result, -1);
        assert_eq!(engine.get("counter1"), Some("-1".to_string()));
        
        // Test decrementing with a specific amount
        let result = engine.decrement("counter1", Some(3)).unwrap();
        assert_eq!(result, -4);
        assert_eq!(engine.get("counter1"), Some("-4".to_string()));
        
        // Test decrementing a key with non-numeric value
        engine.set("text".to_string(), "hello".to_string());
        let result = engine.decrement("text", None);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_string_operations() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().to_str().unwrap();
        let mut engine = KvEngine::new(storage_path).unwrap();
        
        // Set up a key for testing
        engine.set("greeting".to_string(), "World!".to_string());
        
        // Test append to existing key
        let result = engine.append("greeting", " Hello!").unwrap();
        assert_eq!(result, "World! Hello!");
        assert_eq!(engine.get("greeting"), Some("World! Hello!".to_string()));
        
        // Test prepend to existing key
        let result = engine.prepend("greeting", "Hey! ").unwrap();
        assert_eq!(result, "Hey! World! Hello!");
        assert_eq!(engine.get("greeting"), Some("Hey! World! Hello!".to_string()));
        
        // Test append to non-existent key (should create the key)
        let result = engine.append("nonexistent", "value").unwrap();
        assert_eq!(result, "value");
        assert_eq!(engine.get("nonexistent"), Some("value".to_string()));
        
        // Test prepend to non-existent key (should create the key)
        let result = engine.prepend("another_nonexistent", "prefix").unwrap();
        assert_eq!(result, "prefix");
        assert_eq!(engine.get("another_nonexistent"), Some("prefix".to_string()));
        
        // Set up a new key for testing
        engine.set("new_key".to_string(), "Start: ".to_string());
        assert_eq!(engine.get("new_key"), Some("Start: ".to_string()));
    }
    
    #[test]
    fn test_truncate_operation() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().to_str().unwrap();
        let mut engine = KvEngine::new(storage_path).unwrap();
        
        // Add some data
        engine.set("key1".to_string(), "value1".to_string());
        engine.set("key2".to_string(), "value2".to_string());
        engine.set("key3".to_string(), "value3".to_string());
        
        // Verify data exists
        assert_eq!(engine.keys().len(), 3);
        assert_eq!(engine.get("key1"), Some("value1".to_string()));
        assert_eq!(engine.get("key2"), Some("value2".to_string()));
        assert_eq!(engine.get("key3"), Some("value3".to_string()));
        
        // Truncate the store
        engine.truncate();
        
        // Verify all data is gone
        assert_eq!(engine.keys().len(), 0);
        assert_eq!(engine.get("key1"), None);
        assert_eq!(engine.get("key2"), None);
        assert_eq!(engine.get("key3"), None);
        
        // Verify we can add new data after truncate
        engine.set("new_key".to_string(), "new_value".to_string());
        assert_eq!(engine.keys().len(), 1);
        assert_eq!(engine.get("new_key"), Some("new_value".to_string()));
    }
}
