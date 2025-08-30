//! # Storage Engine Module
//!
//! This module contains the storage components for MerkleKV:
//!
//! - **`kv_trait`**: Common interface for all storage engines
//! - **`rwlock_engine`**: Thread-safe in-memory storage using RwLock<HashMap>
//! - **`kv_engine`**: Non-thread-safe in-memory storage using Arc<HashMap>
//! - **`merkle`**: Merkle tree implementation for efficient synchronization
//!
//! ## Design Philosophy
//!
//! The storage layer is designed to be modular and replaceable. All storage engines
//! implement the `KVEngineStoreTrait` interface, allowing easy swapping between
//! different implementations without changing the rest of the codebase.
//!
//! ## Future Enhancements
//!
//! - Replace in-memory storage with persistent engine (RocksDB, Sled, etc.)
//! - Add Write-Ahead Logging (WAL) for durability
//! - Implement compression and efficient serialization
//! - Add support for range queries and iteration
//! - Optimize Merkle tree for incremental updates

pub mod kv_engine;
pub mod kv_trait;
pub mod merkle;
pub mod rwlock_engine;
pub mod sled_engine;
pub mod factory;

// Re-export the trait and engines for convenience
pub use kv_engine::KvEngine;
pub use kv_trait::KVEngineStoreTrait;
pub use rwlock_engine::RwLockEngine;
pub use sled_engine::SledEngine;
pub use factory::{create_storage_engine, create_storage_engine_simple};
