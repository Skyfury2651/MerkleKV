//! # MerkleKV - Distributed Key-Value Store
//!
//! MerkleKV is a distributed, eventually consistent key-value store that uses Merkle trees
//! for efficient data synchronization between nodes. This is an early-stage implementation
//! that demonstrates the core concepts of distributed systems.
//!
//! ## Architecture Overview
//!
//! The system consists of several key components:
//! - **Storage Engine**: In-memory key-value store (will be replaced with persistent storage)
//! - **Merkle Tree**: Cryptographic hash tree for efficient comparison of dataset states
//! - **TCP Server**: Handles client connections and command processing
//! - **Replication**: MQTT-based message passing for real-time updates
//! - **Sync Manager**: Anti-entropy protocol using Merkle trees for eventual consistency
//!
//! ## Current Status
//!
//! This is a skeleton implementation with the following limitations:
//! - Storage is in-memory only (no persistence)
//! - Sync/replication logic is stubbed out
//! - No clustering or peer discovery
//! - Basic error handling
//!
//! See the README.md for the full roadmap and implementation plan.

use anyhow::Result;
use std::path::PathBuf;

// Core modules for the MerkleKV system
mod config; // Configuration management
mod protocol; // Command parsing and protocol handling
mod replication; // MQTT-based replication (stub)
mod server; // TCP server for client connections
mod store; // Storage engine and Merkle tree
mod sync; // Anti-entropy synchronization (stub)
mod change_event; // Change event schema & codecs

// Import storage engines
use crate::store::{KVEngineStoreTrait, KvEngine, RwLockEngine};

/// Main entry point for the MerkleKV server.
///
/// This function:
/// 1. Initializes logging using env_logger
/// 2. Loads configuration from config.toml
/// 3. Creates a multi-threaded Tokio runtime
/// 4. Initializes the storage engine (configurable via config file or command line)
/// 5. Starts the TCP server to handle client connections
///
/// The server runs indefinitely until terminated by the user.
///
/// # Configuration Priority
/// 1. Command line arguments (highest priority)
/// 2. Configuration file (config.toml)
/// 3. Default values (lowest priority)
///
/// # Command Line Arguments
/// * `--config <path>` - Path to configuration file (default: config.toml)
/// * `--engine <type>` - Storage engine type: "memory", "rwlock", or "sled" (overrides config file)
/// * `--storage-path <path>` - Storage path (overrides config file)
fn main() -> Result<()> {
    // Initialize logging - use RUST_LOG environment variable to control verbosity
    // Example: RUST_LOG=info cargo run
    env_logger::init();

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let mut config_path = PathBuf::from("config.toml");
    let mut engine_type = None;
    let mut storage_path = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                if i + 1 < args.len() {
                    config_path = PathBuf::from(&args[i + 1]);
                    i += 2;
                } else {
                    eprintln!("Error: --config requires a path argument");
                    std::process::exit(1);
                }
            }
            "--engine" => {
                if i + 1 < args.len() {
                    engine_type = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("Error: --engine requires a type argument");
                    std::process::exit(1);
                }
            }
            "--storage-path" => {
                if i + 1 < args.len() {
                    storage_path = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("Error: --storage-path requires a path argument");
                    std::process::exit(1);
                }
            }
            _ => i += 1,
        }
    }

    // Load configuration from file
    let mut config = config::Config::load(&config_path)?;

    // Override with command line arguments if provided
    if let Some(engine) = engine_type {
        config.storage.engine = match engine.to_lowercase().as_str() {
            "memory" | "kv" => config::StorageEngine::Memory,
            "rwlock" => config::StorageEngine::RwLock,
            "sled" => config::StorageEngine::Sled,
            _ => {
                eprintln!("Error: Unknown engine type '{}'", engine);
                eprintln!("Available engines: memory, rwlock, sled");
                std::process::exit(1);
            }
        };
    }
    if let Some(path) = storage_path {
        config.storage.path = path;
    }

    // Create a multi-threaded async runtime for handling concurrent connections
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all() // Enable all Tokio features (timers, I/O, etc.)
        .build()?;

    // Start the server in the async runtime
    runtime.block_on(async {
        // Initialize the storage engine based on configuration
        let store: Box<dyn KVEngineStoreTrait + Send + Sync> = match config.storage.engine {
            config::StorageEngine::Memory => {
                println!("⚠️  WARNING: Using non-thread-safe Memory engine!");
                println!("   This engine is NOT safe for concurrent access.");
                println!("   Only use this for single-threaded applications or testing.");
                store::create_storage_engine(&config.storage)?
            }
            config::StorageEngine::RwLock => {
                println!("Using thread-safe RwLock engine");
                store::create_storage_engine(&config.storage)?
            }
            config::StorageEngine::Sled => {
                println!("Using persistent Sled engine at {}", config.storage.path);
                store::create_storage_engine(&config.storage)?
            }
        };

        // Create and start the TCP server
        let server = server::Server::new(config.clone(), store);
        server.run().await
    })
}
