use anyhow::Result;

use super::{
    kv_engine::KvEngine,
    rwlock_engine::RwLockEngine,
    sled_engine::{SledEngine, SledConfig},
    KVEngineStoreTrait,
};
use crate::config::StorageConfig;

pub fn create_storage_engine(config: &StorageConfig) -> Result<Box<dyn KVEngineStoreTrait>> {
    match config.engine {
        crate::config::StorageEngine::Memory => {
            log::info!("Creating in-memory storage engine (non-thread-safe)");
            Ok(Box::new(KvEngine::new(&config.path)?))
        }
        crate::config::StorageEngine::RwLock => {
            log::info!("Creating thread-safe in-memory storage engine");
            Ok(Box::new(RwLockEngine::new(&config.path)?))
        }
        crate::config::StorageEngine::Sled => {
            log::info!("Creating persistent Sled storage engine at {}", config.path);
            
            let cache_size = config.cache_size_mb * 1024 * 1024 / 1024;
            let max_db_size = config.max_db_size_mb * 1024 * 1024;
            
            let sled_config = SledConfig {
                compression: config.compression,
                cache_size: cache_size.max(100),
                flush_interval_ms: config.flush_interval_ms,
                max_db_size,
            };
            
            Ok(Box::new(SledEngine::with_config(&config.path, sled_config)?))
        }
    }
}

pub fn create_storage_engine_simple(
    engine_type: &str,
    storage_path: &str,
) -> Result<Box<dyn KVEngineStoreTrait>> {
    let config = match engine_type.to_lowercase().as_str() {
        "memory" | "kv" => StorageConfig {
            engine: crate::config::StorageEngine::Memory,
            path: storage_path.to_string(),
            ..Default::default()
        },
        "rwlock" => StorageConfig {
            engine: crate::config::StorageEngine::RwLock,
            path: storage_path.to_string(),
            ..Default::default()
        },
        "sled" => StorageConfig {
            engine: crate::config::StorageEngine::Sled,
            path: storage_path.to_string(),
            ..Default::default()
        },
        _ => {
            return Err(anyhow::anyhow!(
                "Unknown engine type: {}. Available engines: memory, rwlock, sled",
                engine_type
            ));
        }
    };

    create_storage_engine(&config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{StorageConfig, StorageEngine};
    use tempfile::tempdir;

    #[test]
    fn test_create_memory_engine() {
        let config = StorageConfig {
            engine: StorageEngine::Memory,
            path: "./test_data".to_string(),
            ..Default::default()
        };

        let engine = create_storage_engine(&config).unwrap();
        assert!(engine.set("key1".to_string(), "value1".to_string()).is_ok());
        assert_eq!(engine.get("key1"), Some("value1".to_string()));
    }

    #[test]
    fn test_create_rwlock_engine() {
        let config = StorageConfig {
            engine: StorageEngine::RwLock,
            path: "./test_data".to_string(),
            ..Default::default()
        };

        let engine = create_storage_engine(&config).unwrap();
        assert!(engine.set("key1".to_string(), "value1".to_string()).is_ok());
        assert_eq!(engine.get("key1"), Some("value1".to_string()));
    }

    #[test]
    fn test_create_sled_engine() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().join("test.db");

        let config = StorageConfig {
            engine: StorageEngine::Sled,
            path: storage_path.to_str().unwrap().to_string(),
            compression: true,
            cache_size_mb: 50,
            flush_interval_ms: 500,
            max_db_size_mb: 100,
        };

        let engine = create_storage_engine(&config).unwrap();
        assert!(engine.set("key1".to_string(), "value1".to_string()).is_ok());
        assert_eq!(engine.get("key1"), Some("value1".to_string()));
    }

    #[test]
    fn test_create_engine_simple() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().join("test.db");

        let engine = create_storage_engine_simple("sled", storage_path.to_str().unwrap()).unwrap();
        assert!(engine.set("key1".to_string(), "value1".to_string()).is_ok());
        assert_eq!(engine.get("key1"), Some("value1".to_string()));

        let engine = create_storage_engine_simple("rwlock", "./test_data").unwrap();
        assert!(engine.set("key2".to_string(), "value2".to_string()).is_ok());
        assert_eq!(engine.get("key2"), Some("value2".to_string()));

        let engine = create_storage_engine_simple("memory", "./test_data").unwrap();
        assert!(engine.set("key3".to_string(), "value3".to_string()).is_ok());
        assert_eq!(engine.get("key3"), Some("value3".to_string()));
    }

    #[test]
    fn test_create_engine_invalid_type() {
        let result = create_storage_engine_simple("invalid", "./test_data");
        assert!(result.is_err());
        if let Err(e) = result {
            let err_msg = e.to_string();
            assert!(err_msg.contains("Unknown engine type"));
        }
    }
}
