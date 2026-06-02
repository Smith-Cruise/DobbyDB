use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::cache::TableScopedPath;
use datafusion::execution::cache::cache_manager::CachedFileList;
use datafusion::object_store::path::Path;
use datafusion::object_store::{ObjectMeta, ObjectStore};
use futures::StreamExt;
use std::sync::Arc;
use url::Url;

pub(super) async fn list_files_by_directories(
    state: &dyn Session,
    object_store: &Arc<dyn ObjectStore>,
    dir_locations: Vec<String>,
) -> Result<Vec<ObjectMeta>> {
    let concurrency = state.config_options().execution.meta_fetch_concurrency;
    let tasks = dir_locations.into_iter().map(|location| {
        let object_store = Arc::clone(object_store);

        async move { list_files(state, &object_store, &location).await }
    });

    let results = futures::stream::iter(tasks)
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

    Ok(results.into_iter().flatten().collect())
}

pub(super) async fn list_files(
    state: &dyn Session,
    object_store: &Arc<dyn ObjectStore>,
    directory_full_location: &str,
) -> Result<Vec<ObjectMeta>> {
    let relative_path = location_to_object_store_path(directory_full_location)?;
    let cache_key = TableScopedPath {
        table: None,
        path: relative_path.clone(),
    };

    let list_files_cache = state.runtime_env().cache_manager.get_list_files_cache();
    if let Some(cache) = &list_files_cache
        && let Some(cached_files) = cache.get(&cache_key)
    {
        return Ok(cached_files.files.as_ref().clone());
    }

    let file_object_metas: Vec<_> = object_store
        .list(Some(&relative_path))
        .collect::<Vec<_>>()
        .await;

    let mut results = Vec::new();
    for file_object_meta in file_object_metas {
        let meta = file_object_meta.map_err(|e| DataFusionError::External(Box::new(e)))?;
        let file_name = meta.location.filename().unwrap_or("");

        if meta.size == 0 {
            continue;
        }

        if file_name.starts_with('_') || file_name.starts_with('.') {
            continue;
        }
        results.push(meta);
    }

    if let Some(cache) = list_files_cache {
        cache.put(&cache_key, CachedFileList::new(results.clone()));
    }

    Ok(results)
}

fn location_to_object_store_path(location: &str) -> Result<Path> {
    let parsed = Url::parse(location).map_err(|e| DataFusionError::External(e.into()))?;
    Ok(Path::from(parsed.path().trim_start_matches('/')))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::object_store::ObjectStoreExt;
    use datafusion::object_store::memory::InMemory;
    use datafusion::object_store::path::Path;
    use datafusion::prelude::SessionContext;

    #[test]
    fn test_location_to_object_store_path() {
        let path = location_to_object_store_path(
            "s3://warehouse/hive/tpch_hive.db/textfile_no_partition_table",
        )
        .unwrap();
        assert_eq!(
            path.as_ref(),
            "hive/tpch_hive.db/textfile_no_partition_table"
        );

        let path = location_to_object_store_path(
            "s3://warehouse/hive/tpch_hive.db/textfile_partition_table/p=1",
        )
            .unwrap();
        assert_eq!(
            path.as_ref(),
            "hive/tpch_hive.db/textfile_partition_table/p=1"
        );
    }

    #[tokio::test]
    async fn test_list_files_filters_hidden_and_empty_objects() {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put_test_object(&store, "table/dt=2024-01-01/file1.parquet", b"a").await;
        put_test_object(&store, "table/dt=2024-01-01/_temporary", b"a").await;
        put_test_object(&store, "table/dt=2024-01-01/.metadata", b"a").await;
        put_test_object(&store, "table/dt=2024-01-01/empty.parquet", b"").await;

        let files = list_files(&state, &store, "memory:///table/dt=2024-01-01")
            .await
            .unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0].location.as_ref(),
            "table/dt=2024-01-01/file1.parquet"
        );
    }

    #[tokio::test]
    async fn test_list_files_reuses_cached_file_list() {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put_test_object(&store, "table/dt=2024-01-01/file1.parquet", b"a").await;

        let first_files = list_files(&state, &store, "memory:///table/dt=2024-01-01")
            .await
            .unwrap();
        assert_eq!(first_files.len(), 1);

        put_test_object(&store, "table/dt=2024-01-01/file2.parquet", b"b").await;

        let fresh_ctx = SessionContext::new();
        let fresh_state = fresh_ctx.state();
        let uncached_files = list_files(&fresh_state, &store, "memory:///table/dt=2024-01-01")
            .await
            .unwrap();
        assert_eq!(uncached_files.len(), 2);

        let cached_files = list_files(&state, &store, "memory:///table/dt=2024-01-01")
            .await
            .unwrap();
        assert_eq!(cached_files.len(), 1);
        assert_eq!(
            cached_files[0].location.as_ref(),
            "table/dt=2024-01-01/file1.parquet"
        );
    }

    #[tokio::test]
    async fn test_list_files_by_directories_across_multiple_dirs() {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put_test_object(&store, "table/dt=2024-01-01/file1.parquet", b"a").await;
        put_test_object(&store, "table/dt=2024-01-02/file2.parquet", b"bb").await;

        let files = list_files_by_directories(
            &state,
            &store,
            vec![
                "memory:///table/dt=2024-01-01".to_string(),
                "memory:///table/dt=2024-01-02".to_string(),
            ],
        )
        .await
        .unwrap();

        assert_eq!(files.len(), 2);
        let mut paths: Vec<&str> = files.iter().map(|f| f.location.as_ref()).collect();
        paths.sort();
        assert_eq!(
            paths,
            vec![
                "table/dt=2024-01-01/file1.parquet",
                "table/dt=2024-01-02/file2.parquet",
            ]
        );
    }

    async fn put_test_object(store: &Arc<dyn ObjectStore>, path: &str, data: &[u8]) {
        store
            .put(&Path::from(path), data.to_vec().into())
            .await
            .unwrap();
    }
}
