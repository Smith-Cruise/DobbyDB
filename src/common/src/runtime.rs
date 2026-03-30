use std::sync::{Arc, OnceLock, RwLock};
use tokio::runtime::{Builder, Handle};

static RUNTIME_MANAGER: OnceLock<RwLock<RuntimeManager>> = OnceLock::new();

pub struct RuntimeManager {
    io_runtime: Arc<tokio::runtime::Runtime>,
}

impl RuntimeManager {
    fn new() -> Self {
        let threads_num = std::thread::available_parallelism().unwrap().get() * 2;
        let io_runtime = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(threads_num)
                .thread_name("io-runtime")
                .enable_all()
                .build()
                .unwrap(),
        );
        Self { io_runtime }
    }

    pub fn io_handle(&self) -> Handle {
        self.io_runtime.handle().clone()
    }
}

pub fn get_runtime_manager() -> &'static RwLock<RuntimeManager> {
    RUNTIME_MANAGER.get_or_init(|| RwLock::new(RuntimeManager::new()))
}
