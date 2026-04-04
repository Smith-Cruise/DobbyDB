use std::sync::{Arc, OnceLock, RwLock};
use tokio::runtime::{Builder, Handle};

static RUNTIME_MANAGER: OnceLock<RwLock<RuntimeManager>> = OnceLock::new();

pub struct RuntimeManager {
    cpu_runtime: Arc<tokio::runtime::Runtime>,
    io_runtime: Arc<tokio::runtime::Runtime>,
}

impl RuntimeManager {
    fn new() -> Self {
        let cpu_num = std::thread::available_parallelism().unwrap().get();
        let cpu_runtime = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(cpu_num * 2)
                .thread_name("cpu-runtime")
                .enable_all()
                .build()
                .unwrap(),
        );

        let io_runtime = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(cpu_num * 3)
                .thread_name("io-runtime")
                .enable_all()
                .build()
                .unwrap(),
        );
        Self {
            cpu_runtime,
            io_runtime,
        }
    }

    pub fn io_handle(&self) -> Handle {
        self.io_runtime.handle().clone()
    }

    pub fn cpu_handle(&self) -> Handle {
        self.cpu_runtime.handle().clone()
    }
}

pub fn get_runtime_manager() -> &'static RwLock<RuntimeManager> {
    RUNTIME_MANAGER.get_or_init(|| RwLock::new(RuntimeManager::new()))
}
