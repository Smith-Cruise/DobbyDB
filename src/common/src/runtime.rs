use std::sync::Arc;
use tokio::runtime::{Builder, Handle};

pub struct RuntimeManager {
    cpu_handle: Handle,
    io_handle: Handle,
    _cpu_runtime: Option<Arc<tokio::runtime::Runtime>>,
    _io_runtime: Option<Arc<tokio::runtime::Runtime>>,
}

impl Default for RuntimeManager {
    fn default() -> Self {
        if let Ok(handle) = Handle::try_current() {
            return Self {
                cpu_handle: handle.clone(),
                io_handle: handle,
                _cpu_runtime: None,
                _io_runtime: None,
            };
        }

        let cpu_num = std::thread::available_parallelism().unwrap().get();
        let cpu_runtime = Arc::new(Self::build_runtime(cpu_num * 2, "cpu-runtime"));
        let io_runtime = Arc::new(Self::build_runtime(cpu_num * 3, "io-runtime"));

        Self {
            cpu_handle: cpu_runtime.handle().clone(),
            io_handle: io_runtime.handle().clone(),
            _cpu_runtime: Some(cpu_runtime),
            _io_runtime: Some(io_runtime),
        }
    }
}

impl RuntimeManager {
    fn build_runtime(worker_threads: usize, thread_name: &str) -> tokio::runtime::Runtime {
        Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_name(thread_name)
            .enable_all()
            .build()
            .unwrap()
    }

    pub fn io_handle(&self) -> Handle {
        self.io_handle.clone()
    }

    pub fn cpu_handle(&self) -> Handle {
        self.cpu_handle.clone()
    }
}
