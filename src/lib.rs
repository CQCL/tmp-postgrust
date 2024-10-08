/*!
`tmp-postgrust` provides temporary postgresql processes that are cleaned up
after being dropped.


# Inspiration / Similar Projects
- [tmp-postgres](https://github.com/jfischoff/tmp-postgres)
- [testing.postgresql](https://github.com/tk0miya/testing.postgresql)
*/
#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]

/// Methods for Asynchronous API
#[cfg(feature = "tokio-process")]
pub mod asynchronous;
/// Common Errors
pub mod errors;
mod search;
/// Methods for Synchronous API
pub mod synchronous;

use std::fs::{metadata, set_permissions};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex, OnceLock};
use std::{fs::File, io::Write};

use ctor::dtor;
use nix::unistd::{Gid, Uid};
use tempfile::{Builder, TempDir};
use tracing::{debug, info, instrument};

use crate::errors::{TmpPostgrustError, TmpPostgrustResult};

const TMP_POSTGRUST_DB_NAME: &str = "tmp-postgrust";
const TMP_POSTGRUST_USER_NAME: &str = "tmp-postgrust-user";

pub(crate) static POSTGRES_UID_GID: OnceLock<(Uid, Gid)> = OnceLock::new();

/// As the static variables declared by this crate contain values that
/// need to be dropped at program exit to clean up resources, we use a
/// `#[dtor]` hack to drop the variables if they have been initialized.
#[dtor]
fn cleanup_static() {
    #[cfg(feature = "tokio-process")]
    if let Some(factory_mutex) = TOKIO_POSTGRES_FACTORY.get() {
        let mut guard = factory_mutex.blocking_lock();
        drop(guard.take());
    }

    if let Some(factory_mutex) = DEFAULT_POSTGRES_FACTORY.get() {
        let mut guard = factory_mutex
            .lock()
            .expect("Failed to lock default factory mutex.");
        drop(guard.take());
    }
}

static DEFAULT_POSTGRES_FACTORY: OnceLock<Mutex<Option<TmpPostgrustFactory>>> = OnceLock::new();

/// Create a new default instance, initializing the `DEFAULT_POSTGRES_FACTORY` if it
/// does not already exist.
///
/// # Errors
///
/// Will return `Err` if postgres is not installed on system
///
/// # Panics
///
/// Will panic if a `TmpPostgrustFactory::try_new` returns an error the first time the function
/// is called.
pub fn new_default_process() -> TmpPostgrustResult<synchronous::ProcessGuard> {
    let factory_mutex = DEFAULT_POSTGRES_FACTORY.get_or_init(|| {
        Mutex::new(Some(
            TmpPostgrustFactory::try_new().expect("Failed to initialize default postgres factory."),
        ))
    });
    let guard = factory_mutex
        .lock()
        .expect("Failed to lock default factory mutex.");
    let factory = guard
        .as_ref()
        .expect("Default factory is uninitialized or has been dropped.");
    factory.new_instance()
}

/// Create a new default instance, initializing the `DEFAULT_POSTGRES_FACTORY` if it
/// does not already exist. The function passed as the `migrate` parameters
/// will be run the first time the factory is initialised.
///
/// # Errors
///
/// Will return `Err` if postgres is not installed on system
///
/// # Panics
///
/// Will panic if a `TmpPostgrustFactory::try_new` returns an error the first time the function
/// is called.
pub fn new_default_process_with_migrations(
    migrate: impl Fn(&str) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
) -> TmpPostgrustResult<synchronous::ProcessGuard> {
    let factory_mutex = DEFAULT_POSTGRES_FACTORY.get_or_init(|| {
        let factory =
            TmpPostgrustFactory::try_new().expect("Failed to initialize default postgres factory.");
        factory
            .run_migrations(migrate)
            .expect("Failed to run migrations");

        Mutex::new(Some(factory))
    });
    let guard = factory_mutex
        .lock()
        .expect("Failed to lock default factory mutex.");
    let factory = guard
        .as_ref()
        .expect("Default factory is uninitialized or has been dropped.");
    factory.new_instance()
}

/// Static factory that can be re-used between tests.
#[cfg(feature = "tokio-process")]
static TOKIO_POSTGRES_FACTORY: tokio::sync::OnceCell<
    tokio::sync::Mutex<Option<TmpPostgrustFactory>>,
> = tokio::sync::OnceCell::const_new();

/// Create a new default instance, initializing the `TOKIO_POSTGRES_FACTORY` if it
/// does not already exist.
///
/// # Errors
///
/// Will return `Err` if postgres is not installed on system
///
/// # Panics
///
/// Will panic if a `TmpPostgrustFactory::try_new_async` returns an error the first time the function
/// is called.
#[cfg(feature = "tokio-process")]
pub async fn new_default_process_async() -> TmpPostgrustResult<asynchronous::ProcessGuard> {
    let factory_mutex = TOKIO_POSTGRES_FACTORY
        .get_or_try_init(|| async {
            TmpPostgrustFactory::try_new_async()
                .await
                .map(|factory| tokio::sync::Mutex::new(Some(factory)))
        })
        .await?;
    let guard = factory_mutex.lock().await;
    let factory = guard
        .as_ref()
        .expect("Default tokio factory is uninitialized or has been dropped.");
    factory.new_instance_async().await
}

/// Create a new default instance, initializing the `TOKIO_POSTGRES_FACTORY` if it
/// does not already exist. The function passed as the `migrate` parameters
/// will be run the first time the factory is initialised.
///
/// # Errors
///
/// Will return `Err` if postgres is not installed on system
///
/// # Panics
///
/// Will panic if a `TmpPostgrustFactory::try_new_async` returns an error the first time the function
/// is called.
#[cfg(feature = "tokio-process")]
pub async fn new_default_process_async_with_migrations(
    migrate: impl Fn(&str) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
) -> TmpPostgrustResult<asynchronous::ProcessGuard> {
    let factory_mutex = TOKIO_POSTGRES_FACTORY
        .get_or_try_init(|| async {
            TmpPostgrustFactory::try_new_async().await.map(|factory| {
                factory
                    .run_migrations(migrate)
                    .expect("Failed to run migrations.");
                tokio::sync::Mutex::new(Some(factory))
            })
        })
        .await?;
    let guard = factory_mutex.lock().await;
    let factory = guard
        .as_ref()
        .expect("Default tokio factory is uninitialized or has been dropped.");
    factory.new_instance_async().await
}

/// Factory for creating new temporary postgresql processes.
#[derive(Debug)]
pub struct TmpPostgrustFactory {
    socket_dir: Arc<TempDir>,
    cache_dir: Arc<TempDir>,
    config: String,
    next_port: AtomicU32,
}

impl TmpPostgrustFactory {
    /// Build a Postgresql configuration for temporary databases as a String.
    fn build_config(socket_dir: &Path) -> String {
        let mut config = String::new();
        // Minimize chance of running out of shared memory
        config.push_str("shared_buffers = '12MB'\n");
        // Disable TCP connections.
        config.push_str("listen_addresses = ''\n");
        // Listen on UNIX socket.
        config.push_str(&format!(
            "unix_socket_directories = \'{}\'\n",
            socket_dir.to_str().unwrap()
        ));

        config
    }

    /// Try to create a new factory by creating temporary directories and the necessary config.
    #[instrument]
    pub fn try_new() -> TmpPostgrustResult<TmpPostgrustFactory> {
        let socket_dir = Builder::new()
            .prefix("tmp-postgrust-socket")
            .tempdir()
            .map_err(TmpPostgrustError::CreateSocketDirFailed)?;
        let cache_dir = Builder::new()
            .prefix("tmp-postgrust-cache")
            .tempdir()
            .map_err(TmpPostgrustError::CreateCacheDirFailed)?;

        synchronous::chown_to_non_root(cache_dir.path())?;
        synchronous::chown_to_non_root(socket_dir.path())?;
        synchronous::exec_init_db(cache_dir.path())?;

        let config = TmpPostgrustFactory::build_config(socket_dir.path());

        let factory = TmpPostgrustFactory {
            socket_dir: Arc::new(socket_dir),
            cache_dir: Arc::new(cache_dir),
            config,
            next_port: AtomicU32::new(5432),
        };
        let process = factory.start_postgresql(&factory.cache_dir)?;
        synchronous::exec_create_user(process.socket_dir.path(), process.port, &process.user_name)?;
        synchronous::exec_create_db(
            process.socket_dir.path(),
            process.port,
            &process.user_name,
            &process.db_name,
        )?;

        Ok(factory)
    }

    /// Try to create a new factory by creating temporary directories and the necessary config.
    #[cfg(feature = "tokio-process")]
    #[instrument]
    pub async fn try_new_async() -> TmpPostgrustResult<TmpPostgrustFactory> {
        let socket_dir = Builder::new()
            .prefix("tmp-postgrust-socket")
            .tempdir()
            .map_err(TmpPostgrustError::CreateSocketDirFailed)?;
        let cache_dir = Builder::new()
            .prefix("tmp-postgrust-cache")
            .tempdir()
            .map_err(TmpPostgrustError::CreateCacheDirFailed)?;

        asynchronous::chown_to_non_root(cache_dir.path()).await?;
        asynchronous::chown_to_non_root(socket_dir.path()).await?;
        asynchronous::exec_init_db(cache_dir.path()).await?;

        let config = TmpPostgrustFactory::build_config(socket_dir.path());

        let factory = TmpPostgrustFactory {
            socket_dir: Arc::new(socket_dir),
            cache_dir: Arc::new(cache_dir),
            config,
            next_port: AtomicU32::new(5432),
        };
        let process = factory.start_postgresql_async(&factory.cache_dir).await?;
        asynchronous::exec_create_user(process.socket_dir.path(), process.port, &process.user_name)
            .await?;
        asynchronous::exec_create_db(
            process.socket_dir.path(),
            process.port,
            &process.user_name,
            &process.db_name,
        )
        .await?;

        Ok(factory)
    }

    /// Run migrations against the cache directory, will cause all subsequent instances
    /// to be run against a version of the database where the migrations have been applied.
    ///
    /// # Errors
    ///
    /// Will error if Postgresql is unable to start or if the migrate function returns
    /// an error.
    pub fn run_migrations(
        &self,
        migrate: impl FnOnce(&str) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> TmpPostgrustResult<()> {
        let process = self.start_postgresql(&self.cache_dir)?;

        migrate(&process.connection_string()).map_err(TmpPostgrustError::MigrationsFailed)?;

        Ok(())
    }

    /// Start a new postgresql instance and return a process guard that will ensure it is cleaned
    /// up when dropped.
    #[instrument(skip(self))]
    pub fn new_instance(&self) -> TmpPostgrustResult<synchronous::ProcessGuard> {
        let data_directory = Builder::new()
            .prefix("tmp-postgrust-db")
            .tempdir()
            .map_err(TmpPostgrustError::CreateDataDirFailed)?;
        let data_directory_path = data_directory.path();

        set_permissions(
            &data_directory,
            metadata(self.cache_dir.path()).unwrap().permissions(),
        )
        .unwrap();
        synchronous::exec_copy_dir(self.cache_dir.path(), data_directory_path)?;

        if !data_directory_path.join("PG_VERSION").exists() {
            return Err(TmpPostgrustError::EmptyDataDirectory);
        };

        self.start_postgresql(&Arc::new(data_directory))
    }

    #[instrument(skip(self))]
    fn start_postgresql(
        &self,
        dir: &Arc<TempDir>,
    ) -> TmpPostgrustResult<synchronous::ProcessGuard> {
        File::create(dir.path().join("postgresql.conf"))
            .map_err(TmpPostgrustError::CreateConfigFailed)?
            .write_all(self.config.as_bytes())
            .map_err(TmpPostgrustError::CreateConfigFailed)?;

        let port = self
            .next_port
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        synchronous::chown_to_non_root(dir.path())?;
        let mut postgres_process_handle = synchronous::start_postgres_subprocess(dir.path(), port)?;
        let stdout = postgres_process_handle.stdout.take().unwrap();
        let stderr = postgres_process_handle.stderr.take().unwrap();

        let stdout_reader = BufReader::new(stdout).lines();
        let mut stderr_reader = BufReader::new(stderr).lines();

        while let Some(Ok(line)) = stderr_reader.next() {
            debug!("Postgresql: {}", line);
            if line.contains("database system is ready to accept connections") {
                info!("temporary database system is read to accept connections");
                break;
            }
        }

        Ok(synchronous::ProcessGuard {
            stdout_reader: Some(stdout_reader),
            stderr_reader: Some(stderr_reader),
            port,
            db_name: TMP_POSTGRUST_DB_NAME.to_string(),
            user_name: TMP_POSTGRUST_USER_NAME.to_string(),
            postgres_process: postgres_process_handle,
            _data_directory: Arc::clone(dir),
            _cache_directory: Arc::clone(&self.cache_dir),
            socket_dir: Arc::clone(&self.socket_dir),
        })
    }

    /// Start a new postgresql instance and return a process guard that will ensure it is cleaned
    /// up when dropped.
    #[cfg(feature = "tokio-process")]
    #[instrument(skip(self))]
    pub async fn new_instance_async(&self) -> TmpPostgrustResult<asynchronous::ProcessGuard> {
        use tokio::fs::{metadata, set_permissions};

        let data_directory = Builder::new()
            .prefix("tmp-postgrust-db")
            .tempdir()
            .map_err(TmpPostgrustError::CreateDataDirFailed)?;
        let data_directory_path = data_directory.path();

        set_permissions(
            &data_directory,
            metadata(self.cache_dir.path()).await.unwrap().permissions(),
        )
        .await
        .unwrap();
        asynchronous::exec_copy_dir(self.cache_dir.path(), data_directory_path).await?;

        if !data_directory_path.join("PG_VERSION").exists() {
            return Err(TmpPostgrustError::EmptyDataDirectory);
        };

        self.start_postgresql_async(&Arc::new(data_directory)).await
    }

    #[cfg(feature = "tokio-process")]
    #[instrument(skip(self))]
    async fn start_postgresql_async(
        &self,
        dir: &Arc<TempDir>,
    ) -> TmpPostgrustResult<asynchronous::ProcessGuard> {
        use tokio::io::AsyncBufReadExt;

        let process_permit = asynchronous::MAX_CONCURRENT_PROCESSES
            .acquire()
            .await
            .unwrap();

        File::create(dir.path().join("postgresql.conf"))
            .map_err(TmpPostgrustError::CreateConfigFailed)?
            .write_all(self.config.as_bytes())
            .map_err(TmpPostgrustError::CreateConfigFailed)?;

        let port = self
            .next_port
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        asynchronous::chown_to_non_root(dir.path()).await?;
        let mut postgres_process_handle =
            asynchronous::start_postgres_subprocess(dir.path(), port)?;
        let stdout = postgres_process_handle.stdout.take().unwrap();
        let stderr = postgres_process_handle.stderr.take().unwrap();

        let stdout_reader = tokio::io::BufReader::new(stdout).lines();
        let mut stderr_reader = tokio::io::BufReader::new(stderr).lines();

        while let Some(line) = stderr_reader.next_line().await.unwrap() {
            debug!("Postgresql: {}", line);
            if line.contains("database system is ready to accept connections") {
                info!("temporary database system is read to accept connections");
                break;
            }
        }

        Ok(asynchronous::ProcessGuard {
            stdout_reader: Some(stdout_reader),
            stderr_reader: Some(stderr_reader),
            port,
            db_name: TMP_POSTGRUST_DB_NAME.to_string(),
            user_name: TMP_POSTGRUST_USER_NAME.to_string(),
            _data_directory: Arc::clone(dir),
            _cache_directory: Arc::clone(&self.cache_dir),
            socket_dir: Arc::clone(&self.socket_dir),
            postgres_process: postgres_process_handle,
            _process_permit: process_permit,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use test_log::test;
    use tokio_postgres::NoTls;
    use tracing::error;

    #[test(tokio::test)]
    async fn it_works() {
        let factory = TmpPostgrustFactory::try_new().expect("failed to create factory");

        let postgresql_proc = factory
            .new_instance()
            .expect("failed to create a new instance");

        let (client, conn) = tokio_postgres::connect(&postgresql_proc.connection_string(), NoTls)
            .await
            .expect("failed to connect to postgresql");

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                error!("connection error: {}", e);
            }
        });

        client.query("SELECT 1;", &[]).await.unwrap();
    }

    #[cfg(feature = "tokio-process")]
    #[test(tokio::test)]
    async fn it_works_async() {
        let factory = TmpPostgrustFactory::try_new_async()
            .await
            .expect("failed to create factory");

        let postgresql_proc = factory
            .new_instance_async()
            .await
            .expect("failed to create a new instance");

        let (client, conn) = tokio_postgres::connect(&postgresql_proc.connection_string(), NoTls)
            .await
            .expect("failed to connect to postgresql");

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                error!("connection error: {}", e);
            }
        });

        client.query("SELECT 1;", &[]).await.unwrap();
    }

    #[test(tokio::test)]
    async fn two_simulatenous_processes() {
        let factory = TmpPostgrustFactory::try_new().expect("failed to create factory");

        let proc1 = factory
            .new_instance()
            .expect("failed to create a new instance");

        let proc2 = factory
            .new_instance()
            .expect("failed to create a new instance");

        let (client1, conn1) = tokio_postgres::connect(&proc1.connection_string(), NoTls)
            .await
            .expect("failed to connect to postgresql");

        tokio::spawn(async move {
            if let Err(e) = conn1.await {
                error!("connection error: {}", e);
            }
        });

        let (client2, conn2) = tokio_postgres::connect(&proc2.connection_string(), NoTls)
            .await
            .expect("failed to connect to postgresql");

        tokio::spawn(async move {
            if let Err(e) = conn2.await {
                error!("connection error: {}", e);
            }
        });

        client1.query("SELECT 1;", &[]).await.unwrap();
        client2.query("SELECT 1;", &[]).await.unwrap();
    }

    #[cfg(feature = "tokio-process")]
    #[test(tokio::test)]
    async fn two_simulatenous_processes_async() {
        let factory = TmpPostgrustFactory::try_new_async()
            .await
            .expect("failed to create factory");

        let proc1 = factory
            .new_instance_async()
            .await
            .expect("failed to create a new instance");

        let proc2 = factory
            .new_instance_async()
            .await
            .expect("failed to create a new instance");

        let (client1, conn1) = tokio_postgres::connect(&proc1.connection_string(), NoTls)
            .await
            .expect("failed to connect to postgresql");

        tokio::spawn(async move {
            if let Err(e) = conn1.await {
                error!("connection error: {}", e);
            }
        });

        let (client2, conn2) = tokio_postgres::connect(&proc2.connection_string(), NoTls)
            .await
            .expect("failed to connect to postgresql");

        tokio::spawn(async move {
            if let Err(e) = conn2.await {
                error!("connection error: {}", e);
            }
        });

        client1.query("SELECT 1;", &[]).await.unwrap();
        client2.query("SELECT 1;", &[]).await.unwrap();
    }

    #[cfg(feature = "tokio-process")]
    #[test(tokio::test)]
    async fn default_process_factory_1() {
        let proc = new_default_process_async().await.unwrap();

        let (client, conn) = tokio_postgres::connect(&proc.connection_string(), NoTls)
            .await
            .expect("failed to connect to postgresql");

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                error!("connection error: {}", e);
            }
        });

        // Chance to catch concurrent tests or database that have already been used.
        client.execute("CREATE TABLE lock ();", &[]).await.unwrap();
    }

    #[cfg(feature = "tokio-process")]
    #[test(tokio::test)]
    async fn default_process_factory_2() {
        let proc = new_default_process_async().await.unwrap();

        let (client, conn) = tokio_postgres::connect(&proc.connection_string(), NoTls)
            .await
            .expect("failed to connect to postgresql");

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                error!("connection error: {}", e);
            }
        });

        // Chance to catch concurrent tests or database that have already been used.
        client.execute("CREATE TABLE lock ();", &[]).await.unwrap();
    }

    #[cfg(feature = "tokio-process")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn default_process_factory_multithread_1() {
        let proc = new_default_process_async().await.unwrap();

        let (client, conn) = tokio_postgres::connect(&proc.connection_string(), NoTls)
            .await
            .expect("failed to connect to postgresql");

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                error!("connection error: {}", e);
            }
        });

        // Chance to catch concurrent tests or database that have already been used.
        client.execute("CREATE TABLE lock ();", &[]).await.unwrap();
    }

    #[cfg(feature = "tokio-process")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn default_process_factory_multithread_2() {
        let proc = new_default_process_async().await.unwrap();

        let (client, conn) = tokio_postgres::connect(&proc.connection_string(), NoTls)
            .await
            .expect("failed to connect to postgresql");

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                error!("connection error: {}", e);
            }
        });

        // Chance to catch concurrent tests or database that have already been used.
        client.execute("CREATE TABLE lock ();", &[]).await.unwrap();
    }
}
