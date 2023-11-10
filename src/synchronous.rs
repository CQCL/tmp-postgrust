use std::convert::TryInto;
use std::fs::create_dir_all;
use std::io::BufReader;
use std::io::Lines;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::Child;
use std::process::ChildStderr;
use std::process::ChildStdout;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;

use nix::sys::signal;
use nix::sys::signal::Signal;
use nix::unistd::User;
use nix::unistd::{Pid, Uid};
use tempfile::TempDir;
use tracing::{debug, instrument};

use crate::errors::{ProcessCapture, TmpPostgrustError, TmpPostgrustResult};
use crate::search::all_dir_entries;
use crate::search::build_copy_dst_path;
use crate::search::find_postgresql_command;
use crate::POSTGRES_UID_GID;

#[instrument(skip(command, fail))]
fn exec_process(
    command: &mut Command,
    fail: impl FnOnce(ProcessCapture) -> TmpPostgrustError,
) -> TmpPostgrustResult<()> {
    debug!("running command: {:?}", command);

    let output = command
        .output()
        .map_err(|err| TmpPostgrustError::ExecSubprocessFailed {
            source: err,
            command: format!("{command:?}"),
        })?;

    if output.status.success() {
        for line in String::from_utf8(output.stdout).unwrap().lines() {
            debug!("{}", line);
        }
        Ok(())
    } else {
        Err(fail(ProcessCapture {
            stdout: String::from_utf8(output.stdout).unwrap(),
            stderr: String::from_utf8(output.stderr).unwrap(),
        }))
    }
}

#[instrument]
pub(crate) fn start_postgres_subprocess(
    data_directory: &Path,
    port: u32,
) -> TmpPostgrustResult<Child> {
    let postgres_path =
        find_postgresql_command("bin", "postgres").expect("failed to find postgres");

    let mut command = Command::new(postgres_path);
    command
        .env("PGDATA", data_directory.to_str().unwrap())
        .arg("-p")
        .arg(port.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd_as_non_root(&mut command);
    command
        .spawn()
        .map_err(TmpPostgrustError::SpawnSubprocessFailed)
}

#[instrument]
pub(crate) fn exec_init_db(data_directory: &Path) -> TmpPostgrustResult<()> {
    let initdb_path = find_postgresql_command("bin", "initdb").expect("failed to find initdb");

    debug!("Initializing database in: {:?}", data_directory);

    let mut command = Command::new(initdb_path);
    command
        .env("PGDATA", data_directory.to_str().unwrap())
        .arg("--username=postgres");
    cmd_as_non_root(&mut command);
    exec_process(&mut command, TmpPostgrustError::InitDBFailed)
}

#[instrument]
pub(crate) fn exec_copy_dir(src_dir: &Path, dst_dir: &Path) -> TmpPostgrustResult<()> {
    let (dirs, others) = all_dir_entries(src_dir)?;

    for entry in dirs {
        create_dir_all(build_copy_dst_path(&entry, src_dir, dst_dir)?)
            .map_err(TmpPostgrustError::CopyCachedInitDBFailedFileNotFound)?;
    }

    for entry in others {
        reflink_copy::reflink_or_copy(&entry, build_copy_dst_path(&entry, src_dir, dst_dir)?)
            .map_err(TmpPostgrustError::CopyCachedInitDBFailedFileNotFound)?;
    }

    Ok(())
}

#[instrument]
pub(crate) fn chown_to_non_root(dir: &Path) -> TmpPostgrustResult<()> {
    let current_uid = Uid::effective();
    if !current_uid.is_root() {
        return Ok(());
    }

    let (uid, gid) = POSTGRES_UID_GID.get_or_init(|| {
        User::from_name("postgres")
            .ok()
            .flatten()
            .map(|u| (u.uid, u.gid))
            .expect("no user `postgres` found is system")
    });
    let mut cmd = Command::new("chown");
    cmd.arg("-R").arg(format!("{uid}:{gid}")).arg(dir);
    exec_process(&mut cmd, TmpPostgrustError::UpdatingPermissionsFailed)?;
    Ok(())
}

#[instrument]
pub(crate) fn exec_create_db(
    socket: &Path,
    port: u32,
    owner: &str,
    dbname: &str,
) -> TmpPostgrustResult<()> {
    let mut command = Command::new("createdb");
    command
        .arg("-h")
        .arg(socket)
        .arg("-p")
        .arg(port.to_string())
        .arg("-U")
        .arg("postgres")
        .arg("-O")
        .arg(owner)
        .arg("--echo")
        .arg(dbname);
    cmd_as_non_root(&mut command);
    exec_process(&mut command, TmpPostgrustError::CreateDBFailed)
}

#[instrument]
pub(crate) fn exec_create_user(socket: &Path, port: u32, username: &str) -> TmpPostgrustResult<()> {
    let mut command = Command::new("createuser");
    command
        .arg("-h")
        .arg(socket)
        .arg("-p")
        .arg(port.to_string())
        .arg("-U")
        .arg("postgres")
        .arg("--superuser")
        .arg("--echo")
        .arg(username);
    cmd_as_non_root(&mut command);
    exec_process(&mut command, TmpPostgrustError::CreateDBFailed)
}

/// `ProcessGuard` represents a postgresql process that is running in the background.
/// once the guard is dropped the process will be killed.
pub struct ProcessGuard {
    /// Allows users to read stdout by line for debugging.
    pub stdout_reader: Option<Lines<BufReader<ChildStdout>>>,
    /// Allows users to read stderr by line for debugging.
    pub stderr_reader: Option<Lines<BufReader<ChildStderr>>>,
    /// Connection string for connecting to the temporary postgresql instance.
    pub connection_string: String,

    // Signal that the postgres process should be killed.
    pub(crate) postgres_process: Child,
    // Prevent the data directory from being dropped while
    // the process is running.
    pub(crate) _data_directory: TempDir,
    // Prevent socket directory from being dropped while
    // the process is running.
    pub(crate) _socket_dir: Arc<TempDir>,
}

/// Signal that the process needs to end.
impl Drop for ProcessGuard {
    fn drop(&mut self) {
        signal::kill(
            Pid::from_raw(self.postgres_process.id().try_into().unwrap()),
            Signal::SIGINT,
        )
        .unwrap();
        self.postgres_process.wait().unwrap();
    }
}

fn cmd_as_non_root(command: &mut Command) {
    let current_uid = Uid::effective();
    if current_uid.is_root() {
        let (uid, gid) = POSTGRES_UID_GID.get_or_init(|| {
            User::from_name("postgres")
                .ok()
                .flatten()
                .map(|u| (u.uid, u.gid))
                .expect("no user `postgres` found is system")
        });
        command.uid(uid.as_raw()).gid(gid.as_raw());
        // PostgreSQL cannot be run as root, so change to default user
        command.uid(uid.as_raw()).gid(gid.as_raw());
    }
}
