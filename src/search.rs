use std::path::{Path, PathBuf};

use glob::glob;
use which::which;

use crate::errors::{TmpPostgrustError, TmpPostgrustResult};

/// Addtional file system locations to search for binaries
/// if `initdb` and `postgres` are not in the $PATH.
const SEARCH_PATHS: [&str; 5] = [
    "/usr/local/pgsql",
    "/usr/local",
    "/usr/pgsql-*",
    "/usr/lib/postgresql/*",
    "/opt/local/lib/postgresql*",
];

pub(crate) fn find_postgresql_command(dir: &str, name: &str) -> Result<PathBuf, ()> {
    // Use binaries from $PATH if available.
    if let Ok(path) = which(name) {
        return Ok(path);
    };

    // Check common install locations for the first available postgresql.
    for path in SEARCH_PATHS {
        if let Some(entry) = glob(&(path.to_string() + "/" + dir + "/" + name))
            .expect("Failed to read glob pattern")
            .flatten()
            .next()
        {
            return Ok(entry);
        }
    }
    Err(())
}

/// Return a tuple of directory paths and other paths in a sub path by recursing through
/// and reading all directories.
pub(crate) fn all_dir_entries(src_dir: &Path) -> TmpPostgrustResult<(Vec<PathBuf>, Vec<PathBuf>)> {
    let mut dirs = Vec::new();
    let mut others = Vec::new();
    for read_dir in src_dir
        .read_dir()
        .map_err(TmpPostgrustError::CopyCachedInitDBFailedFileNotFound)?
    {
        let entry = read_dir.map_err(TmpPostgrustError::CopyCachedInitDBFailedFileNotFound)?;
        let entry_file_type = entry
            .file_type()
            .map_err(TmpPostgrustError::CopyCachedInitDBFailedCouldNotReadFileType)?;

        if entry_file_type.is_dir() {
            let (sub_dirs, sub_others) = all_dir_entries(&entry.path())?;
            dirs.push(entry.path());
            dirs.extend(sub_dirs);
            others.extend(sub_others);
        } else {
            others.push(entry.path());
        }
    }
    Ok((dirs, others))
}

pub(crate) fn build_copy_dst_path(
    target_path: &Path,
    src_dir: &Path,
    dst_dir: &Path,
) -> TmpPostgrustResult<PathBuf> {
    let entry_sub_path = target_path
        .strip_prefix(src_dir)
        .map_err(TmpPostgrustError::CopyCachedInitDBFailedCouldNotStripPathPrefix)?;
    let dst_path = dst_dir.join(entry_sub_path);

    Ok(dst_path)
}
