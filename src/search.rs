use std::path::PathBuf;

use glob::glob;
use which::which;

/// Addtional file system locations to search for binaries
/// if `initdb` and `postgres` are not in the $PATH.
const SEARCH_PATHS: [&'static str; 5] = [
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
        for entry in
            glob(&(path.to_string() + "/" + dir + "/" + name)).expect("Failed to read glob pattern")
        {
            if let Ok(path) = entry {
                return Ok(path);
            }
        }
    }
    Err(())
}
