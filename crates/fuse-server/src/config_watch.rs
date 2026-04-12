// SPDX-License-Identifier: Apache-2.0
//! Config change detection — watch fuse.toml for modifications.

use std::path::Path;
use std::time::{Duration, SystemTime};

/// Watch a config file for changes and notify via callback.
pub async fn watch_config(path: &Path, interval: Duration, mut on_change: impl FnMut()) {
    let mut last_modified = file_mtime(path);
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        let current = file_mtime(path);
        if current != last_modified {
            tracing::info!(path = %path.display(), "Config file changed — restart to apply");
            on_change();
            last_modified = current;
        }
    }
}

fn file_mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).ok().and_then(|m| m.modified().ok())
}

/// Check if config file has been modified since a given time.
pub fn config_changed_since(path: &Path, since: SystemTime) -> bool {
    file_mtime(path).map(|m| m > since).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_file_mtime_exists() {
        let dir = std::env::temp_dir().join("fuse_config_watch");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test.toml");
        std::fs::File::create(&path)
            .unwrap()
            .write_all(b"[engine]")
            .unwrap();
        assert!(file_mtime(&path).is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_file_mtime_missing() {
        assert!(file_mtime(Path::new("/nonexistent/file.toml")).is_none());
    }

    #[test]
    fn test_config_changed_since() {
        let dir = std::env::temp_dir().join("fuse_config_changed");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test.toml");
        let before = SystemTime::now() - Duration::from_secs(10);
        std::fs::File::create(&path)
            .unwrap()
            .write_all(b"x")
            .unwrap();
        assert!(config_changed_since(&path, before));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
