//! Every platform-specific filesystem operation, behind a small safe API (§9).
//!
//! Paths received from a manifest are opened one component at a time relative to a directory
//! descriptor, with `O_NOFOLLOW`, so a symlink anywhere beneath a root is refused rather than
//! followed. String-prefix checks alone cannot provide that guarantee.
//!
//! Durability notes:
//! - Dropping a file handle is not a checked durability operation; callers sync explicitly.
//! - A new or renamed directory entry is durable only after its parent directory is synced.
//! - On macOS, std's `sync_all`/`sync_data` issue `F_FULLFSYNC`; macOS is still only a
//!   development platform (see `FilesystemProfile`).

use std::fs::File;
use std::io;
use std::os::fd::{AsFd, OwnedFd};
use std::path::Path;

use rustix::fs::{FlockOperation, Mode, OFlags, RenameFlags};

/// Opens a trusted directory path from configuration (the receiver root or a spool).
pub fn open_dir(path: &Path) -> io::Result<OwnedFd> {
    Ok(rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )?)
}

fn open_component_dir(base: impl AsFd, name: &str) -> io::Result<OwnedFd> {
    Ok(rustix::fs::openat(
        base,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?)
}

/// Splits a validated relative path into its parent (if any) and final component.
pub fn split_parent(relative: &str) -> (Option<&str>, &str) {
    match relative.rsplit_once('/') {
        Some((parent, name)) => (Some(parent), name),
        None => (None, relative),
    }
}

/// Opens an existing directory beneath `base`, refusing symlinks in every component.
pub fn open_dir_beneath(base: impl AsFd, relative: &str) -> io::Result<OwnedFd> {
    let mut components = relative.split('/');
    let first = components.next().unwrap_or("");
    let mut current = open_component_dir(&base, first)?;
    for name in components {
        current = open_component_dir(&current, name)?;
    }
    Ok(current)
}

/// Opens the parent directory of `relative` beneath `base` (or a duplicate of `base` itself).
fn open_parent_beneath(base: impl AsFd, relative: &str) -> io::Result<(OwnedFd, &str)> {
    let (parent, name) = split_parent(relative);
    let parent_fd = match parent {
        Some(parent) => open_dir_beneath(&base, parent)?,
        None => base.as_fd().try_clone_to_owned()?,
    };
    Ok((parent_fd, name))
}

/// Creates the directory `relative` beneath `base` if it does not exist. Its parent must exist.
/// An existing symlink or file at that name is an error.
pub fn ensure_dir_beneath(base: impl AsFd, relative: &str) -> io::Result<OwnedFd> {
    let (parent, name) = open_parent_beneath(&base, relative)?;
    match rustix::fs::mkdirat(&parent, name, Mode::from_raw_mode(0o700)) {
        Ok(()) => {}
        Err(rustix::io::Errno::EXIST) => {}
        Err(error) => return Err(error.into()),
    }
    open_component_dir(&parent, name)
}

/// Creates (or reopens) a regular file beneath `base` for reading and writing, refusing a
/// symlink at the final component.
pub fn create_file_beneath(base: impl AsFd, relative: &str) -> io::Result<File> {
    let (parent, name) = open_parent_beneath(&base, relative)?;
    let fd = rustix::fs::openat(
        &parent,
        name,
        OFlags::RDWR | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::from_raw_mode(0o600),
    )?;
    let file = File::from(fd);
    require_regular(&file)?;
    Ok(file)
}

/// Creates a new regular file beneath `base`; fails if anything already has that name.
pub fn create_new_file_beneath(base: impl AsFd, relative: &str, mode: u32) -> io::Result<File> {
    let (parent, name) = open_parent_beneath(&base, relative)?;
    let fd = rustix::fs::openat(
        &parent,
        name,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::from_raw_mode(mode as rustix::fs::RawMode),
    )?;
    Ok(File::from(fd))
}

/// Opens an existing regular file beneath `base`.
pub fn open_file_beneath(base: impl AsFd, relative: &str, writable: bool) -> io::Result<File> {
    let (parent, name) = open_parent_beneath(&base, relative)?;
    let access = if writable {
        OFlags::RDWR
    } else {
        OFlags::RDONLY
    };
    let fd = rustix::fs::openat(
        &parent,
        name,
        access | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    let file = File::from(fd);
    require_regular(&file)?;
    Ok(file)
}

fn require_regular(file: &File) -> io::Result<()> {
    if file.metadata()?.is_file() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "not a regular file",
        ))
    }
}

/// Persists a directory's entries (new children, renames into or out of it).
pub fn sync_dir(dir: impl AsFd) -> io::Result<()> {
    File::from(dir.as_fd().try_clone_to_owned()?).sync_all()
}

/// Persists the directory at a trusted path.
pub fn sync_dir_path(path: &Path) -> io::Result<()> {
    sync_dir(open_dir(path)?)
}

/// Atomically renames `old` to `new` and fails with `AlreadyExists` instead of replacing an
/// existing destination. Linux uses `renameat2(RENAME_NOREPLACE)`; macOS uses
/// `renameatx_np(RENAME_EXCL)`. Unsupported filesystems return an error; there is no fallback
/// to an ordinary rename, which could replace a destination.
pub fn rename_no_replace(
    old_dir: impl AsFd,
    old: &str,
    new_dir: impl AsFd,
    new: &str,
) -> io::Result<()> {
    Ok(rustix::fs::renameat_with(
        old_dir,
        old,
        new_dir,
        new,
        RenameFlags::NOREPLACE,
    )?)
}

/// Bytes available to an unprivileged user on the filesystem holding `path`.
pub fn available_bytes(path: &Path) -> io::Result<u64> {
    let stats = rustix::fs::statvfs(path)?;
    Ok(stats.f_bavail.saturating_mul(stats.f_frsize))
}

/// An exclusive advisory lock on a file, held until dropped. One receiver process (or one
/// cleanup command) may use a receiver root at a time.
pub struct ExclusiveLock {
    _file: File,
}

pub fn lock_exclusive(path: &Path) -> io::Result<ExclusiveLock> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)?;
    match rustix::fs::flock(&file, FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => Ok(ExclusiveLock { _file: file }),
        Err(rustix::io::Errno::WOULDBLOCK) => Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "another process is using this receiver root",
        )),
        Err(error) => Err(error.into()),
    }
}

/// Which durability profile a receiver root is on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilesystemProfile {
    /// Filesystem name, for example `ext4` or `apfs`.
    pub name: String,
    /// True only for the supported profile: Linux on an ext2/3/4-family filesystem.
    pub supported: bool,
}

#[cfg(target_os = "linux")]
pub fn filesystem_profile(path: &Path) -> io::Result<FilesystemProfile> {
    // The ext2, ext3 and ext4 drivers share this superblock magic.
    const EXT4_SUPER_MAGIC: i64 = 0xEF53;
    let stats = rustix::fs::statfs(path)?;
    #[allow(clippy::unnecessary_cast)]
    let magic = stats.f_type as i64;
    let name = match magic {
        EXT4_SUPER_MAGIC => "ext4".to_string(),
        0x0102_1994 => "tmpfs".to_string(),
        0x794c_7630 => "overlayfs".to_string(),
        0x9123_683E => "btrfs".to_string(),
        0x5846_5342 => "xfs".to_string(),
        other => format!("unknown (magic {other:#x})"),
    };
    Ok(FilesystemProfile {
        supported: magic == EXT4_SUPER_MAGIC,
        name,
    })
}

#[cfg(target_os = "macos")]
pub fn filesystem_profile(path: &Path) -> io::Result<FilesystemProfile> {
    let stats = rustix::fs::statfs(path)?;
    let name: String = stats
        .f_fstypename
        .iter()
        .take_while(|&&c| c != 0)
        .map(|&c| c as u8 as char)
        .collect();
    Ok(FilesystemProfile {
        name,
        supported: false,
    })
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn filesystem_profile(_path: &Path) -> io::Result<FilesystemProfile> {
    Ok(FilesystemProfile {
        name: "unknown".into(),
        supported: false,
    })
}

/// Startup self-check: proves, inside `dir`, that the operations the durability contract
/// depends on actually work here. Any failure means the receiver must not start.
///
/// 1. Create and `sync_data` a file, and sync its directory.
/// 2. No-replace rename onto an existing name must fail with `AlreadyExists` and leave both
///    files unchanged.
/// 3. No-replace rename onto a free name must succeed.
pub fn probe_durability_operations(dir: &Path) -> Result<(), String> {
    let probe = dir.join(format!("probe-{}", uuid::Uuid::new_v4().simple()));
    let result = run_probe(&probe);
    let _ = std::fs::remove_dir_all(&probe);
    result
}

fn run_probe(probe: &Path) -> Result<(), String> {
    use std::io::Write;
    std::fs::create_dir(probe).map_err(|e| format!("create probe directory: {e}"))?;
    let probe_fd = open_dir(probe).map_err(|e| format!("open probe directory: {e}"))?;
    for (name, content) in [("a", b"first".as_slice()), ("b", b"second".as_slice())] {
        let mut file = create_new_file_beneath(&probe_fd, name, 0o600)
            .map_err(|e| format!("create probe file: {e}"))?;
        file.write_all(content)
            .map_err(|e| format!("write probe file: {e}"))?;
        file.sync_data()
            .map_err(|e| format!("sync_data is not supported here: {e}"))?;
    }
    sync_dir(&probe_fd).map_err(|e| format!("directory sync is not supported here: {e}"))?;
    match rename_no_replace(&probe_fd, "a", &probe_fd, "b") {
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(format!("no-replace rename is not supported here: {e}")),
        Ok(()) => return Err("no-replace rename replaced an existing file".into()),
    }
    let kept = std::fs::read(probe.join("b")).map_err(|e| format!("read probe file: {e}"))?;
    if kept != b"second" {
        return Err("no-replace rename changed the existing destination".into());
    }
    rename_no_replace(&probe_fd, "a", &probe_fd, "c")
        .map_err(|e| format!("no-replace rename to a free name failed: {e}"))?;
    sync_dir(&probe_fd).map_err(|e| format!("directory sync failed after rename: {e}"))?;
    Ok(())
}

/// True when an I/O error means the disk or quota is full.
pub fn is_out_of_space(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(code) if code == rustix::io::Errno::NOSPC.raw_os_error()
            || code == rustix::io::Errno::DQUOT.raw_os_error()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::fs::symlink;

    #[test]
    fn beneath_operations_refuse_symlinks() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("real")).unwrap();
        symlink(outside.path(), root.path().join("link")).unwrap();
        std::fs::write(outside.path().join("secret"), b"x").unwrap();
        symlink(
            outside.path().join("secret"),
            root.path().join("real/file-link"),
        )
        .unwrap();
        let base = open_dir(root.path()).unwrap();

        assert!(open_dir_beneath(&base, "link").is_err());
        assert!(open_file_beneath(&base, "link/secret", false).is_err());
        assert!(create_file_beneath(&base, "link/new").is_err());
        assert!(open_file_beneath(&base, "real/file-link", false).is_err());
        assert!(create_file_beneath(&base, "real/file-link").is_err());
        assert!(ensure_dir_beneath(&base, "link/sub").is_err());
        assert!(!outside.path().join("new").exists());

        let mut file = create_file_beneath(&base, "real/ok").unwrap();
        file.write_all(b"data").unwrap();
        assert_eq!(std::fs::read(root.path().join("real/ok")).unwrap(), b"data");
        ensure_dir_beneath(&base, "real/sub").unwrap();
        ensure_dir_beneath(&base, "real/sub").unwrap();
        assert!(root.path().join("real/sub").is_dir());
    }

    #[test]
    fn no_replace_rename_never_overwrites() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("from"), b"new").unwrap();
        std::fs::write(root.path().join("to"), b"old").unwrap();
        let base = open_dir(root.path()).unwrap();
        let error = rename_no_replace(&base, "from", &base, "to").unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(root.path().join("to")).unwrap(), b"old");
        rename_no_replace(&base, "from", &base, "free").unwrap();
        assert_eq!(std::fs::read(root.path().join("free")).unwrap(), b"new");
    }

    #[test]
    fn probe_passes_on_a_local_filesystem() {
        let root = tempfile::tempdir().unwrap();
        probe_durability_operations(root.path()).unwrap();
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[test]
    fn second_lock_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("lock");
        let first = lock_exclusive(&path).unwrap();
        assert_eq!(
            lock_exclusive(&path).err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(first);
        lock_exclusive(&path).unwrap();
    }
}
