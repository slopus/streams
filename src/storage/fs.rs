//! The [`Fs`] / [`File`] seam: a narrow filesystem abstraction the durability
//! layer (WAL, snapshots, segment store, recovery) routes **every** byte of disk
//! I/O through, plus the production [`RealFs`] (a thin `std::fs` wrapper).
//!
//! # Why a seam
//!
//! The crash-consistency contract (ARCHITECTURE §0.3/§4) is defined entirely at
//! the boundary between "in memory" and "on durable media": a write is only
//! durable after the right `fsync`/`fdatasync`; a rename installs a snapshot only
//! after a directory fsync; a torn tail is the logical end of the log. To *test*
//! that contract — power loss after the Nth FS call, a torn last write, an EIO on
//! `sync_data`, a rename that the FS rolls back because the directory was never
//! fsynced — the storage code must call the filesystem through an injectable
//! interface rather than `std::fs` directly. This module is that interface.
//!
//! [`RealFs`] is the **production** implementation and the **ground truth** for
//! the fake/fault implementations (which live under `#[cfg(test)]`): it forwards
//! straight to `std::fs`/`std::io` with zero added behavior, so wiring `RealFs`
//! everywhere is a transparent refactor (no observable change vs. calling
//! `std::fs` inline — same syscalls, same order, same errors).
//!
//! # The two traits
//!
//! - [`Fs`] is the **namespace**: path-level operations (open a file, rename,
//!   unlink, list a directory, create a directory tree, fsync a directory, probe
//!   existence). One `Arc<dyn Fs>` is shared by the whole durability layer.
//! - [`File`] is **one open handle**: positioned reads/writes (`read_at` /
//!   `write_at`, where `write_at` may report a *short write*), truncation
//!   (`set_len`), the two fsync flavors (`sync_data` = `fdatasync`, `sync_all` =
//!   `fsync`), and the current on-disk length (`metadata_len`).
//!
//! `write_at` deliberately returns the number of bytes written (`usize`), exactly
//! like `pwrite(2)`: a real disk (and the fault injector) may accept fewer bytes
//! than offered. Callers that need an all-or-nothing append loop until the buffer
//! is drained (see `wal`); this is what makes a *short write* a first-class,
//! testable event rather than a hidden assumption.

use std::io;
use std::path::Path;
use std::sync::Arc;

/// Options for opening a file via [`Fs::open`]. Mirrors the subset of
/// `std::fs::OpenOptions` the durability layer needs (no append-mode — the WAL
/// positions its own writes with `write_at`).
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenOpts {
    /// Create the file if it does not exist.
    pub create: bool,
    /// Open for reading.
    pub read: bool,
    /// Open for writing.
    pub write: bool,
    /// Truncate the file to zero length on open.
    pub truncate: bool,
}

impl OpenOpts {
    /// A read-only open (the recovery / snapshot-load read path).
    pub fn read_only() -> Self {
        OpenOpts {
            create: false,
            read: true,
            write: false,
            truncate: false,
        }
    }

    /// Create-or-open read+write, truncating any existing contents (a fresh
    /// `.tmp` or a fresh preallocated WAL file).
    pub fn create_truncate() -> Self {
        OpenOpts {
            create: true,
            read: true,
            write: true,
            truncate: true,
        }
    }

    /// Create-or-open read+write **without** truncating (recovery resumes appends
    /// after a recovered prefix).
    pub fn create_keep() -> Self {
        OpenOpts {
            create: true,
            read: true,
            write: true,
            truncate: false,
        }
    }

    /// Open an existing file read+write without truncating (the
    /// `truncate_active` torn-tail repair: open, `set_len`, fsync).
    pub fn rw_existing() -> Self {
        OpenOpts {
            create: false,
            read: true,
            write: true,
            truncate: false,
        }
    }
}

/// One open file handle. Positioned I/O (`*_at`) keeps handles cheaply shareable
/// and matches the WAL/segment access pattern (no implicit cursor state to keep
/// in sync). `Send` so the active WAL file can move onto the dedicated writer
/// thread; not required `Sync` (each handle is owned by the one thread that holds
/// it — the WAL writer thread, or a recovery/snapshot call).
pub trait File: Send {
    /// Read up to `buf.len()` bytes starting at byte `offset`, returning the
    /// number read (0 at/after EOF; a short read near EOF is normal).
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;

    /// Write `buf` starting at byte `offset`, returning the number of bytes
    /// written — which **may be fewer than `buf.len()`** (a short write, exactly
    /// like `pwrite(2)`). Callers requiring all-or-nothing must loop.
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> io::Result<usize>;

    /// Set the file's length (extend with zeros / truncate). Used for WAL
    /// preallocation and torn-tail truncation.
    fn set_len(&mut self, len: u64) -> io::Result<()>;

    /// Flush file **data** (and the minimal metadata to read it back) to durable
    /// media — `fdatasync(2)`. The WAL group-commit barrier.
    fn sync_data(&self) -> io::Result<()>;

    /// Flush file data **and all metadata** to durable media — `fsync(2)`. Used
    /// for snapshot/segment `.tmp` files and post-truncate hardening.
    fn sync_all(&self) -> io::Result<()>;

    /// The file's current on-disk length in bytes.
    fn metadata_len(&self) -> io::Result<u64>;

    /// Read the entire file from `offset` to EOF into `out` (appending). A
    /// convenience used by the whole-file readers (WAL replay, snapshot load).
    /// The default loops `read_at`; `RealFs` overrides it with `read_to_end`.
    fn read_to_end_from(&self, mut offset: u64, out: &mut Vec<u8>) -> io::Result<()> {
        let mut tmp = [0u8; 64 * 1024];
        loop {
            let n = self.read_at(offset, &mut tmp)?;
            if n == 0 {
                return Ok(());
            }
            out.extend_from_slice(&tmp[..n]);
            offset += n as u64;
        }
    }
}

/// The filesystem namespace the durability layer routes path-level operations
/// through. One `Arc<dyn Fs>` is shared by the WAL, snapshot writer, segment
/// store, and recovery so a single injected implementation governs **all** disk
/// I/O for a data dir. Must be `Send + Sync` (shared across the engine's worker
/// threads and the WAL writer thread).
pub trait Fs: Send + Sync {
    /// Open (or create, per `opts`) the file at `path`.
    fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<Box<dyn File>>;

    /// Atomically rename `from` to `to` (replacing `to`). Durable only after a
    /// subsequent [`Fs::sync_dir`] of the containing directory (a real FS may
    /// roll the rename back on crash before the dir fsync; the fakes model this).
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Remove the file at `path`.
    fn remove_file(&self, path: &Path) -> io::Result<()>;

    /// List the entries of directory `dir`, returning their full paths. A missing
    /// directory yields an empty list (callers treat "no dir" as "no files").
    fn read_dir(&self, dir: &Path) -> io::Result<Vec<std::path::PathBuf>>;

    /// fsync directory `dir` so a contained rename/create/unlink is durable.
    /// Best-effort: a platform that cannot open a directory for fsync returns
    /// `Ok(())` (the same tolerance the inline code had).
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;

    /// Create `dir` and all missing parents.
    fn create_dir_all(&self, dir: &Path) -> io::Result<()>;

    /// Whether `path` exists.
    fn exists(&self, path: &Path) -> bool;

    /// The length of the file at `path`, or an error if it cannot be stat'd
    /// (used by the segment store's `len`). A missing file surfaces as a
    /// `NotFound` `io::Error`.
    fn metadata_len(&self, path: &Path) -> io::Result<u64>;
}

// ---------------------------------------------------------------------------
// RealFs — production + ground truth
// ---------------------------------------------------------------------------

/// The production [`Fs`]: a thin, behavior-preserving wrapper over `std::fs` /
/// `std::io`. Every method forwards directly to the standard library with no
/// added buffering, retry, or reordering, so swapping inline `std::fs` calls for
/// `RealFs` is a transparent refactor. Also the ground truth the in-memory fakes
/// are validated against.
#[derive(Debug, Clone, Default)]
pub struct RealFs;

impl RealFs {
    /// Construct an `Arc<dyn Fs>` for the production wiring.
    pub fn arc() -> Arc<dyn Fs> {
        Arc::new(RealFs)
    }
}

/// A real open file: an owned `std::fs::File` doing positioned I/O.
struct RealFile {
    file: std::fs::File,
}

impl File for RealFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        read_at_impl(&self.file, offset, buf)
    }

    fn write_at(&mut self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        write_at_impl(&self.file, offset, buf)
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn metadata_len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn read_to_end_from(&self, offset: u64, out: &mut Vec<u8>) -> io::Result<()> {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = self.file.try_clone()?;
        f.seek(SeekFrom::Start(offset))?;
        f.read_to_end(out)?;
        Ok(())
    }
}

#[cfg(unix)]
fn read_at_impl(file: &std::fs::File, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.read_at(buf, offset)
}

#[cfg(unix)]
fn write_at_impl(file: &std::fs::File, offset: u64, buf: &[u8]) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.write_at(buf, offset)
}

#[cfg(not(unix))]
fn read_at_impl(file: &std::fs::File, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = file.try_clone()?;
    f.seek(SeekFrom::Start(offset))?;
    f.read(buf)
}

#[cfg(not(unix))]
fn write_at_impl(file: &std::fs::File, offset: u64, buf: &[u8]) -> io::Result<usize> {
    use std::io::{Seek, SeekFrom, Write};
    let mut f = file.try_clone()?;
    f.seek(SeekFrom::Start(offset))?;
    f.write(buf)
}

impl Fs for RealFs {
    fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<Box<dyn File>> {
        let file = std::fs::OpenOptions::new()
            .create(opts.create)
            .read(opts.read)
            .write(opts.write)
            .truncate(opts.truncate)
            .open(path)?;
        Ok(Box::new(RealFile { file }))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn read_dir(&self, dir: &Path) -> io::Result<Vec<std::path::PathBuf>> {
        let mut out = Vec::new();
        let rd = match std::fs::read_dir(dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(e),
        };
        for entry in rd {
            out.push(entry?.path());
        }
        Ok(out)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        // Best-effort, matching the prior inline `fsync_dir`: a platform that
        // cannot open a directory as a file just skips the fsync.
        match std::fs::File::open(dir) {
            Ok(f) => f.sync_all(),
            Err(_) => Ok(()),
        }
    }

    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        std::fs::create_dir_all(dir)
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn metadata_len(&self, path: &Path) -> io::Result<u64> {
        Ok(std::fs::metadata(path)?.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `RealFs` round-trips a positioned write/read and reports length.
    #[test]
    fn realfs_write_read_len() {
        let dir = tempfile::tempdir().unwrap();
        let fs = RealFs;
        let p = dir.path().join("f");
        {
            let mut f = fs.open(&p, OpenOpts::create_truncate()).unwrap();
            let n = f.write_at(0, b"hello world").unwrap();
            assert_eq!(n, 11);
            f.sync_all().unwrap();
            assert_eq!(f.metadata_len().unwrap(), 11);
        }
        let f = fs.open(&p, OpenOpts::read_only()).unwrap();
        let mut buf = [0u8; 5];
        let n = f.read_at(6, &mut buf).unwrap();
        assert_eq!(&buf[..n], b"world");
        let mut all = Vec::new();
        f.read_to_end_from(0, &mut all).unwrap();
        assert_eq!(all, b"hello world");
    }

    /// `set_len` truncates and extends; `read_at` past EOF reads zero bytes.
    #[test]
    fn realfs_set_len_and_eof() {
        let dir = tempfile::tempdir().unwrap();
        let fs = RealFs;
        let p = dir.path().join("g");
        let mut f = fs.open(&p, OpenOpts::create_truncate()).unwrap();
        f.write_at(0, b"0123456789").unwrap();
        f.set_len(4).unwrap();
        assert_eq!(f.metadata_len().unwrap(), 4);
        let mut buf = [0u8; 8];
        let n = f.read_at(0, &mut buf).unwrap();
        assert_eq!(&buf[..n], b"0123");
        assert_eq!(f.read_at(100, &mut buf).unwrap(), 0, "read past EOF ⇒ 0");
    }

    /// `rename` + `sync_dir` + `read_dir` + `remove_file` + `exists` behave like
    /// `std::fs`.
    #[test]
    fn realfs_dir_ops() {
        let dir = tempfile::tempdir().unwrap();
        let fs = RealFs;
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        {
            let mut f = fs.open(&a, OpenOpts::create_truncate()).unwrap();
            f.write_at(0, b"x").unwrap();
            f.sync_all().unwrap();
        }
        assert!(fs.exists(&a));
        fs.rename(&a, &b).unwrap();
        fs.sync_dir(dir.path()).unwrap();
        assert!(!fs.exists(&a));
        assert!(fs.exists(&b));
        let entries = fs.read_dir(dir.path()).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(fs.metadata_len(&b).unwrap(), 1);
        fs.remove_file(&b).unwrap();
        assert!(!fs.exists(&b));
        // A missing directory lists as empty, not an error.
        assert!(fs.read_dir(&dir.path().join("nope")).unwrap().is_empty());
    }

    /// `create_dir_all` is idempotent.
    #[test]
    fn realfs_create_dir_all() {
        let dir = tempfile::tempdir().unwrap();
        let fs = RealFs;
        let nested = dir.path().join("x/y/z");
        fs.create_dir_all(&nested).unwrap();
        fs.create_dir_all(&nested).unwrap(); // idempotent
        assert!(fs.exists(&nested));
    }
}
