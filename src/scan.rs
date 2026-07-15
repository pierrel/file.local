use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use ignore::{
    WalkBuilder,
    gitignore::{Gitignore, GitignoreBuilder},
};

use crate::model::{Entry, Record, RelativePath, ShareId};
use crate::state::{State, file_record};

pub fn scan(
    state: &State,
    share: &ShareId,
    root: &Path,
    previous: &[Record],
) -> Result<Vec<Record>> {
    scan_mode(state, share, root, previous, true)
}

pub fn preview(
    state: &State,
    share: &ShareId,
    root: &Path,
    previous: &[Record],
) -> Result<Vec<Record>> {
    scan_mode(state, share, root, previous, false)
}

fn scan_mode(
    state: &State,
    share: &ShareId,
    root: &Path,
    previous: &[Record],
    advance_sequence: bool,
) -> Result<Vec<Record>> {
    let peer = state.peer_id()?;
    let root_dir = cap_std::fs::Dir::open_ambient_dir(root, cap_std::ambient_authority())?;
    let mut preview_sequence = previous
        .iter()
        .map(|record| record.version.sequence)
        .max()
        .unwrap_or(0);
    let previous: BTreeMap<Vec<u8>, &Record> = previous
        .iter()
        .map(|record| (record.path.as_bytes().to_vec(), record))
        .collect();
    let mut builder = WalkBuilder::new(root);
    builder
        .hidden(false)
        .ignore(false)
        .git_ignore(true)
        .git_global(false)
        .git_exclude(false)
        .follow_links(false);

    let mut records = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for result in builder.build() {
        let dent = result?;
        if dent.path() == root {
            continue;
        }
        let relative = dent.path().strip_prefix(root)?;
        if state.is_owned_temp(share, relative)? {
            continue;
        }
        if relative.components().any(|component| {
            let name = component.as_os_str().as_encoded_bytes();
            name == b".git" || name.starts_with(b".flocal-tmp-")
        }) {
            continue;
        }
        let path = RelativePath::from_path(relative)?;
        seen.insert(path.as_bytes().to_vec());
        let metadata = root_dir.symlink_metadata(relative)?;
        let entry = if metadata.file_type().is_symlink() {
            Entry::Symlink {
                target: path_bytes(&root_dir.read_link_contents(relative)?),
            }
        } else if metadata.is_dir() {
            Entry::Directory
        } else if metadata.is_file() {
            let input = open_regular_nofollow(&root_dir, relative)?;
            let (hash, size) = if advance_sequence {
                state.store_object(input)
            } else {
                state.hash_object(input)
            }
            .with_context(|| format!("capturing {}", dent.path().display()))?;
            Entry::File {
                hash,
                size,
                executable: is_executable(&metadata),
            }
        } else {
            continue;
        };
        if let Some(old) = previous.get(path.as_bytes())
            && old.version.entry == entry
        {
            records.push((*old).clone());
        } else {
            let sequence = if advance_sequence {
                state.next_sequence(share)?
            } else {
                preview_sequence += 1;
                preview_sequence
            };
            let mut seen = previous
                .get(path.as_bytes())
                .map(|old| old.version.seen.clone())
                .unwrap_or_default();
            if let Some(old) = previous.get(path.as_bytes()) {
                remember(&mut seen, old.version.id());
            }
            records.push(file_record(
                path,
                peer.clone(),
                sequence,
                modified_ns(&metadata),
                seen,
                entry,
            ));
        }
    }
    for (bytes, old) in previous {
        if seen.contains(&bytes) || matches!(old.version.entry, Entry::Tombstone) {
            continue;
        }
        if root_dir.symlink_metadata(old.path.to_path_buf()).is_ok() {
            records.push(old.clone());
        } else {
            let sequence = if advance_sequence {
                state.next_sequence(share)?
            } else {
                preview_sequence += 1;
                preview_sequence
            };
            records.push(file_record(
                old.path.clone(),
                peer.clone(),
                sequence,
                crate::state::now_ns(),
                {
                    let mut seen = old.version.seen.clone();
                    remember(&mut seen, old.version.id());
                    seen
                },
                Entry::Tombstone,
            ));
        }
    }
    records.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
    Ok(records)
}

fn remember(seen: &mut Vec<crate::model::VersionId>, version: crate::model::VersionId) {
    if let Some(existing) = seen.iter_mut().find(|item| item.peer == version.peer) {
        existing.sequence = existing.sequence.max(version.sequence);
    } else {
        seen.push(version);
    }
}

fn modified_ns(metadata: &cap_std::fs::Metadata) -> i64 {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.into_std().duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos() as i64)
        .unwrap_or_else(crate::state::now_ns)
}

fn open_regular_nofollow(root: &cap_std::fs::Dir, relative: &Path) -> Result<std::fs::File> {
    use std::os::fd::AsFd;
    let parent = relative.parent().unwrap_or_else(|| Path::new(""));
    let name = relative.file_name().context("regular file has no name")?;
    let directory = if parent.as_os_str().is_empty() {
        root.try_clone()?
    } else {
        root.open_dir(parent)?
    };
    let fd = rustix::fs::openat(
        directory.as_fd(),
        name,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?;
    Ok(fd.into())
}

pub struct IgnoreMatcher {
    root: PathBuf,
    matcher: Gitignore,
}

impl IgnoreMatcher {
    pub fn new(root: &Path) -> Result<Self> {
        let mut builder = GitignoreBuilder::new(root);
        let root_dir = cap_std::fs::Dir::open_ambient_dir(root, cap_std::ambient_authority())?;
        let mut walker = WalkBuilder::new(root);
        walker
            .hidden(false)
            .ignore(false)
            .git_ignore(false)
            .git_global(false)
            .git_exclude(false)
            .follow_links(false);
        let mut ignore_files = Vec::new();
        for result in walker.build() {
            let dent = result?;
            let relative = dent.path().strip_prefix(root)?;
            if relative
                .components()
                .any(|component| component.as_os_str() == ".git")
            {
                continue;
            }
            if relative
                .file_name()
                .is_some_and(|name| name == ".gitignore")
                && dent.file_type().is_some_and(|kind| kind.is_file())
            {
                ignore_files.push(relative.to_path_buf());
            }
        }
        ignore_files.sort_by(|left, right| {
            left.components()
                .count()
                .cmp(&right.components().count())
                .then_with(|| left.cmp(right))
        });
        for relative in ignore_files {
            let parent = relative.parent().unwrap_or_else(|| Path::new(""));
            let directory = if parent.as_os_str().is_empty() {
                root_dir.try_clone()?
            } else {
                root_dir.open_dir(parent)?
            };
            add_ignore_file(&mut builder, &directory, root.join(relative))?;
        }
        Ok(Self {
            root: root.to_path_buf(),
            matcher: builder.build()?,
        })
    }

    pub fn is_ignored(&self, relative: &RelativePath, is_dir: bool) -> bool {
        if relative
            .to_path_buf()
            .components()
            .any(|component| component.as_os_str() == ".git")
        {
            return true;
        }
        let path = relative.to_path_buf();
        self.matcher
            .matched_path_or_any_parents(self.root.join(path), is_dir)
            .is_ignore()
    }

    pub fn is_record_ignored(&self, record: &Record) -> bool {
        match record.version.entry {
            Entry::Directory => self.is_ignored(&record.path, true),
            Entry::Tombstone => {
                self.is_ignored(&record.path, false) || self.is_ignored(&record.path, true)
            }
            _ => self.is_ignored(&record.path, false),
        }
    }
}

pub fn is_ignored(root: &Path, relative: &RelativePath, is_dir: bool) -> Result<bool> {
    Ok(IgnoreMatcher::new(root)?.is_ignored(relative, is_dir))
}

fn add_ignore_file(
    builder: &mut GitignoreBuilder,
    directory: &cap_std::fs::Dir,
    source: std::path::PathBuf,
) -> Result<()> {
    use std::io::Read;
    use std::os::fd::AsFd;
    match rustix::fs::openat(
        directory.as_fd(),
        ".gitignore",
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    ) {
        Ok(fd) => {
            let mut file = std::fs::File::from(fd);
            if !file.metadata()?.is_file() {
                return Ok(());
            }
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            for line in String::from_utf8_lossy(&bytes).lines() {
                builder.add_line(Some(source.clone()), line)?;
            }
        }
        Err(rustix::io::Errno::NOENT | rustix::io::Errno::LOOP) => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

#[cfg(unix)]
fn path_bytes(path: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes().to_vec()
}

#[cfg(unix)]
fn is_executable(metadata: &cap_std::fs::Metadata) -> bool {
    use cap_std::fs::PermissionsExt;
    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn honors_gitignore_and_excludes_git() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir_all(root.join(".git"))?;
        fs::write(root.join(".gitignore"), "ignored.txt\n")?;
        fs::write(root.join("kept.txt"), "yes")?;
        fs::write(root.join("ignored.txt"), "no")?;
        fs::write(root.join(".git/config"), "secret")?;
        let state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let records = scan(&state, &share, &root, &[])?;
        let names: Vec<_> = records.iter().map(|r| r.path.display()).collect();
        assert!(names.contains(&"kept.txt".into()));
        assert!(names.contains(&".gitignore".into()));
        assert!(!names.iter().any(|name| name.contains("ignored.txt")));
        assert!(!names.iter().any(|name| name.contains(".git/")));
        Ok(())
    }

    #[test]
    fn matcher_preserves_nested_precedence_and_ignores_symlinked_rules() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir_all(root.join("nested"))?;
        fs::write(root.join(".gitignore"), "nested/*.txt\nonly-dir/\n")?;
        fs::write(root.join("nested/.gitignore"), "!keep.txt\n")?;
        let matcher = IgnoreMatcher::new(&root)?;
        assert!(!matcher.is_ignored(
            &RelativePath::from_bytes(b"nested/keep.txt".to_vec())?,
            false
        ));
        assert!(matcher.is_ignored(
            &RelativePath::from_bytes(b"nested/drop.txt".to_vec())?,
            false
        ));
        let directory_rule = RelativePath::from_bytes(b"only-dir".to_vec())?;
        assert!(matcher.is_ignored(&directory_rule, true));
        assert!(!matcher.is_ignored(&directory_rule, false));

        fs::remove_file(root.join(".gitignore"))?;
        let outside = temp.path().join("outside-ignore");
        fs::write(&outside, "victim.txt\n")?;
        std::os::unix::fs::symlink(outside, root.join(".gitignore"))?;
        let matcher = IgnoreMatcher::new(&root)?;
        assert!(!matcher.is_ignored(&RelativePath::from_bytes(b"victim.txt".to_vec())?, false));
        Ok(())
    }
}
