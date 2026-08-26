use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use ignore::gitignore::{Gitignore, GitignoreBuilder};

use crate::model::{Entry, Record, RelativePath, ShareId};
use crate::state::{State, file_record};

pub fn scan(
    state: &State,
    share: &ShareId,
    root: &Path,
    previous: &[Record],
) -> Result<Vec<Record>> {
    let root_dir = cap_std::fs::Dir::open_ambient_dir(root, cap_std::ambient_authority())?;
    scan_cap(state, share, root, &root_dir, previous)
}

pub fn preview(
    state: &State,
    share: &ShareId,
    root: &Path,
    previous: &[Record],
) -> Result<Vec<Record>> {
    let root_dir = cap_std::fs::Dir::open_ambient_dir(root, cap_std::ambient_authority())?;
    preview_cap(state, share, root, &root_dir, previous)
}

pub fn scan_cap(
    state: &State,
    share: &ShareId,
    display_root: &Path,
    root: &cap_std::fs::Dir,
    previous: &[Record],
) -> Result<Vec<Record>> {
    Ok(scan_cap_with_ignores(state, share, display_root, root, previous)?.0)
}

pub(crate) fn scan_cap_with_ignores(
    state: &State,
    share: &ShareId,
    display_root: &Path,
    root: &cap_std::fs::Dir,
    previous: &[Record],
) -> Result<(Vec<Record>, IgnoreMatcher)> {
    scan_mode(state, share, display_root, root, previous, true)
}

pub fn preview_cap(
    state: &State,
    share: &ShareId,
    display_root: &Path,
    root: &cap_std::fs::Dir,
    previous: &[Record],
) -> Result<Vec<Record>> {
    Ok(preview_cap_with_ignores(state, share, display_root, root, previous)?.0)
}

pub(crate) fn preview_cap_with_ignores(
    state: &State,
    share: &ShareId,
    display_root: &Path,
    root: &cap_std::fs::Dir,
    previous: &[Record],
) -> Result<(Vec<Record>, IgnoreMatcher)> {
    scan_mode(state, share, display_root, root, previous, false)
}

fn scan_mode(
    state: &State,
    share: &ShareId,
    display_root: &Path,
    root_dir: &cap_std::fs::Dir,
    previous: &[Record],
    advance_sequence: bool,
) -> Result<(Vec<Record>, IgnoreMatcher)> {
    let peer = state.peer_id()?;
    let shared_heads = state.shared_heads(share)?;
    let mut preview_sequence = previous
        .iter()
        .map(|record| record.version.sequence)
        .max()
        .unwrap_or(0);
    let previous: BTreeMap<Vec<u8>, &Record> = previous
        .iter()
        .map(|record| (record.path.as_bytes().to_vec(), record))
        .collect();
    let mut records = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut loaded_scopes = BTreeMap::new();
    let mut objects = state.object_store_budget()?;
    let mut directories = vec![(PathBuf::new(), None::<Arc<IgnoreScope>>)];
    while let Some((directory_path, parent_scope)) = directories.pop() {
        let scope = load_ignore_scope(
            display_root,
            root_dir,
            &directory_path,
            parent_scope.clone(),
        )?;
        if let Some(scope) = &scope
            && parent_scope
                .as_ref()
                .is_none_or(|parent| !Arc::ptr_eq(scope, parent))
        {
            loaded_scopes.insert(directory_path.clone(), scope.clone());
        }
        let directory = if directory_path.as_os_str().is_empty() {
            root_dir.try_clone()?
        } else {
            root_dir.open_dir(&directory_path)?
        };
        let mut names = directory
            .entries()?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<std::io::Result<Vec<_>>>()?;
        names.sort();
        for name in names {
            let relative = directory_path.join(name);
            if state.is_owned_temp(share, &relative)? || is_internal_path(&relative) {
                continue;
            }
            let path = RelativePath::from_path(&relative)?;
            let metadata = root_dir.symlink_metadata(&relative)?;
            if scope_is_ignored(scope.as_ref(), display_root, &relative, metadata.is_dir()) {
                continue;
            }
            seen.insert(path.as_bytes().to_vec());
            let entry = if metadata.file_type().is_symlink() {
                Entry::Symlink {
                    target: path_bytes(&root_dir.read_link_contents(&relative)?),
                }
            } else if metadata.is_dir() {
                directories.push((relative.clone(), scope.clone()));
                Entry::Directory
            } else if metadata.is_file() {
                let input = open_regular_nofollow(root_dir, &relative)?;
                let (hash, size) = objects.store_object(state, input).with_context(|| {
                    format!("capturing {}", display_root.join(&relative).display())
                })?;
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
                let mut causal = previous
                    .get(path.as_bytes())
                    .map(|old| old.version.seen.clone())
                    .unwrap_or_default();
                if let Some(old) = previous.get(path.as_bytes()) {
                    remember(&mut causal, old.version.id());
                }
                let merge_base = previous.get(path.as_bytes()).and_then(|old| {
                    if shared_heads.get(path.as_bytes()) == old.version.as_base().as_ref() {
                        old.version.as_base()
                    } else if old.version.peer == peer
                        && old.version.id_authenticator.is_some()
                        && old.version.version_authenticator.is_some()
                    {
                        old.version.merge_base.clone()
                    } else {
                        None
                    }
                });
                records.push(file_record(
                    path,
                    peer.clone(),
                    sequence,
                    modified_ns(&metadata),
                    causal,
                    entry,
                ));
                let version = &mut records.last_mut().expect("record was just pushed").version;
                version.merge_base = merge_base;
            }
        }
    }
    for (bytes, old) in previous {
        if seen.contains(&bytes) {
            continue;
        }
        // A tombstone is carried forward verbatim: dropping it before the
        // peer has synchronized the deletion would let the peer's old record
        // resurrect the path. Acknowledgment-based pruning is deferred.
        if matches!(old.version.entry, Entry::Tombstone) {
            records.push(old.clone());
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
    if advance_sequence {
        for record in &mut records {
            if record.version.id_authenticator.is_some() {
                continue;
            }
            if record.version.peer != peer {
                record.version.peer = peer.clone();
                record.version.sequence = state.next_sequence(share)?;
                record.version.seen.clear();
                record.version.merge_base = None;
            } else {
                record
                    .version
                    .seen
                    .retain(|version| version.authenticator.is_some());
                if record
                    .version
                    .merge_base
                    .as_ref()
                    .is_some_and(|base| base.authenticator.is_none())
                {
                    record.version.merge_base = None;
                }
            }
            state.authenticate_record(share, record)?;
        }
    }
    records.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
    Ok((
        records,
        IgnoreMatcher {
            root: display_root.to_path_buf(),
            scopes: loaded_scopes,
        },
    ))
}

struct IgnoreScope {
    matcher: Gitignore,
    parent: Option<Arc<IgnoreScope>>,
}

fn load_ignore_scope(
    display_root: &Path,
    root: &cap_std::fs::Dir,
    directory: &Path,
    parent: Option<Arc<IgnoreScope>>,
) -> Result<Option<Arc<IgnoreScope>>> {
    let relative = directory.join(".gitignore");
    let metadata = match root.symlink_metadata(&relative) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(parent),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspecting {}", display_root.join(relative).display()));
        }
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Ok(parent);
    }
    let source = display_root.join(&relative);
    let source_directory = if directory.as_os_str().is_empty() {
        root.try_clone()?
    } else {
        root.open_dir(directory)?
    };
    let mut builder = GitignoreBuilder::new(display_root.join(directory));
    add_ignore_file(&mut builder, &source_directory, source)?;
    Ok(Some(Arc::new(IgnoreScope {
        matcher: builder.build()?,
        parent,
    })))
}

fn scope_is_ignored(
    mut scope: Option<&Arc<IgnoreScope>>,
    display_root: &Path,
    relative: &Path,
    is_dir: bool,
) -> bool {
    while let Some(current) = scope {
        let matched = current
            .matcher
            .matched_path_or_any_parents(display_root.join(relative), is_dir);
        if !matched.is_none() {
            return matched.is_ignore();
        }
        scope = current.parent.as_ref();
    }
    false
}

fn is_internal_path(relative: &Path) -> bool {
    relative.components().any(|component| {
        let name = component.as_os_str().as_encoded_bytes();
        name == b".git" || name.starts_with(b".flocal-tmp-")
    })
}

fn remember(seen: &mut Vec<crate::model::VersionId>, version: crate::model::VersionId) {
    if let Some(existing) = seen.iter_mut().find(|item| item.peer == version.peer) {
        if version.sequence > existing.sequence {
            *existing = version;
        }
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
    scopes: BTreeMap<PathBuf, Arc<IgnoreScope>>,
}

impl IgnoreMatcher {
    pub fn new(root: &Path) -> Result<Self> {
        let root_dir = cap_std::fs::Dir::open_ambient_dir(root, cap_std::ambient_authority())?;
        Self::from_cap(root, &root_dir)
    }

    pub fn from_cap(root: &Path, root_dir: &cap_std::fs::Dir) -> Result<Self> {
        let mut scopes = BTreeMap::new();
        let mut directories = vec![(PathBuf::new(), None::<Arc<IgnoreScope>>)];
        while let Some((relative, parent)) = directories.pop() {
            let scope = load_ignore_scope(root, root_dir, &relative, parent.clone())?;
            if let Some(scope) = &scope
                && parent
                    .as_ref()
                    .is_none_or(|parent| !Arc::ptr_eq(scope, parent))
            {
                scopes.insert(relative.clone(), scope.clone());
            }
            let directory = if relative.as_os_str().is_empty() {
                root_dir.try_clone()?
            } else {
                root_dir.open_dir(&relative)?
            };
            for entry in directory.entries()? {
                let path = relative.join(entry?.file_name());
                if is_internal_path(&path) {
                    continue;
                }
                let metadata = root_dir.symlink_metadata(&path)?;
                if metadata.is_dir()
                    && !metadata.file_type().is_symlink()
                    && !scope_is_ignored(scope.as_ref(), root, &path, true)
                {
                    directories.push((path, scope.clone()));
                }
            }
        }
        Ok(Self {
            root: root.to_path_buf(),
            scopes,
        })
    }

    pub fn is_ignored(&self, relative: &RelativePath, is_dir: bool) -> bool {
        let path = relative.to_path_buf();
        let scope = path
            .ancestors()
            .find_map(|directory| self.scopes.get(directory));
        scope_is_ignored(scope, &self.root, &path, is_dir)
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
        fs::create_dir(root.join("ignored-dir"))?;
        fs::write(root.join(".gitignore"), "ignored.txt\nignored-dir/\n")?;
        fs::write(root.join("kept.txt"), "yes")?;
        fs::write(root.join("ignored.txt"), "no")?;
        fs::write(root.join(".git/config"), "secret")?;
        fs::write(root.join("ignored-dir/secret"), "no")?;
        let mut ignored_permissions = fs::metadata(root.join("ignored-dir"))?.permissions();
        use std::os::unix::fs::PermissionsExt;
        ignored_permissions.set_mode(0o000);
        fs::set_permissions(root.join("ignored-dir"), ignored_permissions)?;
        let state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let records = scan(&state, &share, &root, &[])?;
        fs::set_permissions(root.join("ignored-dir"), fs::Permissions::from_mode(0o700))?;
        let names: Vec<_> = records.iter().map(|r| r.path.display()).collect();
        assert!(names.contains(&"kept.txt".into()));
        assert!(names.contains(&".gitignore".into()));
        assert!(!names.iter().any(|name| name.contains("ignored.txt")));
        assert!(!names.iter().any(|name| name.contains("ignored-dir")));
        assert!(!names.iter().any(|name| name.contains(".git/")));
        Ok(())
    }

    #[test]
    fn matcher_preserves_nested_precedence_and_ignores_symlinked_rules() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir_all(root.join("nested"))?;
        fs::write(root.join(".gitignore"), "nested/*.txt\nonly-dir/\n")?;
        fs::create_dir_all(root.join("nested/foo"))?;
        fs::write(root.join("nested/.gitignore"), "!/keep.txt\nfoo/bar\n")?;
        fs::write(root.join("nested/keep.txt"), "yes")?;
        fs::write(root.join("nested/drop.txt"), "no")?;
        fs::write(root.join("nested/foo/bar"), "no")?;
        let matcher = IgnoreMatcher::new(&root)?;
        assert!(!matcher.is_ignored(
            &RelativePath::from_bytes(b"nested/keep.txt".to_vec())?,
            false
        ));
        assert!(matcher.is_ignored(
            &RelativePath::from_bytes(b"nested/drop.txt".to_vec())?,
            false
        ));
        assert!(matcher.is_ignored(
            &RelativePath::from_bytes(b"nested/foo/bar".to_vec())?,
            false
        ));
        let directory_rule = RelativePath::from_bytes(b"only-dir".to_vec())?;
        assert!(is_ignored(&root, &directory_rule, true)?);
        assert!(!matcher.is_ignored(&directory_rule, false));

        fs::remove_file(root.join(".gitignore"))?;
        let outside = temp.path().join("outside-ignore");
        fs::write(&outside, "victim.txt\n")?;
        std::os::unix::fs::symlink(outside, root.join(".gitignore"))?;
        let matcher = IgnoreMatcher::new(&root)?;
        assert!(!matcher.is_ignored(&RelativePath::from_bytes(b"victim.txt".to_vec())?, false));

        fs::remove_file(root.join(".gitignore"))?;
        fs::write(root.join(".gitignore"), "nested/*.txt\n")?;
        let state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let names: Vec<_> = scan(&state, &share, &root, &[])?
            .into_iter()
            .map(|record| record.path.display())
            .collect();
        assert!(names.contains(&"nested/keep.txt".into()));
        assert!(!names.contains(&"nested/drop.txt".into()));
        Ok(())
    }

    #[test]
    fn ignored_parent_prunes_nested_negation() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir_all(root.join("private"))?;
        fs::write(root.join(".gitignore"), "private/\n")?;
        fs::write(root.join("private/.gitignore"), "!secret\n")?;
        fs::write(root.join("private/secret"), "local")?;

        let matcher = IgnoreMatcher::new(&root)?;
        let secret = RelativePath::from_bytes(b"private/secret".to_vec())?;
        assert!(matcher.is_ignored(&secret, false));

        let state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let records = scan(&state, &share, &root, &[])?;
        assert!(
            !records
                .iter()
                .any(|record| record.path.display().starts_with("private"))
        );
        Ok(())
    }
}
