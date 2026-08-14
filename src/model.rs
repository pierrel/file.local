use std::cmp::Ordering;
use std::fmt;
use std::path::{Component, Path, PathBuf};

use anyhow::{Result, bail};
use base64::Engine;
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct PeerId(pub String);

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ShareId(pub String);

impl PeerId {
    pub fn generate() -> Self {
        Self(random_id("peer"))
    }
}

impl ShareId {
    pub fn generate() -> Self {
        Self(random_id("share"))
    }
}

fn random_id(prefix: &str) -> String {
    let value: [u8; 12] = rand::random();
    format!(
        "{prefix}-{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value)
    )
}

#[derive(Clone, Eq, PartialEq, Hash, Serialize)]
pub struct RelativePath(Vec<u8>);

impl RelativePath {
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        if bytes.is_empty() || bytes.contains(&0) {
            bail!("path must be non-empty and contain no NUL byte");
        }
        let path = bytes_to_path(&bytes);
        let mut normalized = PathBuf::new();
        for component in path.components() {
            match component {
                Component::Normal(name)
                    if name != ".git" && !name.as_encoded_bytes().starts_with(b".flocal-tmp-") =>
                {
                    normalized.push(name)
                }
                Component::Normal(_) => bail!("reserved path is never synchronized"),
                _ => bail!("path must be normalized and relative"),
            }
        }
        if path_to_bytes(&normalized) != bytes {
            bail!("path must use its canonical relative representation");
        }
        Ok(Self(bytes))
    }

    pub fn from_path(path: &Path) -> Result<Self> {
        Self::from_bytes(path_to_bytes(path))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn to_path_buf(&self) -> PathBuf {
        bytes_to_path(&self.0)
    }

    pub fn display(&self) -> String {
        match std::str::from_utf8(&self.0) {
            Ok(path) => path.escape_default().to_string(),
            Err(_) => self
                .0
                .iter()
                .map(|byte| match byte {
                    b' '..=b'~' if *byte != b'\\' => (*byte as char).to_string(),
                    b'\\' => "\\\\".to_owned(),
                    _ => format!("\\x{byte:02x}"),
                })
                .collect(),
        }
    }
}

impl<'de> Deserialize<'de> for RelativePath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        Self::from_bytes(bytes).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize)]
#[serde(transparent)]
pub struct ObjectHash(String);

impl ObjectHash {
    pub fn parse(value: String) -> Result<Self> {
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            bail!("object hash must be exactly 64 lowercase hexadecimal characters");
        }
        Ok(Self(value))
    }

    pub fn from_blake3(hash: blake3::Hash) -> Self {
        Self(hash.to_hex().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ObjectHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ObjectHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::parse(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

impl fmt::Debug for RelativePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RelativePath")
            .field(&self.display())
            .finish()
    }
}

#[cfg(unix)]
fn path_to_bytes(path: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes().to_vec()
}

#[cfg(unix)]
fn bytes_to_path(bytes: &[u8]) -> PathBuf {
    use std::os::unix::ffi::OsStrExt;
    PathBuf::from(std::ffi::OsStr::from_bytes(bytes))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Entry {
    File {
        hash: ObjectHash,
        size: u64,
        executable: bool,
    },
    Directory,
    Symlink {
        target: Vec<u8>,
    },
    Tombstone,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct VersionId {
    pub peer: PeerId,
    pub sequence: u64,
    #[serde(default)]
    pub authenticator: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BaseVersion {
    pub id: VersionId,
    pub entry: Entry,
    #[serde(default)]
    pub authenticator: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Version {
    pub peer: PeerId,
    pub sequence: u64,
    #[serde(default)]
    pub id_authenticator: Option<String>,
    pub timestamp_ns: i64,
    #[serde(default)]
    pub seen: Vec<VersionId>,
    #[serde(default)]
    pub merge_base: Option<BaseVersion>,
    #[serde(default)]
    pub version_authenticator: Option<String>,
    #[serde(default)]
    pub base_authenticator: Option<String>,
    pub entry: Entry,
}

impl Version {
    pub fn id(&self) -> VersionId {
        VersionId {
            peer: self.peer.clone(),
            sequence: self.sequence,
            authenticator: self.id_authenticator.clone(),
        }
    }

    pub fn as_base(&self) -> Option<BaseVersion> {
        matches!(self.entry, Entry::File { .. }).then(|| BaseVersion {
            id: self.id(),
            entry: self.entry.clone(),
            authenticator: self.base_authenticator.clone(),
        })
    }

    pub fn has_seen(&self, other: &VersionId) -> bool {
        self.id() == *other
            || self
                .seen
                .iter()
                .any(|seen| seen.peer == other.peer && seen.sequence >= other.sequence)
    }

    pub fn lww_cmp(&self, other: &Self) -> Ordering {
        self.timestamp_ns
            .cmp(&other.timestamp_ns)
            .then_with(|| self.peer.0.cmp(&other.peer.0))
            .then_with(|| self.sequence.cmp(&other.sequence))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub path: RelativePath,
    pub version: Version,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PeerConfig {
    pub peer_id: PeerId,
    pub host: String,
    pub remote_path: Vec<u8>,
    pub executable: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_rejects_traversal_and_git() {
        assert!(RelativePath::from_bytes(b"../secret".to_vec()).is_err());
        assert!(RelativePath::from_bytes(b"a/.git/config".to_vec()).is_err());
        assert!(RelativePath::from_bytes(b".flocal-tmp-peer".to_vec()).is_err());
        assert!(RelativePath::from_bytes(b"a/.flocal-tmp-peer/file".to_vec()).is_err());
        assert!(RelativePath::from_bytes(b"src/main.rs".to_vec()).is_ok());
        assert!(RelativePath::from_bytes(b"src//main.rs".to_vec()).is_err());
        assert!(RelativePath::from_bytes(b"src/./main.rs".to_vec()).is_err());
        assert!(RelativePath::from_bytes(b"src/main.rs/".to_vec()).is_err());
        assert_eq!(
            RelativePath::from_bytes(b"invalid-\xff".to_vec())
                .unwrap()
                .display(),
            "invalid-\\xff"
        );
        #[cfg(unix)]
        assert_eq!(
            RelativePath::from_bytes(b"back\\slash-\xff".to_vec())
                .unwrap()
                .display(),
            "back\\\\slash-\\xff"
        );
    }

    #[test]
    fn wire_types_reject_untrusted_paths_and_hashes() {
        assert!(serde_json::from_str::<RelativePath>(r#"[46,46,47,120]"#).is_err());
        assert!(serde_json::from_str::<RelativePath>(r#"[46,103,105,116,47,120]"#).is_err());
        let reserved = serde_json::to_string(&b"a/.flocal-tmp-peer".to_vec()).unwrap();
        assert!(serde_json::from_str::<RelativePath>(&reserved).is_err());
        assert!(serde_json::from_str::<ObjectHash>(r#""../../secret""#).is_err());
        assert!(serde_json::from_str::<ObjectHash>(r#""ABCDEF""#).is_err());
    }
}
