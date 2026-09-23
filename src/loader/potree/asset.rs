use std::{path::PathBuf, sync::OnceLock};

use bytes::Bytes;
use potree::{asset::PotreeAsset, metadata::Metadata};
use thiserror::Error;

use crate::{ByteSource, ByteSourceError, FileSource, HttpSource};

/// How to reach a per-node file when the dataset uses the "multi-file"
/// octree layout (see [`OctreeLayout`]).
enum NodeFetch {
    Http { base_url: String },
    Fs { base_path: PathBuf },
}

/// Which octree storage layout a dataset uses, detected from
/// `metadata.json`'s `octreeLayout` field the first time it's read.
#[derive(Clone, Copy, PartialEq, Eq)]
enum OctreeLayout {
    /// All node point data lives in one `octree.bin`, addressed via HTTP
    /// Range requests / byte offsets. This is Potree v2's original layout.
    SingleFile,
    /// Each node's point data is its own file under `octree/<name>.bin`,
    /// fetched with a plain GET (200 response). Avoids Chrome's cache
    /// unreliability for concurrent Range requests sharing one cache key.
    MultiFile,
}

pub struct PotreeAssetSource<S: ByteSource> {
    metadata: S,
    hierarchy: S,
    octree: S,
    node_fetch: NodeFetch,
    layout: OnceLock<OctreeLayout>,
}

#[derive(Debug, Error)]
pub enum PotreeAssetSourceError {
    #[error(transparent)]
    ByteSource(#[from] ByteSourceError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

impl<S: ByteSource> PotreeAsset for PotreeAssetSource<S> {
    type Error = PotreeAssetSourceError;

    async fn read_metadata(&self) -> Result<Metadata, Self::Error> {
        let buffer = self.metadata.read_to_end(0).await?;
        let metadata: Metadata = serde_json::from_slice(&buffer)?;

        let layout = if metadata.octree_layout.as_deref() == Some("multi-file") {
            OctreeLayout::MultiFile
        } else {
            OctreeLayout::SingleFile
        };
        // `Hierarchy::load` always calls `read_metadata` exactly once before
        // any `read_octree_node` call, so this is set before it's read.
        let _ = self.layout.set(layout);

        Ok(metadata)
    }

    async fn read_hierarchy(&self, offset: u64, length: usize) -> Result<Bytes, Self::Error> {
        let buffer = self.hierarchy.read_range(offset, length as u64).await?;
        Ok(buffer.into())
    }

    async fn read_octree(&self, offset: u64, length: usize) -> Result<Bytes, Self::Error> {
        let buffer = self.octree.read_range(offset, length as u64).await?;
        Ok(buffer.into())
    }

    async fn read_octree_node(
        &self,
        name: &str,
        offset: u64,
        length: usize,
    ) -> Result<Bytes, Self::Error> {
        // Defensive fallback to single-file behavior if this is somehow
        // called before `read_metadata` — never panics.
        match self
            .layout
            .get()
            .copied()
            .unwrap_or(OctreeLayout::SingleFile)
        {
            OctreeLayout::SingleFile => self.read_octree(offset, length).await,
            OctreeLayout::MultiFile => match &self.node_fetch {
                NodeFetch::Http { base_url } => {
                    let url = format!("{base_url}/{name}.bin");
                    let bytes = HttpSource::open(&url)?.read_whole_no_range().await?;
                    Ok(bytes.into())
                }
                NodeFetch::Fs { base_path } => {
                    let bytes = FileSource::open(base_path.join(format!("{name}.bin")))?
                        .read_to_end(0)
                        .await?;
                    Ok(bytes.into())
                }
            },
        }
    }
}

impl PotreeAssetSource<FileSource> {
    pub fn from_path(
        path: impl Into<PathBuf>,
    ) -> Result<PotreeAssetSource<FileSource>, ByteSourceError> {
        let path: PathBuf = path.into();

        Ok(PotreeAssetSource {
            metadata: FileSource::open(path.join("metadata.json"))?,
            hierarchy: FileSource::open(path.join("hierarchy.bin"))?,
            octree: FileSource::open(path.join("octree.bin"))?,
            node_fetch: NodeFetch::Fs {
                base_path: path.join("octree"),
            },
            layout: OnceLock::new(),
        })
    }
}

impl PotreeAssetSource<HttpSource> {
    pub fn from_url(url: &str) -> Result<PotreeAssetSource<HttpSource>, ByteSourceError> {
        let base_url = if url.ends_with('/') {
            // remove leading /
            url.trim_end_matches('/').to_string()
        } else {
            match url.rfind('/') {
                // remove last part of the url if it ends with (metadata.json, hierarchy.bin or
                // octree.bin)
                Some(index) => {
                    let (path, end) = url.split_at(index);
                    match &end[1..] {
                        "metadata.json" | "hierarchy.bin" | "octree.bin" => path.to_string(),
                        _ => url.to_string(),
                    }
                }
                None => url.to_string(),
            }
        };

        Ok(PotreeAssetSource {
            metadata: HttpSource::open(&format!("{}/metadata.json", base_url))?,
            hierarchy: HttpSource::open(&format!("{}/hierarchy.bin", base_url))?,
            octree: HttpSource::open(&format!("{}/octree.bin", base_url))?,
            node_fetch: NodeFetch::Http {
                base_url: format!("{base_url}/octree"),
            },
            layout: OnceLock::new(),
        })
    }
}
