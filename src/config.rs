//! Loading a `pb::Graph` from a file on disk, in one of three formats:
//! TOML, JSON, or prost's binary wire format. The format is picked from the
//! file extension, or overridden explicitly by the caller.

use std::path::Path;

use prost::Message;

use crate::pb;

/// The on-disk encoding of a graph config file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Json,
    Toml,
    Binary,
}

impl Format {
    /// Infers a format from a file's extension: `.json` => Json, `.toml`
    /// => Toml, `.pb` => Binary. Any other extension (or none) => None.
    pub fn from_path(path: &Path) -> Option<Format> {
        match path.extension()?.to_str()? {
            "json" => Some(Format::Json),
            "toml" => Some(Format::Toml),
            "pb" => Some(Format::Binary),
            _ => None,
        }
    }
}

/// Everything that can go wrong loading a graph config.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read {path}: {source}")]
    Io {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse {path} as {format:?}: {source}")]
    Parse {
        path: std::path::PathBuf,
        format: Format,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error(
        "cannot determine config format for {path} (no --format given and the extension is not .json, .toml, or .pb)"
    )]
    UnknownFormat { path: std::path::PathBuf },
}

/// Loads a `pb::Graph` from `path`. If `format` is `None`, the format is
/// inferred from `path`'s extension via [`Format::from_path`]; an explicit
/// `format` always overrides the extension.
pub fn load(path: &Path, format: Option<Format>) -> Result<pb::Graph, ConfigError> {
    let format = match format.or_else(|| Format::from_path(path)) {
        Some(format) => format,
        None => {
            return Err(ConfigError::UnknownFormat {
                path: path.to_path_buf(),
            });
        }
    };

    let bytes = std::fs::read(path).map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;

    match format {
        Format::Json => serde_json::from_slice(&bytes).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            format,
            source: Box::new(source),
        }),
        Format::Toml => {
            let text = std::str::from_utf8(&bytes).map_err(|source| ConfigError::Parse {
                path: path.to_path_buf(),
                format,
                source: Box::new(source),
            })?;
            toml::from_str(text).map_err(|source| ConfigError::Parse {
                path: path.to_path_buf(),
                format,
                source: Box::new(source),
            })
        }
        Format::Binary => {
            pb::Graph::decode(bytes.as_slice()).map_err(|source| ConfigError::Parse {
                path: path.to_path_buf(),
                format,
                source: Box::new(source),
            })
        }
    }
}
