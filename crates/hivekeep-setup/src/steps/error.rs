//! The error every install step returns.

use crate::config::ConfigError;
use crate::images::ImageError;
use crate::kube::KubeError;
use crate::kustomize::KustomizeError;

#[derive(Debug, thiserror::Error)]
pub enum StepError {
    #[error(transparent)]
    Kube(#[from] KubeError),
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Image(#[from] ImageError),
    #[error(transparent)]
    Kustomize(#[from] KustomizeError),
    #[error("{0}")]
    Msg(String),
    /// A lower error with the operation it interrupted.
    #[error("{context}: {source}")]
    Context {
        context: String,
        source: Box<StepError>,
    },
}

impl StepError {
    pub fn msg(m: impl Into<String>) -> Self {
        Self::Msg(m.into())
    }

    pub fn context(self, context: impl Into<String>) -> Self {
        Self::Context {
            context: context.into(),
            source: Box::new(self),
        }
    }

    /// The innermost error.
    pub fn root(&self) -> &StepError {
        match self {
            Self::Context { source, .. } => source.root(),
            other => other,
        }
    }

    /// The cluster error underneath, if any.
    pub fn kube(&self) -> Option<&KubeError> {
        match self.root() {
            Self::Kube(k) => Some(k),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StepError {
    fn from(e: std::io::Error) -> Self {
        Self::Msg(e.to_string())
    }
}

pub type Result<T, E = StepError> = std::result::Result<T, E>;

/// `bail!("...")` returns a [`StepError::Msg`].
#[macro_export]
macro_rules! bail {
    ($($arg:tt)*) => { return Err($crate::steps::StepError::Msg(format!($($arg)*))) };
}
