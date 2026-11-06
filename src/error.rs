//! Command-line errors and their exit codes.
//!
//! Exit codes: 0 success, 2 invalid input, 3 transfer rejected or faulted by the receiver,
//! 4 environment or runtime failure (unreachable receiver, disk, configuration).

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExitKind {
    InvalidInput,
    Rejected,
    Environment,
}

#[derive(Clone, Debug)]
pub struct AppError {
    pub kind: ExitKind,
    pub message: String,
    /// The receiver's machine-readable error code, when the error came from a response.
    pub remote_code: Option<String>,
}

impl AppError {
    pub fn invalid(message: impl Into<String>) -> AppError {
        AppError {
            kind: ExitKind::InvalidInput,
            message: message.into(),
            remote_code: None,
        }
    }

    pub fn rejected(message: impl Into<String>) -> AppError {
        AppError {
            kind: ExitKind::Rejected,
            message: message.into(),
            remote_code: None,
        }
    }

    pub fn environment(message: impl Into<String>) -> AppError {
        AppError {
            kind: ExitKind::Environment,
            message: message.into(),
            remote_code: None,
        }
    }

    pub fn exit_code(&self) -> i32 {
        match self.kind {
            ExitKind::InvalidInput => 2,
            ExitKind::Rejected => 3,
            ExitKind::Environment => 4,
        }
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for AppError {}
