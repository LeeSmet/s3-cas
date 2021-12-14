use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone)]
pub enum FsError {
    Db(sled::Error),
    MalformedObject,
}

impl Display for FsError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Cas FS error: {}",
            match self {
                FsError::Db(e) => e as &dyn std::fmt::Display,
                FsError::MalformedObject => &"corrupt object",
            }
        )
    }
}

impl std::error::Error for FsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FsError::Db(ref e) => Some(e),
            &FsError::MalformedObject => None,
        }
    }
}

impl From<sled::Error> for FsError {
    fn from(e: sled::Error) -> Self {
        FsError::Db(e)
    }
}
