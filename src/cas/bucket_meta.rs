use super::{errors::FsError, fs::PTR_SIZE};
use chrono::{SecondsFormat, TimeZone, Utc};
use s3_server::dto::Bucket;
use std::convert::{TryFrom, TryInto};

#[derive(Debug)]
pub struct BucketMeta {
    ctime: i64,
    name: String,
}

impl BucketMeta {
    pub fn new(ctime: i64, name: String) -> Self {
        Self { ctime, name }
    }

    pub fn ctime(&self) -> i64 {
        self.ctime
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl From<BucketMeta> for Bucket {
    fn from(bm: BucketMeta) -> Self {
        Bucket {
            creation_date: Some(
                Utc.timestamp(bm.ctime, 0)
                    .to_rfc3339_opts(SecondsFormat::Secs, true),
            ),
            name: Some(bm.name),
        }
    }
}

impl From<&BucketMeta> for Vec<u8> {
    fn from(b: &BucketMeta) -> Self {
        let mut out = Vec::with_capacity(8 + PTR_SIZE + b.name.len());
        out.extend_from_slice(&b.ctime.to_le_bytes());
        out.extend_from_slice(&b.name.len().to_le_bytes());
        out.extend_from_slice(b.name.as_bytes());
        out
    }
}

impl TryFrom<&[u8]> for BucketMeta {
    type Error = FsError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < 8 + PTR_SIZE {
            return Err(FsError::MalformedObject);
        }
        let name_len = usize::from_le_bytes(value[8..8 + PTR_SIZE].try_into().unwrap());
        if value.len() != 8 + PTR_SIZE + name_len {
            return Err(FsError::MalformedObject);
        }
        Ok(BucketMeta {
            ctime: i64::from_le_bytes(value[..8].try_into().unwrap()),
            // SAFETY: this is safe because we only store valid strings in the first place.
            name: unsafe { String::from_utf8_unchecked(value[8 + PTR_SIZE..].to_vec()) },
        })
    }
}
