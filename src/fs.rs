use crate::bytes::BytesStream;
use async_trait::async_trait;
use s3_server::errors::S3StorageError;
use s3_server::S3Storage;
use sled::{Db, IVec};
use std::{
    convert::{TryFrom, TryInto},
    fmt::{self, Display, Formatter},
    path::PathBuf,
};

const BLOCK_TREE: &str = "_BLOCKS";
const BLOCKID_SIZE: usize = 16;

type BlockID = [u8; BLOCKID_SIZE]; // Size of an md5 hash

pub struct CasFS {
    db: Db,
    root: PathBuf,
}

impl CasFS {
    /// Open the tree containing the block map.
    fn block_tree(&self) -> Result<sled::Tree, sled::Error> {
        self.db.open_tree(BLOCK_TREE)
    }

    /// Check if a bucket with a given name exists.
    fn bucket_exists(&self, bucket_name: &str) -> bool {
        let bn_iv = IVec::from(bucket_name);
        self.db.tree_names().contains(&bn_iv)
    }

    /// Open the tree containing the objects in a bucket.
    fn bucket(&self, bucket_name: &str) -> Result<sled::Tree, sled::Error> {
        self.db.open_tree(bucket_name)
    }
}

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

struct Object {
    size: u64,
    atime: u64,
    ctime: u64,
    mtime: u64,
    key: String,
    blocks: Vec<BlockID>,
}

impl From<Object> for Vec<u8> {
    fn from(o: Object) -> Self {
        let mut data = Vec::with_capacity(32 + 8 + o.key.len() + 8 + o.blocks.len() * BLOCKID_SIZE);

        data.extend_from_slice(&o.size.to_le_bytes());
        data.extend_from_slice(&o.atime.to_le_bytes());
        data.extend_from_slice(&o.ctime.to_le_bytes());
        data.extend_from_slice(&o.mtime.to_le_bytes());
        data.extend_from_slice(&o.key.len().to_le_bytes());
        data.extend_from_slice(o.key.as_bytes());
        data.extend_from_slice(&o.blocks.len().to_le_bytes());
        for block in o.blocks {
            data.extend_from_slice(&block);
        }

        data
    }
}

impl TryFrom<&[u8]> for Object {
    type Error = FsError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < 40 {
            return Err(FsError::MalformedObject);
        }
        // unwrap is safe since we checked the lenght of the object
        let key_len = usize::from_le_bytes(value[32..40].try_into().unwrap());
        // already check the block length is included here
        if value.len() < 40 + key_len + 8 {
            return Err(FsError::MalformedObject);
        }
        // SAFETY: This is safe as we only fill in valid strings when storing, and we checked the
        // length of the data
        let key = unsafe { String::from_utf8_unchecked(value[40..40 + key_len].to_vec()) };

        let block_len =
            usize::from_le_bytes(value[40 + key_len..40 + key_len + 8].try_into().unwrap());

        if value.len() != 48 + key_len + block_len * BLOCKID_SIZE {
            return Err(FsError::MalformedObject);
        }

        let mut blocks = Vec::with_capacity(block_len);

        for chunk in value[48 + key_len..].chunks_exact(BLOCKID_SIZE) {
            blocks.push(chunk.try_into().unwrap());
        }

        Ok(Object {
            size: u64::from_le_bytes(value[0..8].try_into().unwrap()),
            atime: u64::from_le_bytes(value[8..16].try_into().unwrap()),
            ctime: u64::from_le_bytes(value[16..24].try_into().unwrap()),
            mtime: u64::from_le_bytes(value[24..32].try_into().unwrap()),
            key,
            blocks,
        })
    }
}

#[async_trait]
impl S3Storage for CasFS {
    async fn complete_multipart_upload(
        &self,
        input: s3_server::dto::CompleteMultipartUploadRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::CompleteMultipartUploadOutput,
        s3_server::dto::CompleteMultipartUploadError,
    > {
        todo!()
    }

    async fn copy_object(
        &self,
        input: s3_server::dto::CopyObjectRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::CopyObjectOutput,
        s3_server::dto::CopyObjectError,
    > {
        todo!()
    }

    async fn create_multipart_upload(
        &self,
        input: s3_server::dto::CreateMultipartUploadRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::CreateMultipartUploadOutput,
        s3_server::dto::CreateMultipartUploadError,
    > {
        todo!()
    }

    async fn create_bucket(
        &self,
        input: s3_server::dto::CreateBucketRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::CreateBucketOutput,
        s3_server::dto::CreateBucketError,
    > {
        todo!()
    }

    async fn delete_bucket(
        &self,
        input: s3_server::dto::DeleteBucketRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::DeleteBucketOutput,
        s3_server::dto::DeleteBucketError,
    > {
        todo!()
    }

    async fn delete_object(
        &self,
        input: s3_server::dto::DeleteObjectRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::DeleteObjectOutput,
        s3_server::dto::DeleteObjectError,
    > {
        todo!()
    }

    async fn delete_objects(
        &self,
        input: s3_server::dto::DeleteObjectsRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::DeleteObjectsOutput,
        s3_server::dto::DeleteObjectsError,
    > {
        todo!()
    }

    async fn get_bucket_location(
        &self,
        input: s3_server::dto::GetBucketLocationRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::GetBucketLocationOutput,
        s3_server::dto::GetBucketLocationError,
    > {
        todo!()
    }

    async fn get_object(
        &self,
        input: s3_server::dto::GetObjectRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::GetObjectOutput,
        s3_server::dto::GetObjectError,
    > {
        todo!()
    }

    async fn head_bucket(
        &self,
        input: s3_server::dto::HeadBucketRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::HeadBucketOutput,
        s3_server::dto::HeadBucketError,
    > {
        todo!()
    }

    async fn head_object(
        &self,
        input: s3_server::dto::HeadObjectRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::HeadObjectOutput,
        s3_server::dto::HeadObjectError,
    > {
        todo!()
    }

    async fn list_buckets(
        &self,
        input: s3_server::dto::ListBucketsRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::ListBucketsOutput,
        s3_server::dto::ListBucketsError,
    > {
        todo!()
    }

    async fn list_objects(
        &self,
        input: s3_server::dto::ListObjectsRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::ListObjectsOutput,
        s3_server::dto::ListObjectsError,
    > {
        todo!()
    }

    async fn list_objects_v2(
        &self,
        input: s3_server::dto::ListObjectsV2Request,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::ListObjectsV2Output,
        s3_server::dto::ListObjectsV2Error,
    > {
        todo!()
    }

    async fn put_object(
        &self,
        input: s3_server::dto::PutObjectRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::PutObjectOutput,
        s3_server::dto::PutObjectError,
    > {
        todo!()
    }

    async fn upload_part(
        &self,
        input: s3_server::dto::UploadPartRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::UploadPartOutput,
        s3_server::dto::UploadPartError,
    > {
        todo!()
    }
}
