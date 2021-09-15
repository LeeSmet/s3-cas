use crate::bytes::BytesStream;
use async_trait::async_trait;
use faster_hex::hex_string;
use futures::{
    channel::mpsc::unbounded,
    sink::SinkExt,
    stream,
    stream::{StreamExt, TryStreamExt},
    AsyncRead,
};
use md5::{Digest, Md5};
use s3_server::S3Storage;
use s3_server::{dto::UploadPartRequest, errors::S3StorageError};
use serde::{Deserialize, Serialize};
use sled::{Db, IVec, Transactional};
use std::mem;
use std::{
    convert::{TryFrom, TryInto},
    fmt::{self, Display, Formatter},
    io,
    path::PathBuf,
};

const BLOCK_SIZE: usize = 1 << 20;
const BLOCK_TREE: &str = "_BLOCKS";
const PATH_TREE: &str = "_PATHS";
const MULTIPART_TREE: &str = "_MULTIPART_PARTS";
const BLOCKID_SIZE: usize = 16;
const PTR_SIZE: usize = mem::size_of::<usize>(); // Size of a `usize` in bytes

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

    /// Open the tree containing the path map.
    fn path_tree(&self) -> Result<sled::Tree, sled::Error> {
        self.db.open_tree(PATH_TREE)
    }

    /// Open the tree containing the multipart parts.
    fn multipart_tree(&self) -> Result<sled::Tree, sled::Error> {
        self.db.open_tree(MULTIPART_TREE)
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

    /// Save data on the filesystem. A list of block ID's used as keys for the data blocks is
    /// returned, along with the hash of the full byte stream.
    async fn store_bytes<R: AsyncRead + Unpin>(
        &self,
        data: BytesStream<R>,
    ) -> io::Result<(Vec<BlockID>, BlockID)> {
        let block_map = self.block_tree()?;
        let path_map = self.path_tree()?;
        let (tx, rx) = unbounded();
        let mut content_hash = Md5::new();
        data.inspect(|bytes| {
            content_hash.update(match bytes {
                Ok(d) => d.as_ref(),
                Err(_) => &[],
            })
        })
        .chunks(BLOCK_SIZE)
        .zip(stream::repeat((tx, block_map, path_map)))
        .for_each_concurrent(
            5,
            |(byte_chunk, (mut tx, block_map, path_map))| async move {
                for res in &byte_chunk {
                    if let Err(e) = res {
                        if let Err(e) = tx
                            .send(Err(std::io::Error::new(e.kind(), e.to_string())))
                            .await
                        {
                            eprintln!("Could not convey result: {}", e);
                        }
                        return;
                    }
                }
                // unwrap is safe as we checked that there is no error above
                let bytes: Vec<u8> = byte_chunk.into_iter().flat_map(|r| r.unwrap()).collect();
                let mut hasher = Md5::new();
                hasher.update(&bytes);
                let block_hash: BlockID = hasher.finalize().into();
                let data_len = bytes.len();

                // Check if the hash is present in the block map. If it is not, try to find a path, and
                // insert it.
                let should_write: Result<bool, sled::transaction::TransactionError> =
                    (&block_map, &path_map).transaction(|(blocks, paths)| {
                        let bid = blocks.get(block_hash)?;
                        if bid.is_some() {
                            // block already exists, since we did not write we can return here
                            // without error
                            return Ok(false);
                        }

                        // find a free path
                        for index in 0..BLOCKID_SIZE {
                            if paths.get(&block_hash[..index])?.is_some() {
                                // path already used, try the next one
                                continue;
                            };

                            // path is free, insert
                            paths.insert(&block_hash[..index], &block_hash)?;

                            let block = Block {
                                size: data_len,
                                path: block_hash[..index].to_vec(),
                            };

                            // unwrap here is fine since that would be a coding error
                            let block_meta = serde_json::to_vec(&block).unwrap();
                            blocks.insert(&block_hash, block_meta)?;
                            return Ok(true);
                        }

                        // The loop above can only NOT find a path in case it is duplicate
                        // block, wich already breaks out at the start.
                        unreachable!();
                    });

                match should_write {
                    Err(sled::transaction::TransactionError::Storage(e)) => {
                        if let Err(e) = tx.send(Err(e.into())).await {
                            eprintln!("Could not send transaction error: {}", e);
                        }
                        return;
                    }
                    Ok(false) => {
                        if let Err(e) = tx.send(Ok(block_hash)).await {
                            eprintln!("Could not send block id: {}", e);
                        }
                        return;
                    }
                    Ok(true) => {}
                    _ => unreachable!(),
                };

                // write the actual block
                // first load the block again from the DB
                let block: Block = match block_map.get(block_hash) {
                    // unwrap here is fine as that would be a coding error
                    Ok(Some(encoded_block)) => serde_json::from_slice(&encoded_block).unwrap(),
                    // we just inserted this block, so this is by definition impossible
                    Ok(None) => unreachable!(),
                    Err(e) => {
                        if let Err(e) = tx.send(Err(e.into())).await {
                            eprintln!("Could not send db error: {}", e);
                        }
                        return;
                    }
                };

                let block_path = block.disk_path(self.root.clone());
                if let Err(e) = async_fs::create_dir_all(block_path.parent().unwrap()).await {
                    if let Err(e) = tx.send(Err(e)).await {
                        eprintln!("Could not send path create error: {}", e);
                    }
                }
                if let Err(e) = async_fs::write(block_path, bytes).await {
                    if let Err(e) = tx.send(Err(e)).await {
                        eprintln!("Could not send block write error: {}", e);
                    }
                }

                if let Err(e) = tx.send(Ok(block_hash)).await {
                    eprintln!("Could not send block id: {}", e);
                }
            },
        )
        .await;

        Ok((
            rx.try_collect::<Vec<BlockID>>().await?,
            content_hash.finalize().into(),
        ))
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

#[derive(Debug)]
struct Object {
    size: u64,
    atime: u64,
    ctime: u64,
    mtime: u64,
    md5: BlockID,
    key: String, // TODO: is this even needed?
    blocks: Vec<BlockID>,
}

impl From<Object> for Vec<u8> {
    fn from(o: Object) -> Self {
        let mut data = Vec::with_capacity(
            32 + BLOCKID_SIZE + 8 + o.key.len() + 8 + o.blocks.len() * BLOCKID_SIZE,
        );

        data.extend_from_slice(&o.size.to_le_bytes());
        data.extend_from_slice(&o.atime.to_le_bytes());
        data.extend_from_slice(&o.ctime.to_le_bytes());
        data.extend_from_slice(&o.mtime.to_le_bytes());
        data.extend_from_slice(&o.md5);
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
        if value.len() < 32 + BLOCKID_SIZE + PTR_SIZE {
            return Err(FsError::MalformedObject);
        }
        // unwrap is safe since we checked the lenght of the object
        let key_len = usize::from_le_bytes(
            value[32 + BLOCKID_SIZE..32 + BLOCKID_SIZE + PTR_SIZE]
                .try_into()
                .unwrap(),
        );
        // already check the block length is included here
        if value.len() < 32 + BLOCKID_SIZE + PTR_SIZE + key_len + PTR_SIZE {
            return Err(FsError::MalformedObject);
        }
        // SAFETY: This is safe as we only fill in valid strings when storing, and we checked the
        // length of the data
        let key = unsafe {
            String::from_utf8_unchecked(
                value[32 + PTR_SIZE + BLOCKID_SIZE..32 + PTR_SIZE + BLOCKID_SIZE + key_len]
                    .to_vec(),
            )
        };

        let block_len = usize::from_le_bytes(
            value[32 + PTR_SIZE + BLOCKID_SIZE + key_len
                ..32 + PTR_SIZE + BLOCKID_SIZE + key_len + PTR_SIZE]
                .try_into()
                .unwrap(),
        );

        if value.len() != 32 + 2 * PTR_SIZE + BLOCKID_SIZE + key_len + block_len * BLOCKID_SIZE {
            return Err(FsError::MalformedObject);
        }

        let mut blocks = Vec::with_capacity(block_len);

        for chunk in value[32 + 2 * PTR_SIZE + BLOCKID_SIZE + key_len..].chunks_exact(BLOCKID_SIZE)
        {
            blocks.push(chunk.try_into().unwrap());
        }

        Ok(Object {
            size: u64::from_le_bytes(value[0..8].try_into().unwrap()),
            atime: u64::from_le_bytes(value[8..16].try_into().unwrap()),
            ctime: u64::from_le_bytes(value[16..24].try_into().unwrap()),
            mtime: u64::from_le_bytes(value[24..32].try_into().unwrap()),
            md5: value[32..32 + BLOCKID_SIZE].try_into().unwrap(),
            key,
            blocks,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Block {
    size: usize,
    path: Vec<u8>,
}

impl Block {
    fn disk_path(&self, mut root: PathBuf) -> PathBuf {
        // path has at least len 1
        let dirs = &self.path[..self.path.len() - 1];
        for byte in dirs {
            root.push(hex_string(&[*byte]));
        }
        root.push(format!(
            "_{}",
            hex_string(&[self.path[self.path.len() - 1]])
        ));
        root
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
        let UploadPartRequest {
            body,
            bucket,
            content_length,
            content_md5,
            key,
            part_number,
            upload_id,
            ..
        } = input;

        let body = body.ok_or_else(|| {
            code_error!(IncompleteBody, "You did not provide the number of bytes specified by the Content-Length HTTP header.")
        })?;
        todo!()
    }
}
