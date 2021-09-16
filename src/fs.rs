use async_trait::async_trait;
use chrono::prelude::*;
use faster_hex::hex_string;
use futures::{
    channel::mpsc::unbounded,
    sink::SinkExt,
    stream,
    stream::{StreamExt, TryStreamExt},
    AsyncRead,
};
use md5::{Digest, Md5};
use s3_server::{
    dto::{
        Bucket, ByteStream, CompleteMultipartUploadError, CompleteMultipartUploadOutput,
        CompleteMultipartUploadRequest, CreateBucketOutput, CreateBucketRequest,
        CreateMultipartUploadOutput, CreateMultipartUploadRequest, GetBucketLocationOutput,
        GetBucketLocationRequest, HeadBucketOutput, HeadBucketRequest, ListBucketsOutput,
        ListBucketsRequest, ListObjectsOutput, ListObjectsRequest, ListObjectsV2Output,
        ListObjectsV2Request, Object as S3Object,
    },
    errors::S3StorageResult,
    path::S3Path,
    S3Storage,
};
use s3_server::{
    dto::{UploadPartOutput, UploadPartRequest},
    errors::S3StorageError,
};
use serde::{Deserialize, Serialize};
use sled::{Db, IVec, Transactional};
use std::{
    convert::{TryFrom, TryInto},
    fmt::{self, Display, Formatter},
    io, mem,
    path::PathBuf,
};
use uuid::Uuid;

const BLOCK_SIZE: usize = 1 << 20; // Supposedly 1 MiB
const BUCKET_META_TREE: &str = "_BUCKETS";
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
    pub fn new(mut root: PathBuf) -> Self {
        let mut db_path = root.clone();
        db_path.push("db");
        root.push("blocks");
        Self {
            db: sled::open(db_path).unwrap(),
            root,
        }
    }

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
    fn bucket_exists(&self, bucket_name: &str) -> Result<bool, sled::Error> {
        self.bucket_meta_tree()?.contains_key(bucket_name)
    }

    /// Open the tree containing the objects in a bucket.
    fn bucket(&self, bucket_name: &str) -> Result<sled::Tree, sled::Error> {
        self.db.open_tree(bucket_name)
    }

    /// Open the tree containing the bucket metadata
    fn bucket_meta_tree(&self) -> Result<sled::Tree, sled::Error> {
        self.db.open_tree(BUCKET_META_TREE)
    }

    /// Get a list of all buckets in the system.
    fn buckets(&self) -> Result<Vec<Bucket>, sled::Error> {
        Ok(self
            .bucket_meta_tree()?
            .scan_prefix(&[])
            .values()
            .filter_map(|raw_value| {
                let value = match raw_value {
                    Err(_) => return None,
                    Ok(v) => v,
                };
                // unwrap here is fine as it means the db is corrupt
                let bucket_meta = serde_json::from_slice::<BucketMeta>(&value).unwrap();
                Some(Bucket {
                    name: Some(bucket_meta.name),
                    creation_date: Some(Utc.timestamp(bucket_meta.ctime, 0).to_rfc3339()),
                })
            })
            .collect())
    }

    /// Save data on the filesystem. A list of block ID's used as keys for the data blocks is
    /// returned, along with the hash of the full byte stream.
    async fn store_bytes(&self, data: ByteStream) -> io::Result<(Vec<BlockID>, BlockID)> {
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
        // TODO: fix this to pull bytes, not results, and to write as soon as possible
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
                        for index in 1..BLOCKID_SIZE {
                            println!("Checking path {}", hex_string(&block_hash[..index]));
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
                    // We don't abort manually so this can't happen
                    Err(sled::transaction::TransactionError::Abort(_)) => unreachable!(),
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
    atime: i64,
    ctime: i64,
    mtime: i64,
    e_tag: BlockID,
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
        data.extend_from_slice(&o.e_tag);
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
            atime: i64::from_le_bytes(value[8..16].try_into().unwrap()),
            ctime: i64::from_le_bytes(value[16..24].try_into().unwrap()),
            mtime: i64::from_le_bytes(value[24..32].try_into().unwrap()),
            e_tag: value[32..32 + BLOCKID_SIZE].try_into().unwrap(),
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

#[derive(Debug, Serialize, Deserialize)]
struct MultiPart {
    size: usize,
    part_number: i64,
    bucket: String,
    key: String,
    upload_id: String,
    hash: BlockID,
    blocks: Vec<BlockID>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BucketMeta {
    name: String,
    ctime: i64,
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
        input: CompleteMultipartUploadRequest,
    ) -> S3StorageResult<CompleteMultipartUploadOutput, s3_server::dto::CompleteMultipartUploadError>
    {
        let CompleteMultipartUploadRequest {
            multipart_upload,
            bucket,
            key,
            upload_id,
            ..
        } = input;

        let multipart_upload = if let Some(multipart_upload) = multipart_upload {
            multipart_upload
        } else {
            let err = code_error!(InvalidPart, "Missing multipart_upload");
            return Err(err.into());
        };

        let multipart_map = trace_try!(self.multipart_tree());

        let mut blocks = vec![];
        let mut cnt: i64 = 0;
        for part in multipart_upload.parts.iter().flatten() {
            let part_number = trace_try!(part
                .part_number
                .ok_or_else(|| { io::Error::new(io::ErrorKind::NotFound, "Missing part_number") }));
            cnt = cnt.wrapping_add(1);
            if part_number != cnt {
                trace_try!(Err(io::Error::new(
                    io::ErrorKind::Other,
                    "InvalidPartOrder"
                )));
            }
            let part_key = format!("{}-{}-{}-{}", &bucket, &key, &upload_id, part_number);
            let part_data_enc = trace_try!(multipart_map.get(part_key));
            let part_data_enc = trace_try!(part_data_enc
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Part not uploaded")));

            // unwrap here is safe as it is a coding error
            let mp: MultiPart = serde_json::from_slice(&part_data_enc).unwrap();

            blocks.extend_from_slice(&mp.blocks);
        }

        let mut hasher = Md5::new();
        let mut size = 0;
        let block_map = trace_try!(self.block_tree());
        for block in &blocks {
            let bi = trace_try!(block_map.get(&block)).unwrap(); // unwrap is fine as all blocks in must be present
            let block_info: Block = serde_json::from_slice(&bi).unwrap(); // unwrap is fine as this would mean the DB is corrupted
            let bytes = trace_try!(async_fs::read(block_info.disk_path(self.root.clone())).await);
            size += bytes.len();
            hasher.update(&bytes);
        }
        let e_tag = hasher.finalize().into();

        let bc = trace_try!(self.bucket(&bucket));

        let now = Utc::now().timestamp();
        let object = Object {
            size: size as u64,
            atime: now,
            mtime: now,
            ctime: now,
            key: key.clone(),
            blocks,
            e_tag,
        };

        trace_try!(bc.insert(&key, Vec::<u8>::from(object)));

        // Try to delete the multipart metadata. If this fails, it is not really an issue.
        for part in multipart_upload.parts.into_iter().flatten() {
            let part_key = format!(
                "{}-{}-{}-{}",
                &bucket,
                &key,
                &upload_id,
                part.part_number.unwrap()
            );

            if let Err(e) = multipart_map.remove(part_key) {
                eprintln!("Could not remove part: {}", e);
            };
        }

        Ok(CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            e_tag: Some(format!("\"{}\"", hex_string(&e_tag))),
            ..CompleteMultipartUploadOutput::default()
        })
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
        input: CreateMultipartUploadRequest,
    ) -> S3StorageResult<CreateMultipartUploadOutput, s3_server::dto::CreateMultipartUploadError>
    {
        let CreateMultipartUploadRequest { bucket, key, .. } = input;

        let upload_id = Uuid::new_v4().to_string();

        Ok(CreateMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id),
            ..CreateMultipartUploadOutput::default()
        })
    }

    async fn create_bucket(
        &self,
        input: CreateBucketRequest,
    ) -> S3StorageResult<CreateBucketOutput, s3_server::dto::CreateBucketError> {
        let CreateBucketRequest { bucket, .. } = input;

        // TODO: check duplicate
        let bucket_meta = trace_try!(self.bucket_meta_tree());

        let bm = serde_json::to_vec(&BucketMeta {
            name: bucket.clone(),
            ctime: Utc::now().timestamp(),
        })
        .unwrap();

        trace_try!(bucket_meta.insert(&bucket, bm));

        Ok(CreateBucketOutput {
            ..CreateBucketOutput::default()
        })
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
        input: GetBucketLocationRequest,
    ) -> S3StorageResult<GetBucketLocationOutput, s3_server::dto::GetBucketLocationError> {
        let GetBucketLocationRequest { bucket, .. } = input;

        let exists = trace_try!(self.bucket_exists(&bucket));

        if !exists {
            return Err(code_error!(NoSuchBucket, "NotFound").into());
        }

        Ok(GetBucketLocationOutput {
            location_constraint: None,
        })
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
        input: HeadBucketRequest,
    ) -> S3StorageResult<HeadBucketOutput, s3_server::dto::HeadBucketError> {
        let HeadBucketRequest { bucket, .. } = input;

        if !trace_try!(self.bucket_exists(&bucket)) {
            return Err(code_error!(NoSuchBucket, "The specified bucket does not exist").into());
        }

        Ok(HeadBucketOutput)
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
        _: ListBucketsRequest,
    ) -> S3StorageResult<ListBucketsOutput, s3_server::dto::ListBucketsError> {
        let buckets = trace_try!(self.buckets());

        Ok(ListBucketsOutput {
            buckets: Some(buckets),
            owner: None,
        })
    }

    async fn list_objects(
        &self,
        input: ListObjectsRequest,
    ) -> S3StorageResult<ListObjectsOutput, s3_server::dto::ListObjectsError> {
        let ListObjectsRequest {
            bucket,
            delimiter,
            prefix,
            encoding_type,
            ..
        } = input;

        let b = trace_try!(self.bucket(&bucket));

        let objects = b
            .scan_prefix(&prefix.as_ref().map(|v| v.as_str()).or(Some("")).unwrap())
            .filter_map(|read_result| {
                let (raw_key, raw_value) = match read_result {
                    Ok((r, k)) => (r, k),
                    Err(_) => return None,
                };

                // SAFETY: we only insert valid utf8 strings
                let key = unsafe { String::from_utf8_unchecked(raw_key.to_vec()) };
                // unwrap is fine as it would mean either a coding error or a corrupt DB
                let obj = Object::try_from(&*raw_value).unwrap();

                Some(S3Object {
                    key: Some(key),
                    e_tag: Some(format!("\"{}\"", hex_string(&obj.e_tag))),
                    last_modified: Some(Utc.timestamp(obj.mtime, 0).to_rfc3339()),
                    owner: None,
                    size: Some(obj.size as i64),
                    storage_class: None,
                })
            })
            .collect();

        Ok(ListObjectsOutput {
            contents: Some(objects),
            delimiter,
            encoding_type,
            name: Some(bucket),
            common_prefixes: None,
            is_truncated: None,
            marker: None,
            max_keys: None,
            next_marker: None,
            prefix,
        })
    }

    async fn list_objects_v2(
        &self,
        input: ListObjectsV2Request,
    ) -> S3StorageResult<ListObjectsV2Output, s3_server::dto::ListObjectsV2Error> {
        let ListObjectsV2Request {
            bucket,
            delimiter,
            prefix,
            encoding_type,
            ..
        } = input;

        let b = trace_try!(self.bucket(&bucket));

        let objects: Vec<_> = b
            .scan_prefix(&prefix.as_ref().map(|v| v.as_str()).or(Some("")).unwrap())
            .filter_map(|read_result| {
                let (raw_key, raw_value) = match read_result {
                    Ok((r, k)) => (r, k),
                    Err(_) => return None,
                };

                // SAFETY: we only insert valid utf8 strings
                let key = unsafe { String::from_utf8_unchecked(raw_key.to_vec()) };
                // unwrap is fine as it would mean either a coding error or a corrupt DB
                let obj = Object::try_from(&*raw_value).unwrap();

                Some(S3Object {
                    key: Some(key),
                    e_tag: Some(format!("\"{}\"", hex_string(&obj.e_tag))),
                    last_modified: Some(Utc.timestamp(obj.mtime, 0).to_rfc3339()),
                    owner: None,
                    size: Some(obj.size as i64),
                    storage_class: None,
                })
            })
            .collect();

        Ok(ListObjectsV2Output {
            key_count: Some(objects.len() as i64),
            contents: Some(objects),
            common_prefixes: None,
            delimiter,
            continuation_token: None,
            encoding_type,
            is_truncated: None,
            prefix,
            name: Some(bucket),
            max_keys: None,
            start_after: None,
            next_continuation_token: None,
        })
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
        input: UploadPartRequest,
    ) -> S3StorageResult<UploadPartOutput, s3_server::dto::UploadPartError> {
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

        let body: ByteStream = body.ok_or_else(|| {
            code_error!(IncompleteBody, "You did not provide the number of bytes specified by the Content-Length HTTP header.")
        })?;

        let content_length = content_length.ok_or_else(|| {
            code_error!(
                MissingContentLength,
                "You did not provide the number of bytes in the Content-Length HTTP header."
            )
        })?;

        let mp_map = trace_try!(self.multipart_tree());

        let (blocks, hash) = trace_try!(self.store_bytes(body).await);

        let e_tag = format!("\"{}\"", hex_string(&hash));
        let storage_key = format!("{}-{}-{}-{}", &bucket, &key, &upload_id, part_number);
        let mp = MultiPart {
            bucket,
            key,
            upload_id,
            part_number,
            blocks,
            hash,
            size: content_length as usize,
        };

        // unwrap here is safe as it would be a coding error
        let enc_mp = serde_json::to_vec(&mp).unwrap();

        trace_try!(mp_map.insert(storage_key, enc_mp));

        Ok(UploadPartOutput {
            e_tag: Some(e_tag),
            ..UploadPartOutput::default()
        })
    }
}
