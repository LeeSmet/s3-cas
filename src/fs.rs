use async_trait::async_trait;
use chrono::prelude::*;
use faster_hex::hex_string;
use futures::ready;
use futures::{
    channel::mpsc::unbounded,
    sink::SinkExt,
    stream,
    stream::{StreamExt, TryStreamExt},
    AsyncRead, Future, Stream,
};
use hyper::body::Bytes;
use md5::{Digest, Md5};
use s3_server::dto::{
    CopyObjectOutput, CopyObjectRequest, CopyObjectResult, DeleteBucketOutput, DeleteBucketRequest,
    DeleteObjectOutput, DeleteObjectRequest, DeleteObjectsOutput, DeleteObjectsRequest,
    DeletedObject, PutObjectOutput, PutObjectRequest,
};
use s3_server::headers::AmzCopySource;
use s3_server::{
    dto::{
        Bucket, ByteStream, CompleteMultipartUploadError, CompleteMultipartUploadOutput,
        CompleteMultipartUploadRequest, CreateBucketOutput, CreateBucketRequest,
        CreateMultipartUploadOutput, CreateMultipartUploadRequest, GetBucketLocationOutput,
        GetBucketLocationRequest, GetObjectOutput, GetObjectRequest, HeadBucketOutput,
        HeadBucketRequest, HeadObjectOutput, HeadObjectRequest, ListBucketsOutput,
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
    pin::Pin,
    task::{Context, Poll},
};
use uuid::Uuid;

const BLOCK_SIZE: usize = 1 << 20; // Supposedly 1 MiB
const BUCKET_META_TREE: &str = "_BUCKETS";
const BLOCK_TREE: &str = "_BLOCKS";
const PATH_TREE: &str = "_PATHS";
const MULTIPART_TREE: &str = "_MULTIPART_PARTS";
const BLOCKID_SIZE: usize = 16;
const PTR_SIZE: usize = mem::size_of::<usize>(); // Size of a `usize` in bytes
const MAX_KEYS: i64 = 1000;

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

    /// Open the tree containing the bucket metadata.
    fn bucket_meta_tree(&self) -> Result<sled::Tree, sled::Error> {
        self.db.open_tree(BUCKET_META_TREE)
    }

    /// Remove a bucket and its associated metadata.
    fn bucket_delete(&self, bucket: &str) -> Result<(), sled::Error> {
        let bmt = self.bucket_meta_tree()?;
        bmt.remove(bucket)?;
        self.db.drop_tree(bucket)?;
        Ok(())
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
    /// returned, along with the hash of the full byte stream, and the length of the stream.
    async fn store_bytes(&self, data: ByteStream) -> io::Result<(Vec<BlockID>, BlockID, u64)> {
        let block_map = self.block_tree()?;
        let path_map = self.path_tree()?;
        let (tx, rx) = unbounded();
        let mut content_hash = Md5::new();
        let data = BufferedByteStream::new(data);
        let mut size = 0;
        data.map(|res| match res {
            Ok(buffers) => buffers.into_iter().map(|buffer| Ok(buffer)).collect(),
            Err(e) => vec![Err(e)],
        })
        .map(|i| stream::iter(i))
        .flatten()
        .inspect(|maybe_bytes| {
            if let Ok(bytes) = maybe_bytes {
                content_hash.update(bytes);
                size += bytes.len() as u64;
            }
        })
        .zip(stream::repeat((tx, block_map, path_map)))
        .for_each_concurrent(
            5,
            |(maybe_chunk, (mut tx, block_map, path_map))| async move {
                if let Err(e) = maybe_chunk {
                    if let Err(e) = tx
                        .send(Err(std::io::Error::new(e.kind(), e.to_string())))
                        .await
                    {
                        eprintln!("Could not convey result: {}", e);
                    }
                    return;
                }
                // unwrap is safe as we checked that there is no error above
                let bytes: Vec<u8> = maybe_chunk.unwrap();
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
            size,
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

impl From<&Object> for Vec<u8> {
    fn from(o: &Object) -> Self {
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
        for block in &o.blocks {
            data.extend_from_slice(block);
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

/// Implementation of a single stream over potentially multiple on disk data block files.
struct BlockStream {
    paths: Vec<PathBuf>,
    fp: usize,                    // pointer to current file path
    file: Option<async_fs::File>, // current file to read
    size: usize,
    open_fut: Option<Pin<Box<dyn Future<Output = io::Result<async_fs::File>> + Send>>>,
}

impl BlockStream {
    fn new(paths: Vec<PathBuf>, size: usize) -> Self {
        Self {
            paths,
            fp: 0,
            file: None,
            size,
            open_fut: None,
        }
    }
}

impl Stream for BlockStream {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // if we have an open file, try to read it
        if let Some(ref mut file) = self.file {
            let mut buf = vec![0; 4096];
            return match Pin::new(file).poll_read(cx, &mut buf) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                Poll::Ready(Ok(0)) => {
                    self.file = None;
                    self.poll_next(cx)
                }
                Poll::Ready(Ok(n)) => {
                    buf.truncate(n);
                    Poll::Ready(Some(Ok(buf.into())))
                }
            };
        }

        // we don't have an open file, check if we have any more left
        if self.fp > self.paths.len() {
            return Poll::Ready(None);
        }

        // try to open the next file
        // if we are not opening one already start doing so
        if self.open_fut.is_none() {
            self.open_fut = Some(Box::pin(async_fs::File::open(self.paths[self.fp].clone())));
            // increment the file pointer for the next file
            self.fp += 1;
        };

        // this will always happen
        if let Some(ref mut open_fut) = self.open_fut {
            let file_res = ready!(open_fut.as_mut().poll(cx));
            // we opened a file, or there is an error
            // clear the open fut as it is done
            self.open_fut = None;
            match file_res {
                // if there is an error, we just return that. The next poll call will try to open the
                // next file
                Err(e) => return Poll::Ready(Some(Err(e))),
                // if we do have an open file, set it as open file, and immediately poll again to try
                // and read from it
                Ok(file) => {
                    self.file = Some(file);
                    return self.poll_next(cx);
                }
            };
        };

        unreachable!();
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size, Some(self.size))
    }
}

struct BufferedByteStream {
    // In a perfect world this would be an AsyncRead type, as that will likely be more performant
    // than reading bytes, and copying them. However the AyndRead implemented on this type is the
    // tokio one, which is not the same as the futures one. And I don't feel like adding a tokio
    // dependency here right now for that.
    // TODO: benchmark both approaches
    bs: ByteStream,
    buffer: Vec<u8>,
    finished: bool,
}

impl BufferedByteStream {
    fn new(bs: ByteStream) -> Self {
        Self {
            bs,
            buffer: Vec::with_capacity(BLOCK_SIZE),
            finished: false,
        }
    }
}

impl Stream for BufferedByteStream {
    type Item = io::Result<Vec<Vec<u8>>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        loop {
            match ready!(Pin::new(&mut self.bs).poll_next(cx)) {
                None => {
                    self.finished = true;
                    if self.buffer.len() > 0 {
                        // since we won't be using the vec anymore, we can replace it with a 0 capacity
                        // vec. This wont' allocate.
                        return Poll::Ready(Some(Ok(vec![mem::replace(
                            &mut self.buffer,
                            Vec::with_capacity(0),
                        )])));
                    }
                    return Poll::Ready(None);
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                Some(Ok(bytes)) => {
                    let mut buf_remainder = self.buffer.capacity() - self.buffer.len();
                    if bytes.len() < buf_remainder {
                        self.buffer.extend_from_slice(&bytes);
                    } else if self.buffer.len() == buf_remainder {
                        self.buffer.extend_from_slice(&bytes);
                        return Poll::Ready(Some(Ok(vec![mem::replace(
                            &mut self.buffer,
                            Vec::with_capacity(BLOCK_SIZE),
                        )])));
                    } else {
                        let mut out = Vec::with_capacity(
                            (bytes.len() - buf_remainder) / self.buffer.capacity() + 1,
                        );
                        self.buffer.extend_from_slice(&bytes[..buf_remainder]);
                        out.push(mem::replace(
                            &mut self.buffer,
                            Vec::with_capacity(BLOCK_SIZE),
                        ));
                        // repurpose buf_remainder as pointer to start of data
                        while bytes[buf_remainder..].len() > BLOCK_SIZE {
                            out.push(Vec::from(&bytes[buf_remainder..buf_remainder + BLOCK_SIZE]));
                            buf_remainder += BLOCK_SIZE;
                        }
                        // place the remainder in our buf
                        self.buffer.extend_from_slice(&bytes[buf_remainder..]);
                        return Poll::Ready(Some(Ok(out)));
                    };
                }
            };
        }
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
            let part_data_enc = trace_try!(multipart_map.get(&part_key));
            let part_data_enc = match part_data_enc {
                Some(pde) => pde,
                None => {
                    eprintln!("Missing part \"{}\" in multipart upload", part_key);
                    return Err(code_error!(InvalidArgument, "Part not uploaded").into());
                }
            };
            //let part_data_enc = trace_try!(part_data_enc
            //    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Part not uploaded")));

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

        trace_try!(bc.insert(&key, Vec::<u8>::from(&object)));

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
        input: CopyObjectRequest,
    ) -> S3StorageResult<CopyObjectOutput, s3_server::dto::CopyObjectError> {
        let copy_source = AmzCopySource::from_header_str(&input.copy_source)
            .map_err(|err| invalid_request!("Invalid header: x-amz-copy-source", err))?;

        let (bucket, key) = match copy_source {
            AmzCopySource::AccessPoint { .. } => {
                return Err(not_supported!("Access point is not supported yet.").into())
            }
            AmzCopySource::Bucket { bucket, key } => (bucket, key),
        };

        if !trace_try!(self.bucket_exists(&bucket)) {
            return Err(code_error!(NoSuchBucket, "Target bucket does not exist").into());
        }

        let source_bk = trace_try!(self.bucket(&input.bucket));
        let mut obj_meta = match trace_try!(source_bk.get(&input.key)) {
            // unwrap here is safe as it means the DB is corrupted
            Some(enc_meta) => Object::try_from(&*enc_meta).unwrap(),
            None => return Err(code_error!(NoSuchKey, "Source key does not exist").into()),
        };

        let now = Utc::now().timestamp();
        obj_meta.mtime = now;
        obj_meta.atime = now;
        obj_meta.ctime = now;
        obj_meta.key = key.to_string();

        // TODO: check duplicate?
        let dst_bk = trace_try!(self.bucket(&bucket));
        trace_try!(dst_bk.insert(key, Vec::<u8>::from(&obj_meta)));

        Ok(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(format!("\"{}\"", hex_string(&obj_meta.e_tag))),
                last_modified: Some(Utc.timestamp(now, 0).to_rfc3339()),
            }),
            ..CopyObjectOutput::default()
        })
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

        if trace_try!(self.bucket_exists(&bucket)) {
            return Err(code_error!(
                BucketAlreadyExists,
                "A bucket with this name already exists"
            )
            .into());
        }
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
        input: DeleteBucketRequest,
    ) -> S3StorageResult<DeleteBucketOutput, s3_server::dto::DeleteBucketError> {
        let DeleteBucketRequest { bucket, .. } = input;

        trace_try!(self.bucket_delete(&bucket));

        Ok(DeleteBucketOutput)
    }

    async fn delete_object(
        &self,
        input: DeleteObjectRequest,
    ) -> S3StorageResult<DeleteObjectOutput, s3_server::dto::DeleteObjectError> {
        let DeleteObjectRequest { bucket, key, .. } = input;

        if !trace_try!(self.bucket_exists(&bucket)) {
            return Err(code_error!(NoSuchBucket, "Bucket does not exist").into());
        }

        trace_try!(trace_try!(self.bucket(&bucket)).remove(&key));

        Ok(DeleteObjectOutput::default())
    }

    async fn delete_objects(
        &self,
        input: DeleteObjectsRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::DeleteObjectsOutput,
        s3_server::dto::DeleteObjectsError,
    > {
        if !trace_try!(self.bucket_exists(&input.bucket)) {
            return Err(code_error!(NoSuchBucket, "Bucket does not exist").into());
        }
        let bucket = trace_try!(self.bucket(&input.bucket));

        let mut deleted = Vec::with_capacity(input.delete.objects.len());
        let mut errors = Vec::new();

        for object in input.delete.objects {
            match bucket.remove(&object.key) {
                Ok(_) => {
                    deleted.push(DeletedObject {
                        key: Some(object.key),
                        ..DeletedObject::default()
                    });
                }
                Err(e) => {
                    eprintln!(
                        "Could not remove key {} from bucket {}",
                        &object.key, &input.bucket
                    );
                    // TODO
                    // errors.push(code_error!(InternalError, "Could not delete key"));
                }
            };
        }

        Ok(DeleteObjectsOutput {
            deleted: Some(deleted),
            errors: if errors.len() == 0 {
                None
            } else {
                Some(errors)
            },
            ..DeleteObjectsOutput::default()
        })
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
        input: GetObjectRequest,
    ) -> S3StorageResult<GetObjectOutput, s3_server::dto::GetObjectError> {
        let GetObjectRequest { bucket, key, .. } = input;

        let bk = trace_try!(self.bucket(&bucket));
        let obj = match trace_try!(bk.get(&key)) {
            None => return Err(code_error!(NoSuchKey, "The specified key does not exist").into()),
            Some(obj) => obj,
        };
        let obj_meta = trace_try!(Object::try_from(&obj.to_vec()[..]));

        let block_map = trace_try!(self.block_tree());
        let mut paths = Vec::with_capacity(obj_meta.blocks.len());
        let mut block_size = 0;
        for block in obj_meta.blocks {
            // unwrap here is safe as we only add blocks to the list of an object if they are
            // corectly inserted in the block map
            let block_meta_enc = trace_try!(block_map.get(block)).unwrap();
            let block_meta = trace_try!(serde_json::from_slice::<Block>(&block_meta_enc));
            block_size += block_meta.size;
            paths.push(block_meta.disk_path(self.root.clone()));
        }
        debug_assert!(obj_meta.size as usize == block_size);
        let block_stream = BlockStream::new(paths, block_size);

        let stream = ByteStream::new_with_size(block_stream, obj_meta.size as usize);
        Ok(GetObjectOutput {
            body: Some(stream),
            content_length: Some(obj_meta.size as i64),
            last_modified: Some(Utc.timestamp(obj_meta.mtime, 0).to_rfc3339()),
            e_tag: Some(format!("\"{}\"", hex_string(&obj_meta.e_tag))),
            ..GetObjectOutput::default()
        })
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
        input: HeadObjectRequest,
    ) -> S3StorageResult<HeadObjectOutput, s3_server::dto::HeadObjectError> {
        let HeadObjectRequest { bucket, key, .. } = input;
        let bk = trace_try!(self.bucket(&bucket));
        let obj = match trace_try!(bk.get(&key)) {
            None => return Err(code_error!(NoSuchKey, "The specified key does not exist").into()),
            Some(obj) => obj,
        };
        let obj_meta = trace_try!(Object::try_from(&obj.to_vec()[..]));

        Ok(HeadObjectOutput {
            content_length: Some(obj_meta.size as i64),
            last_modified: Some(Utc.timestamp(obj_meta.mtime, 0).to_rfc3339()),
            e_tag: Some(format!("\"{}\"", hex_string(&obj_meta.e_tag))),
            ..HeadObjectOutput::default()
        })
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
            marker,
            max_keys,
            ..
        } = input;

        let key_count = max_keys
            .and_then(|mk| {
                if mk > MAX_KEYS {
                    Some(MAX_KEYS)
                } else {
                    Some(mk)
                }
            })
            .unwrap_or(MAX_KEYS);

        let b = trace_try!(self.bucket(&bucket));

        let mut objects = b
            .scan_prefix(&prefix.as_deref().or(Some("")).unwrap())
            .filter_map(|read_result| match read_result {
                Ok((r, k)) => Some((r, k)),
                Err(_) => None,
            })
            .skip_while(|(raw_key, _)| match marker {
                None => false,
                Some(ref marker) => raw_key <= &sled::IVec::from(marker.as_bytes()),
            })
            .map(|(raw_key, raw_value)| {
                // SAFETY: we only insert valid utf8 strings
                let key = unsafe { String::from_utf8_unchecked(raw_key.to_vec()) };
                // unwrap is fine as it would mean either a coding error or a corrupt DB
                let obj = Object::try_from(&*raw_value).unwrap();

                S3Object {
                    key: Some(key),
                    e_tag: Some(format!("\"{}\"", hex_string(&obj.e_tag))),
                    last_modified: Some(Utc.timestamp(obj.mtime, 0).to_rfc3339()),
                    owner: None,
                    size: Some(obj.size as i64),
                    storage_class: None,
                }
            })
            .take((key_count + 1) as usize)
            .collect::<Vec<_>>();

        let mut next_marker = None;
        let truncated = objects.len() == key_count as usize + 1;
        if truncated {
            next_marker = Some(objects.pop().unwrap().key.unwrap())
        }

        Ok(ListObjectsOutput {
            contents: Some(objects),
            delimiter,
            encoding_type,
            name: Some(bucket),
            common_prefixes: None,
            is_truncated: Some(truncated),
            marker,
            max_keys: Some(key_count),
            next_marker,
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
        input: PutObjectRequest,
    ) -> S3StorageResult<PutObjectOutput, s3_server::dto::PutObjectError> {
        if let Some(ref storage_class) = input.storage_class {
            let is_valid = ["STANDARD", "REDUCED_REDUNDANCY"].contains(&storage_class.as_str());
            if !is_valid {
                let err = code_error!(
                    InvalidStorageClass,
                    "The storage class you specified is not valid."
                );
                return Err(err.into());
            }
        }

        let PutObjectRequest {
            body, bucket, key, ..
        } = input;

        let body: ByteStream = body.ok_or_else(|| {
            code_error!(IncompleteBody, "You did not provide the number of bytes specified by the Content-Length HTTP header.")
        })?;

        if !trace_try!(self.bucket_exists(&bucket)) {
            return Err(code_error!(NoSuchBucket, "Bucket does not exist").into());
        }

        let (blocks, hash, size) = trace_try!(self.store_bytes(body).await);
        let e_tag = format!("\"{}\"", hex_string(&hash));
        let now = Utc::now().timestamp();
        let obj_meta = Object {
            size,
            atime: now,
            mtime: now,
            ctime: now,
            e_tag: hash,
            blocks,
            key: key.clone(),
        };

        trace_try!(trace_try!(self.bucket(&bucket)).insert(&key, Vec::<u8>::from(&obj_meta)));

        Ok(PutObjectOutput {
            e_tag: Some(e_tag),
            ..PutObjectOutput::default()
        })
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

        let (blocks, hash, size) = trace_try!(self.store_bytes(body).await);

        if size != content_length as u64 {
            return Err(code_error!(
                InvalidRequest,
                "You did not send the amount of bytes specified by the Content-Length HTTP header."
            )
            .into());
        }

        let mp_map = trace_try!(self.multipart_tree());

        let e_tag = format!("\"{}\"", hex_string(&hash));
        let storage_key = format!("{}-{}-{}-{}", &bucket, &key, &upload_id, part_number);
        let mp = MultiPart {
            bucket,
            key,
            upload_id,
            part_number,
            blocks,
            hash,
            size: size as usize,
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
