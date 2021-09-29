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
                let bucket_meta = BucketMeta::try_from(&*value).expect("Corrupted bucket metadata");
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

                            blocks.insert(&block_hash, Vec::from(&block))?;
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
                    Ok(Some(encoded_block)) => (&*encoded_block)
                        .try_into()
                        .expect("Block data is corrupted"),
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
    ctime: i64,
    e_tag: BlockID,
    // The amount of parts uploaded for this object. In case of a simple put_object, this will be
    // 0. In case of a multipart upload, this wil equal the amount of individual parts. This is
    // required so we can properly construct the formatted hash later.
    parts: usize,
    blocks: Vec<BlockID>,
}

impl Object {
    fn format_e_tag(&self) -> String {
        if self.parts == 0 {
            format!("\"{}\"", hex_string(&self.e_tag))
        } else {
            format!("\"{}-{}\"", hex_string(&self.e_tag), self.parts)
        }
    }
}

impl From<&Object> for Vec<u8> {
    fn from(o: &Object) -> Self {
        let mut data =
            Vec::with_capacity(16 + BLOCKID_SIZE + PTR_SIZE * 2 + o.blocks.len() * BLOCKID_SIZE);

        data.extend_from_slice(&o.size.to_le_bytes());
        data.extend_from_slice(&o.ctime.to_le_bytes());
        data.extend_from_slice(&o.e_tag);
        data.extend_from_slice(&o.parts.to_le_bytes());
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
        if value.len() < 16 + BLOCKID_SIZE + 2 * PTR_SIZE {
            return Err(FsError::MalformedObject);
        }

        let block_len = usize::from_le_bytes(
            value[16 + BLOCKID_SIZE + PTR_SIZE..16 + BLOCKID_SIZE + 2 * PTR_SIZE]
                .try_into()
                .unwrap(),
        );

        if value.len() != 16 + 2 * PTR_SIZE + BLOCKID_SIZE + block_len * BLOCKID_SIZE {
            return Err(FsError::MalformedObject);
        }

        let mut blocks = Vec::with_capacity(block_len);

        for chunk in value[16 + 2 * PTR_SIZE + BLOCKID_SIZE..].chunks_exact(BLOCKID_SIZE) {
            blocks.push(chunk.try_into().unwrap());
        }

        Ok(Object {
            size: u64::from_le_bytes(value[0..8].try_into().unwrap()),
            ctime: i64::from_le_bytes(value[8..16].try_into().unwrap()),
            e_tag: value[16..16 + BLOCKID_SIZE].try_into().unwrap(),
            parts: usize::from_le_bytes(
                value[16 + BLOCKID_SIZE..16 + BLOCKID_SIZE + PTR_SIZE]
                    .try_into()
                    .unwrap(),
            ),
            blocks,
        })
    }
}

// TODO: this can be optimized by making path a `[u8;BLOCKID_SIZE]` and keeping track of a len u8
#[derive(Debug)]
struct Block {
    size: usize,
    path: Vec<u8>,
}

impl From<&Block> for Vec<u8> {
    fn from(b: &Block) -> Self {
        // NOTE: we encode the lenght of the vector as a single byte, since it can only be 16 bytes
        // long.
        let mut out = Vec::with_capacity(PTR_SIZE + b.path.len() + 1);
        out.extend_from_slice(&b.size.to_le_bytes());
        out.extend_from_slice(&(b.path.len() as u8).to_le_bytes());
        out.extend_from_slice(&b.path);
        out
    }
}

impl TryFrom<&[u8]> for Block {
    type Error = FsError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < PTR_SIZE + 1 {
            return Err(FsError::MalformedObject);
        }
        let size = usize::from_le_bytes(value[..PTR_SIZE].try_into().unwrap());
        let vec_size =
            u8::from_le_bytes(value[PTR_SIZE..PTR_SIZE + 1].try_into().unwrap()) as usize;
        if value.len() != PTR_SIZE + 1 + vec_size {
            return Err(FsError::MalformedObject);
        }

        Ok(Block {
            size,
            path: value[PTR_SIZE + 1..].to_vec(),
        })
    }
}

#[derive(Debug)]
struct MultiPart {
    size: usize,
    part_number: i64,
    bucket: String,
    key: String,
    upload_id: String,
    hash: BlockID,
    blocks: Vec<BlockID>,
}

impl From<&MultiPart> for Vec<u8> {
    fn from(mp: &MultiPart) -> Self {
        let mut out = Vec::with_capacity(
            5 * PTR_SIZE
                + 8
                + mp.bucket.len()
                + mp.key.len()
                + mp.upload_id.len()
                + (1 + mp.blocks.len()) * BLOCKID_SIZE,
        );

        out.extend_from_slice(&mp.size.to_le_bytes());
        out.extend_from_slice(&mp.part_number.to_le_bytes());
        out.extend_from_slice(&mp.bucket.len().to_le_bytes());
        out.extend_from_slice(&mp.bucket.as_bytes());
        out.extend_from_slice(&mp.key.len().to_le_bytes());
        out.extend_from_slice(&mp.key.as_bytes());
        out.extend_from_slice(&mp.upload_id.len().to_le_bytes());
        out.extend_from_slice(&mp.upload_id.as_bytes());
        out.extend_from_slice(&mp.hash);
        out.extend_from_slice(&mp.blocks.len().to_le_bytes());
        for block in &mp.blocks {
            out.extend_from_slice(block);
        }

        out
    }
}

impl TryFrom<&[u8]> for MultiPart {
    type Error = FsError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < 5 * PTR_SIZE + 8 + BLOCKID_SIZE {
            return Err(FsError::MalformedObject);
        }

        let bucket_len =
            usize::from_le_bytes(value[8 + PTR_SIZE..8 + 2 * PTR_SIZE].try_into().unwrap());
        if value.len() < 8 + 3 * PTR_SIZE + bucket_len {
            return Err(FsError::MalformedObject);
        }
        // SAFETY: Safe as we only insert valid strings
        let bucket = unsafe {
            String::from_utf8_unchecked(
                value[8 + 2 * PTR_SIZE..8 + 2 * PTR_SIZE + bucket_len].to_vec(),
            )
        };

        let key_len = usize::from_le_bytes(
            value[8 + 2 * PTR_SIZE + bucket_len..8 + 3 * PTR_SIZE + bucket_len]
                .try_into()
                .unwrap(),
        );
        if value.len() < 8 + 4 * PTR_SIZE + bucket_len + key_len {
            return Err(FsError::MalformedObject);
        }
        // SAFETY: Safe as we only insert valid strings
        let key = unsafe {
            String::from_utf8_unchecked(
                value[8 + 3 * PTR_SIZE + bucket_len..8 + 3 * PTR_SIZE + bucket_len + key_len]
                    .to_vec(),
            )
        };

        let upload_id_len = usize::from_le_bytes(
            value[8 + 3 * PTR_SIZE + bucket_len + key_len..8 + 4 * PTR_SIZE + bucket_len + key_len]
                .try_into()
                .unwrap(),
        );
        if value.len() < 8 + 5 * PTR_SIZE + bucket_len + key_len + upload_id_len + BLOCKID_SIZE {
            return Err(FsError::MalformedObject);
        }
        // SAFETY: Safe as we only insert valid strings
        let upload_id = unsafe {
            String::from_utf8_unchecked(
                value[8 + 4 * PTR_SIZE + bucket_len + key_len
                    ..8 + 4 * PTR_SIZE + bucket_len + key_len + upload_id_len]
                    .to_vec(),
            )
        };

        let block_len = usize::from_le_bytes(
            value[8 + 4 * PTR_SIZE + bucket_len + key_len + BLOCKID_SIZE
                ..8 + 5 * PTR_SIZE + bucket_len + key_len + BLOCKID_SIZE]
                .try_into()
                .unwrap(),
        );
        if value.len()
            < 8 + 5 * PTR_SIZE
                + bucket_len
                + key_len
                + upload_id_len
                + (1 + block_len) * BLOCKID_SIZE
        {
            return Err(FsError::MalformedObject);
        }
        let mut blocks = Vec::with_capacity(block_len);
        for chunk in value[8 + 5 * PTR_SIZE + bucket_len + key_len + upload_id_len + BLOCKID_SIZE..]
            .chunks_exact(BLOCKID_SIZE)
        {
            blocks.push(chunk.try_into().unwrap());
        }

        Ok(MultiPart {
            size: usize::from_le_bytes(value[..PTR_SIZE].try_into().unwrap()),
            part_number: i64::from_le_bytes(value[PTR_SIZE..8 + PTR_SIZE].try_into().unwrap()),
            bucket,
            key,
            upload_id,
            hash: value[8 + 4 * PTR_SIZE + bucket_len + key_len + upload_id_len
                ..8 + 4 * PTR_SIZE + bucket_len + key_len + upload_id_len + BLOCKID_SIZE]
                .try_into()
                .unwrap(),
            blocks,
        })
    }
}

#[derive(Debug)]
struct BucketMeta {
    ctime: i64,
    name: String,
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
            name: unsafe { String::from_utf8_unchecked(value[8..].to_vec()) },
        })
    }
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

            // unwrap here is safe as it is a coding error
            let mp = MultiPart::try_from(&*part_data_enc).expect("Corrupted multipart data");

            blocks.extend_from_slice(&mp.blocks);
        }

        // Compute the e_tag of the multpart upload. Per the S3 standard (according to minio), the
        // e_tag of a multipart uploaded object is the Md5 of the Md5 of the parts.
        let mut hasher = Md5::new();
        let mut size = 0;
        let block_map = trace_try!(self.block_tree());
        for block in &blocks {
            let bi = trace_try!(block_map.get(&block)).unwrap(); // unwrap is fine as all blocks in must be present
            let block_info = Block::try_from(&*bi).expect("Block data is corrupt");
            size += block_info.size;
            hasher.update(&block);
        }
        let e_tag = hasher.finalize().into();

        let bc = trace_try!(self.bucket(&bucket));

        let now = Utc::now().timestamp();
        let object = Object {
            size: size as u64,
            ctime: now,
            parts: cnt as usize,
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
            e_tag: Some(object.format_e_tag()),
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
        obj_meta.ctime = now;

        // TODO: check duplicate?
        let dst_bk = trace_try!(self.bucket(&bucket));
        trace_try!(dst_bk.insert(key, Vec::<u8>::from(&obj_meta)));

        Ok(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(obj_meta.format_e_tag()),
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

        let bm = Vec::from(&BucketMeta {
            name: bucket.clone(),
            ctime: Utc::now().timestamp(),
        });

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

        let e_tag = obj_meta.format_e_tag();
        let block_map = trace_try!(self.block_tree());
        let mut paths = Vec::with_capacity(obj_meta.blocks.len());
        let mut block_size = 0;
        for block in obj_meta.blocks {
            // unwrap here is safe as we only add blocks to the list of an object if they are
            // corectly inserted in the block map
            let block_meta_enc = trace_try!(block_map.get(block)).unwrap();
            let block_meta = trace_try!(Block::try_from(&*block_meta_enc));
            block_size += block_meta.size;
            paths.push(block_meta.disk_path(self.root.clone()));
        }
        debug_assert!(obj_meta.size as usize == block_size);
        let block_stream = BlockStream::new(paths, block_size);

        let stream = ByteStream::new_with_size(block_stream, obj_meta.size as usize);
        Ok(GetObjectOutput {
            body: Some(stream),
            content_length: Some(obj_meta.size as i64),
            last_modified: Some(Utc.timestamp(obj_meta.ctime, 0).to_rfc3339()),
            e_tag: Some(e_tag),
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
            last_modified: Some(Utc.timestamp(obj_meta.ctime, 0).to_rfc3339()),
            e_tag: Some(obj_meta.format_e_tag()),
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
                    e_tag: Some(obj.format_e_tag()),
                    last_modified: Some(Utc.timestamp(obj.ctime, 0).to_rfc3339()),
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
            next_marker: if marker.is_some() { next_marker } else { None },
            marker,
            max_keys: Some(key_count),
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
                    e_tag: Some(obj.format_e_tag()),
                    last_modified: Some(Utc.timestamp(obj.ctime, 0).to_rfc3339()),
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
        let now = Utc::now().timestamp();
        let obj_meta = Object {
            size,
            ctime: now,
            e_tag: hash,
            parts: 0,
            blocks,
        };

        trace_try!(trace_try!(self.bucket(&bucket)).insert(&key, Vec::<u8>::from(&obj_meta)));

        Ok(PutObjectOutput {
            e_tag: Some(obj_meta.format_e_tag()),
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

        let enc_mp = Vec::from(&mp);

        trace_try!(mp_map.insert(storage_key, enc_mp));

        Ok(UploadPartOutput {
            e_tag: Some(e_tag),
            ..UploadPartOutput::default()
        })
    }
}
