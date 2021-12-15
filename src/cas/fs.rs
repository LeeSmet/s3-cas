use super::{
    block::{Block, BlockID, BLOCKID_SIZE},
    block_stream::BlockStream,
    errors::FsError,
    multipart::MultiPart,
    object::Object,
    range_request::parse_range_request,
};
use async_trait::async_trait;
use chrono::prelude::*;
use faster_hex::{hex_decode, hex_string};
use futures::{
    channel::mpsc::unbounded,
    ready,
    sink::SinkExt,
    stream,
    stream::{StreamExt, TryStreamExt},
    Stream,
};
use md5::{Digest, Md5};
use s3_server::dto::{
    CopyObjectOutput, CopyObjectRequest, CopyObjectResult, DeleteBucketOutput, DeleteBucketRequest,
    DeleteObjectOutput, DeleteObjectRequest, DeleteObjectsOutput, DeleteObjectsRequest,
    DeletedObject, PutObjectOutput, PutObjectRequest,
};
use s3_server::{
    dto::{
        Bucket, ByteStream, CompleteMultipartUploadOutput, CompleteMultipartUploadRequest,
        CreateBucketOutput, CreateBucketRequest, CreateMultipartUploadOutput,
        CreateMultipartUploadRequest, GetBucketLocationOutput, GetBucketLocationRequest,
        GetObjectOutput, GetObjectRequest, HeadBucketOutput, HeadBucketRequest, HeadObjectOutput,
        HeadObjectRequest, ListBucketsOutput, ListBucketsRequest, ListObjectsOutput,
        ListObjectsRequest, ListObjectsV2Output, ListObjectsV2Request, Object as S3Object,
        UploadPartOutput, UploadPartRequest,
    },
    errors::S3StorageResult,
    headers::AmzCopySource,
    S3Storage,
};
use sled::{Db, Transactional};
use std::{
    convert::{TryFrom, TryInto},
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
pub const PTR_SIZE: usize = mem::size_of::<usize>(); // Size of a `usize` in bytes
const MAX_KEYS: i64 = 1000;

pub struct CasFS {
    db: Db,
    root: PathBuf,
}

impl CasFS {
    pub fn new(mut root: PathBuf, mut meta_path: PathBuf) -> Self {
        meta_path.push("db");
        root.push("blocks");
        Self {
            db: sled::open(meta_path).unwrap(),
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
    // TODO: this is very much not optimal
    async fn bucket_delete(&self, bucket_name: &str) -> Result<(), sled::Error> {
        let bmt = self.bucket_meta_tree()?;
        bmt.remove(bucket_name)?;
        #[cfg(feature = "refcount")]
        {
            let bucket = self.bucket(bucket_name)?;
            for key in bucket.iter().keys() {
                self.delete_object(
                    bucket_name,
                    std::str::from_utf8(&*key?).expect("keys are valid utf-8"),
                )
                .await?;
            }
        }
        self.db.drop_tree(bucket_name)?;
        Ok(())
    }

    /// Delete an object from a bucket.
    async fn delete_object(&self, bucket: &str, object: &str) -> Result<(), sled::Error> {
        #[cfg(not(feature = "refcount"))]
        {
            self.bucket(bucket)?.remove(object).map(|_| ())
        }
        #[cfg(feature = "refcount")]
        {
            // Remove an object. This fetches the object, decrements the refcount of all blocks,
            // and removes blocks which are no longer referenced.
            let block_map = self.block_tree()?;
            let path_map = self.path_tree()?;
            let bucket = self.bucket(bucket)?;
            let blocks_to_delete_res: Result<Vec<Block>, sled::transaction::TransactionError> =
                (&bucket, &block_map).transaction(|(bucket, blocks)| {
                    match bucket.get(object)? {
                        None => Ok(vec![]),
                        Some(o) => {
                            let obj = Object::try_from(&*o).expect("Malformed object");
                            let mut to_delete = Vec::with_capacity(obj.blocks().len());
                            // delete the object in the database, we have it in memory to remove the
                            // blocks as needed.
                            bucket.remove(object)?;
                            for block_id in obj.blocks() {
                                match blocks.get(block_id)? {
                                    // This is technically impossible
                                    None => eprintln!(
                                        "missing block {} in block map",
                                        hex_string(&*block_id)
                                    ),
                                    Some(block_data) => {
                                        let mut block = Block::try_from(&*block_data)
                                            .expect("corrupt block data");
                                        // We are deleting the last reference to the block, delete the
                                        // whole block.
                                        // Importantly, we don't remove the path yet from the path map.
                                        // Leaving this path dangling in the database ensures it is not
                                        // filled in by another block, before we properly delete the
                                        // path from disk.
                                        if block.rc() == 1 {
                                            blocks.remove(&*block_id)?;
                                            to_delete.push(block);
                                        } else {
                                            block.decrement_refcount();
                                            blocks.insert(&*block_id, Vec::from(&block))?;
                                        }
                                    }
                                }
                            }
                            Ok(to_delete)
                        }
                    }
                });

            let blocks_to_delete = match blocks_to_delete_res {
                Err(sled::transaction::TransactionError::Storage(e)) => {
                    return Err(e);
                }
                Ok(blocks) => blocks,
                // We don't abort manually so this can't happen
                Err(sled::transaction::TransactionError::Abort(_)) => unreachable!(),
            };

            // Now delete all the blocks from disk, and unlink them in the path map.
            for block in blocks_to_delete {
                async_fs::remove_file(block.disk_path(self.root.clone()))
                    .await
                    .expect("Could not delete file");
                // Now that the path is free it can be removed from the path map
                if let Err(e) = path_map.remove(block.path()) {
                    // Only print error, we might be able to remove the other ones. If we exist
                    // here, those will be left dangling.
                    eprintln!(
                        "Could not unlink path {} from path map: {}",
                        hex_string(block.path()),
                        e
                    );
                };
            }

            Ok(())
        }
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
            Ok(buffers) => buffers.into_iter().map(Ok).collect(),
            Err(e) => vec![Err(e)],
        })
        .map(stream::iter)
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
                        match blocks.get(block_hash)? {
                            #[allow(unused_variables)] // Don't warn here if we don't use refcount.
                            Some(block_data) => {
                                // Block already exists
                                #[cfg(feature = "refcount")]
                                {
                                    // bump refcount on the block
                                    let mut block = Block::try_from(&*block_data)
                                        .expect("Only valid blocks are stored");
                                    block.increment_refcount();
                                    // write block back
                                    // TODO: this could be done in an `update_and_fetch`
                                    blocks.insert(&block_hash, Vec::from(&block))?;
                                }

                                Ok(false)
                            }
                            None => {
                                // find a free path
                                for index in 1..BLOCKID_SIZE {
                                    if paths.get(&block_hash[..index])?.is_some() {
                                        // path already used, try the next one
                                        continue;
                                    };

                                    // path is free, insert
                                    paths.insert(&block_hash[..index], &block_hash)?;

                                    let block = Block::new(data_len, block_hash[..index].to_vec());

                                    blocks.insert(&block_hash, Vec::from(&block))?;
                                    return Ok(true);
                                }

                                // The loop above can only NOT find a path in case it is duplicate
                                // block, wich already breaks out at the start.
                                unreachable!();
                            }
                        }
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
            name: unsafe { String::from_utf8_unchecked(value[8 + PTR_SIZE..].to_vec()) },
        })
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
                    if self.buffer.is_empty() {
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

            blocks.extend_from_slice(mp.blocks());
        }

        // Compute the e_tag of the multpart upload. Per the S3 standard (according to minio), the
        // e_tag of a multipart uploaded object is the Md5 of the Md5 of the parts.
        let mut hasher = Md5::new();
        let mut size = 0;
        let block_map = trace_try!(self.block_tree());
        for block in &blocks {
            let bi = trace_try!(block_map.get(&block)).unwrap(); // unwrap is fine as all blocks in must be present
            let block_info = Block::try_from(&*bi).expect("Block data is corrupt");
            size += block_info.size();
            hasher.update(&block);
        }
        let e_tag = hasher.finalize().into();

        let bc = trace_try!(self.bucket(&bucket));

        let object = Object::new(size as u64, e_tag, cnt as usize, blocks);

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

        if !trace_try!(self.bucket_exists(bucket)) {
            return Err(code_error!(NoSuchBucket, "Target bucket does not exist").into());
        }

        let source_bk = trace_try!(self.bucket(&input.bucket));
        let mut obj_meta = match trace_try!(source_bk.get(&input.key)) {
            // unwrap here is safe as it means the DB is corrupted
            Some(enc_meta) => Object::try_from(&*enc_meta).unwrap(),
            None => return Err(code_error!(NoSuchKey, "Source key does not exist").into()),
        };

        obj_meta.touch();

        // TODO: check duplicate?
        let dst_bk = trace_try!(self.bucket(bucket));
        trace_try!(dst_bk.insert(key, Vec::<u8>::from(&obj_meta)));

        Ok(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(obj_meta.format_e_tag()),
                last_modified: Some(Utc.timestamp(obj_meta.ctime(), 0).to_rfc3339()),
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

        trace_try!(self.bucket_delete(&bucket).await);

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

        trace_try!(self.delete_object(&bucket, &key).await);

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

        let mut deleted = Vec::with_capacity(input.delete.objects.len());
        let mut errors = Vec::new();

        for object in input.delete.objects {
            match self.delete_object(&input.bucket, &object.key).await {
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
            errors: if errors.is_empty() {
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
        let GetObjectRequest {
            bucket, key, range, ..
        } = input;

        let range = parse_range_request(&range);

        let bk = trace_try!(self.bucket(&bucket));
        let obj = match trace_try!(bk.get(&key)) {
            None => return Err(code_error!(NoSuchKey, "The specified key does not exist").into()),
            Some(obj) => obj,
        };
        let obj_meta = trace_try!(Object::try_from(&obj.to_vec()[..]));
        let stream_size = range.size(obj_meta.size());

        let e_tag = obj_meta.format_e_tag();
        let block_map = trace_try!(self.block_tree());
        let mut paths = Vec::with_capacity(obj_meta.blocks().len());
        let mut block_size = 0;
        for block in obj_meta.blocks() {
            // unwrap here is safe as we only add blocks to the list of an object if they are
            // corectly inserted in the block map
            let block_meta_enc = trace_try!(block_map.get(block)).unwrap();
            let block_meta = trace_try!(Block::try_from(&*block_meta_enc));
            block_size += block_meta.size();
            paths.push((block_meta.disk_path(self.root.clone()), block_meta.size()));
        }
        debug_assert!(obj_meta.size() as usize == block_size);
        let block_stream = BlockStream::new(paths, block_size, range);

        // TODO: part count
        let stream = ByteStream::new_with_size(block_stream, stream_size as usize);
        Ok(GetObjectOutput {
            body: Some(stream),
            content_length: Some(stream_size as i64),
            last_modified: Some(Utc.timestamp(obj_meta.ctime(), 0).to_rfc3339()),
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
            content_length: Some(obj_meta.size() as i64),
            last_modified: Some(Utc.timestamp(obj_meta.ctime(), 0).to_rfc3339()),
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
            .map(|mk| if mk > MAX_KEYS { MAX_KEYS } else { mk })
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
                    last_modified: Some(Utc.timestamp(obj.ctime(), 0).to_rfc3339()),
                    owner: None,
                    size: Some(obj.size() as i64),
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
            start_after,
            max_keys,
            continuation_token,
            ..
        } = input;

        let b = trace_try!(self.bucket(&bucket));

        let key_count = max_keys
            .map(|mk| if mk > MAX_KEYS { MAX_KEYS } else { mk })
            .unwrap_or(MAX_KEYS);

        let token = if let Some(ref rt) = continuation_token {
            let mut out = vec![0; rt.len() / 2];
            if hex_decode(rt.as_bytes(), &mut out).is_err() {
                return Err(
                    code_error!(InvalidToken, "continuation token has an invalid format").into(),
                );
            };
            match String::from_utf8(out) {
                Ok(s) => Some(s),
                Err(_) => {
                    return Err(code_error!(InvalidToken, "continuation token is invalid").into())
                }
            }
        } else {
            None
        };

        let mut objects: Vec<_> = b
            .scan_prefix(prefix.as_deref().or(Some("")).unwrap())
            .filter_map(|read_result| match read_result {
                Ok((r, k)) => Some((r, k)),
                Err(_) => None,
            })
            .skip_while(|(key, _)| match start_after {
                None => false,
                Some(ref start_after) => key <= &sled::IVec::from(start_after.as_bytes()),
            })
            .skip_while(|(key, _)| match token {
                None => false,
                Some(ref t) => key < &sled::IVec::from(t.as_bytes()),
            })
            .map(|(raw_key, raw_value)| {
                // SAFETY: we only insert valid utf8 strings
                let key = unsafe { String::from_utf8_unchecked(raw_key.to_vec()) };
                // unwrap is fine as it would mean either a coding error or a corrupt DB
                let obj = Object::try_from(&*raw_value).unwrap();

                S3Object {
                    key: Some(key),
                    e_tag: Some(obj.format_e_tag()),
                    last_modified: Some(Utc.timestamp(obj.ctime(), 0).to_rfc3339()),
                    owner: None,
                    size: Some(obj.size() as i64),
                    storage_class: None,
                }
            })
            .take((key_count + 1) as usize)
            .collect();

        let mut next_token = None;
        let truncated = objects.len() == key_count as usize + 1;
        if truncated {
            next_token = Some(hex_string(objects.pop().unwrap().key.unwrap().as_bytes()))
        }

        Ok(ListObjectsV2Output {
            key_count: Some(objects.len() as i64),
            contents: Some(objects),
            common_prefixes: None,
            delimiter,
            continuation_token,
            encoding_type,
            is_truncated: Some(truncated),
            prefix,
            name: Some(bucket),
            max_keys: Some(key_count),
            start_after,
            next_continuation_token: next_token,
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

        let obj_meta = Object::new(size, hash, 0, blocks);

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
        let mp = MultiPart::new(
            size as usize,
            part_number,
            bucket,
            key,
            upload_id,
            hash,
            blocks,
        );

        let enc_mp = Vec::from(&mp);

        trace_try!(mp_map.insert(storage_key, enc_mp));

        Ok(UploadPartOutput {
            e_tag: Some(e_tag),
            ..UploadPartOutput::default()
        })
    }
}
