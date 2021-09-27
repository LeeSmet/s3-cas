use s3_server::S3Storage;
use std::sync::Arc;

pub struct Passthrough<F> {
    internal: Arc<F>,
}

impl<F> Passthrough<F> {
    pub fn new(internal: F) -> Self {
        Self {
            internal: Arc::new(internal),
        }
    }
}

#[async_trait::async_trait]
impl<F> S3Storage for Passthrough<F>
where
    F: S3Storage + Send + Sync + Unpin,
{
    async fn complete_multipart_upload(
        &self,
        input: s3_server::dto::CompleteMultipartUploadRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::CompleteMultipartUploadOutput,
        s3_server::dto::CompleteMultipartUploadError,
    > {
        eprintln!("complete_multipart_upload start");
        let res = self.internal.clone().complete_multipart_upload(input).await;
        eprintln!("complete_multipart_upload end");
        res
    }

    async fn copy_object(
        &self,
        input: s3_server::dto::CopyObjectRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::CopyObjectOutput,
        s3_server::dto::CopyObjectError,
    > {
        eprintln!("copy_object start");
        let res = self.internal.clone().copy_object(input).await;
        eprintln!("copy_object end");
        res
    }

    async fn create_multipart_upload(
        &self,
        input: s3_server::dto::CreateMultipartUploadRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::CreateMultipartUploadOutput,
        s3_server::dto::CreateMultipartUploadError,
    > {
        eprintln!("create_multipart_upload start");
        let res = self.internal.clone().create_multipart_upload(input).await;
        eprintln!("create_multipart_upload end");
        res
    }

    async fn create_bucket(
        &self,
        input: s3_server::dto::CreateBucketRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::CreateBucketOutput,
        s3_server::dto::CreateBucketError,
    > {
        eprintln!("create_bucket start");
        let res = self.internal.clone().create_bucket(input).await;
        eprintln!("create_bucket end");
        res
    }

    async fn delete_bucket(
        &self,
        input: s3_server::dto::DeleteBucketRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::DeleteBucketOutput,
        s3_server::dto::DeleteBucketError,
    > {
        eprintln!("delete_bucket start");
        let res = self.internal.clone().delete_bucket(input).await;
        eprintln!("delete_bucket end");
        res
    }

    async fn delete_object(
        &self,
        input: s3_server::dto::DeleteObjectRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::DeleteObjectOutput,
        s3_server::dto::DeleteObjectError,
    > {
        eprintln!("delete_object start");
        let res = self.internal.clone().delete_object(input).await;
        eprintln!("delete_object end");
        res
    }

    async fn delete_objects(
        &self,
        input: s3_server::dto::DeleteObjectsRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::DeleteObjectsOutput,
        s3_server::dto::DeleteObjectsError,
    > {
        eprintln!("delete_objects start");
        let res = self.internal.clone().delete_objects(input).await;
        eprintln!("delete_objects end");
        res
    }

    async fn get_bucket_location(
        &self,
        input: s3_server::dto::GetBucketLocationRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::GetBucketLocationOutput,
        s3_server::dto::GetBucketLocationError,
    > {
        eprintln!("get_bucket_location start");
        let res = self.internal.clone().get_bucket_location(input).await;
        eprintln!("get_bucket_location end");
        res
    }

    async fn get_object(
        &self,
        input: s3_server::dto::GetObjectRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::GetObjectOutput,
        s3_server::dto::GetObjectError,
    > {
        eprintln!("get_object start");
        let res = self.internal.clone().get_object(input).await;
        eprintln!("get_object end");
        res
    }

    async fn head_bucket(
        &self,
        input: s3_server::dto::HeadBucketRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::HeadBucketOutput,
        s3_server::dto::HeadBucketError,
    > {
        eprintln!("head_bucket start");
        let res = self.internal.clone().head_bucket(input).await;
        eprintln!("head_bucket end");
        res
    }

    async fn head_object(
        &self,
        input: s3_server::dto::HeadObjectRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::HeadObjectOutput,
        s3_server::dto::HeadObjectError,
    > {
        eprintln!("head_object start");
        let res = self.internal.clone().head_object(input).await;
        eprintln!("head_object end");
        res
    }

    async fn list_buckets(
        &self,
        input: s3_server::dto::ListBucketsRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::ListBucketsOutput,
        s3_server::dto::ListBucketsError,
    > {
        eprintln!("list_buckets start");
        let res = self.internal.clone().list_buckets(input).await;
        eprintln!("list_buckets end");
        res
    }

    async fn list_objects(
        &self,
        input: s3_server::dto::ListObjectsRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::ListObjectsOutput,
        s3_server::dto::ListObjectsError,
    > {
        eprintln!("list_objects start");
        let res = self.internal.clone().list_objects(input).await;
        eprintln!("list_objects end");
        res
    }

    async fn list_objects_v2(
        &self,
        input: s3_server::dto::ListObjectsV2Request,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::ListObjectsV2Output,
        s3_server::dto::ListObjectsV2Error,
    > {
        eprintln!("list_objects_v2 start");
        let res = self.internal.clone().list_objects_v2(input).await;
        eprintln!("list_objects_v2 end");
        res
    }

    async fn put_object(
        &self,
        input: s3_server::dto::PutObjectRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::PutObjectOutput,
        s3_server::dto::PutObjectError,
    > {
        eprintln!("put_object start");
        let res = self.internal.clone().put_object(input).await;
        eprintln!("put_object end");
        res
    }

    async fn upload_part(
        &self,
        input: s3_server::dto::UploadPartRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::UploadPartOutput,
        s3_server::dto::UploadPartError,
    > {
        eprintln!("upload_part start");
        let res = self.internal.clone().upload_part(input).await;
        eprintln!("upload_part end");
        res
    }
}
