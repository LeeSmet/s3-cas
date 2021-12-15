use async_trait::async_trait;
use prometheus::{register_int_counter_vec, IntCounterVec};
use s3_server::S3Storage;
use std::sync::Arc;

const S3_API_METHODS: &[&'static str] = &[
    "complete_multipart_upload",
    "copy_object",
    "create_multipart_upload",
    "create_bucket",
    "delete_bucket",
    "delete_object",
    "delete_objects",
    "get_bucket_location",
    "get_object",
    "head_bucket",
    "head_object",
    "list_buckets",
    "list_objects",
    "list_objects_v2",
    "put_object",
    "upload_part",
];

#[derive(Clone)]
pub struct SharedMetrics {
    metrics: Arc<Metrics>,
}

impl SharedMetrics {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(Metrics::new()),
        }
    }

    pub fn add_method_call(&self, call_name: &str) {
        self.metrics.add_method_call(call_name);
    }
}

struct Metrics {
    method_calls: IntCounterVec,
}

// TODO: this can be improved, make sure this does not crash on multiple instances;
impl Metrics {
    fn new() -> Self {
        let method_calls = register_int_counter_vec!(
            "s3_api_method_invocations",
            "Amount of times a particular S3 API method has been called in the lifetime of the process",
            &["api_method"],
        ).expect("can register an int counter vec in the default registry");

        // instantiate the correct counters for api calls
        for api in S3_API_METHODS {
            method_calls.with_label_values(&[api]);
        }

        Self { method_calls }
    }

    fn add_method_call(&self, call_name: &str) {
        self.method_calls.with_label_values(&[call_name]).inc();
    }
}

pub struct MetricFs<T> {
    storage: T,
    metrics: SharedMetrics,
}

impl<T> MetricFs<T> {
    pub fn new(storage: T, metrics: SharedMetrics) -> Self {
        Self { storage, metrics }
    }
}

#[async_trait]
impl<T> S3Storage for MetricFs<T>
where
    T: S3Storage + Sync + Send,
{
    async fn complete_multipart_upload(
        &self,
        input: s3_server::dto::CompleteMultipartUploadRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::CompleteMultipartUploadOutput,
        s3_server::dto::CompleteMultipartUploadError,
    > {
        self.metrics.add_method_call("complete_multipart_upload");
        self.storage.complete_multipart_upload(input).await
    }

    async fn copy_object(
        &self,
        input: s3_server::dto::CopyObjectRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::CopyObjectOutput,
        s3_server::dto::CopyObjectError,
    > {
        self.metrics.add_method_call("copy_object");
        self.storage.copy_object(input).await
    }

    async fn create_multipart_upload(
        &self,
        input: s3_server::dto::CreateMultipartUploadRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::CreateMultipartUploadOutput,
        s3_server::dto::CreateMultipartUploadError,
    > {
        self.metrics.add_method_call("create_multipart_upload");
        self.storage.create_multipart_upload(input).await
    }

    async fn create_bucket(
        &self,
        input: s3_server::dto::CreateBucketRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::CreateBucketOutput,
        s3_server::dto::CreateBucketError,
    > {
        self.metrics.add_method_call("create_bucket");
        self.storage.create_bucket(input).await
    }

    async fn delete_bucket(
        &self,
        input: s3_server::dto::DeleteBucketRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::DeleteBucketOutput,
        s3_server::dto::DeleteBucketError,
    > {
        self.metrics.add_method_call("delete_bucket");
        self.storage.delete_bucket(input).await
    }

    async fn delete_object(
        &self,
        input: s3_server::dto::DeleteObjectRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::DeleteObjectOutput,
        s3_server::dto::DeleteObjectError,
    > {
        self.metrics.add_method_call("delete_object");
        self.storage.delete_object(input).await
    }

    async fn delete_objects(
        &self,
        input: s3_server::dto::DeleteObjectsRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::DeleteObjectsOutput,
        s3_server::dto::DeleteObjectsError,
    > {
        self.metrics.add_method_call("delete_objects");
        self.storage.delete_objects(input).await
    }

    async fn get_bucket_location(
        &self,
        input: s3_server::dto::GetBucketLocationRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::GetBucketLocationOutput,
        s3_server::dto::GetBucketLocationError,
    > {
        self.metrics.add_method_call("get_bucket_location");
        self.storage.get_bucket_location(input).await
    }

    async fn get_object(
        &self,
        input: s3_server::dto::GetObjectRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::GetObjectOutput,
        s3_server::dto::GetObjectError,
    > {
        self.metrics.add_method_call("get_object");
        self.storage.get_object(input).await
    }

    async fn head_bucket(
        &self,
        input: s3_server::dto::HeadBucketRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::HeadBucketOutput,
        s3_server::dto::HeadBucketError,
    > {
        self.metrics.add_method_call("head_bucket");
        self.storage.head_bucket(input).await
    }

    async fn head_object(
        &self,
        input: s3_server::dto::HeadObjectRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::HeadObjectOutput,
        s3_server::dto::HeadObjectError,
    > {
        self.metrics.add_method_call("head_object");
        self.storage.head_object(input).await
    }

    async fn list_buckets(
        &self,
        input: s3_server::dto::ListBucketsRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::ListBucketsOutput,
        s3_server::dto::ListBucketsError,
    > {
        self.metrics.add_method_call("list_buckets");
        self.storage.list_buckets(input).await
    }

    async fn list_objects(
        &self,
        input: s3_server::dto::ListObjectsRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::ListObjectsOutput,
        s3_server::dto::ListObjectsError,
    > {
        self.metrics.add_method_call("list_objects");
        self.storage.list_objects(input).await
    }

    async fn list_objects_v2(
        &self,
        input: s3_server::dto::ListObjectsV2Request,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::ListObjectsV2Output,
        s3_server::dto::ListObjectsV2Error,
    > {
        self.metrics.add_method_call("list_objects_v2");
        self.storage.list_objects_v2(input).await
    }

    async fn put_object(
        &self,
        input: s3_server::dto::PutObjectRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::PutObjectOutput,
        s3_server::dto::PutObjectError,
    > {
        self.metrics.add_method_call("put_object");
        self.storage.put_object(input).await
    }

    async fn upload_part(
        &self,
        input: s3_server::dto::UploadPartRequest,
    ) -> s3_server::errors::S3StorageResult<
        s3_server::dto::UploadPartOutput,
        s3_server::dto::UploadPartError,
    > {
        self.metrics.add_method_call("upload_part");
        self.storage.upload_part(input).await
    }
}
