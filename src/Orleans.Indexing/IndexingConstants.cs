namespace Orleans.Indexing
{
    internal static class IndexingConstants
    {
        public const string MEMORY_STORAGE_PROVIDER_NAME = "MemoryStore";
        public const string INDEXING_STORAGE_PROVIDER_NAME = "IndexingStorageProvider";
        public const string INDEXING_WORKFLOWQUEUE_STORAGE_PROVIDER_NAME = "IndexingWorkflowQueueStorageProvider";
        public const string INDEXING_STREAM_PROVIDER_NAME = "IndexingStreamProvider";
        public const string INDEXING_OPTIONS_NAME = nameof(IndexingOptions);

        public const int INDEX_WORKFLOW_QUEUE_HANDLER_GRAIN_SERVICE_TYPE_CODE = 251;
        public const int INDEX_WORKFLOW_QUEUE_GRAIN_SERVICE_TYPE_CODE = 252;
        public const int HASH_INDEX_PARTITIONED_PER_SILO_BUCKET_GRAIN_SERVICE_TYPE_CODE = 253;
    }
}
