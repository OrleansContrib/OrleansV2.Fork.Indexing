using Orleans.Concurrency;
using Orleans.Runtime;

namespace Orleans.Indexing
{
    /// <summary>
    /// A simple implementation of a single-bucket persistent hash-index
    /// 
    /// </summary>
    /// <typeparam name="K">type of hash-index key</typeparam>
    /// <typeparam name="V">type of grain that is being indexed</typeparam>
    [Reentrant]
    public class TotalHashIndexPartitionedPerKeyBucketImpl<K, V> : HashIndexPartitionedPerKeyBucket<K, V>, ITotalHashIndexPartitionedPerKeyBucket<K, V> where V : class, IIndexableGrain
    {
        public TotalHashIndexPartitionedPerKeyBucketImpl() : base(IndexingConstants.INDEXING_STORAGE_PROVIDER_NAME) { }

        internal override GrainReference GetNextBucket(out IIndexInterface<K, V> nextBucketIndexInterface)
        {
            var nextBucket = GrainFactory.GetGrain<TotalHashIndexPartitionedPerKeyBucketImpl<K, V>>(IndexUtils.GetNextIndexBucketIdInChain(this.AsWeaklyTypedReference()));
            nextBucketIndexInterface = nextBucket;
            return nextBucket.AsWeaklyTypedReference();
        }
    }
}
