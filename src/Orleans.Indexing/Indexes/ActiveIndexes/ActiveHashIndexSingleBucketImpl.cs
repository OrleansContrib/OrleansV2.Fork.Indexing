using Orleans.Concurrency;
using Orleans.Runtime;

namespace Orleans.Indexing
{
    /// <summary>
    /// A simple implementation of a single-grain in-memory hash-index
    /// </summary>
    /// <typeparam name="K">type of hash-index key</typeparam>
    /// <typeparam name="V">type of grain that is being indexed</typeparam>
    [Reentrant]
    public class ActiveHashIndexSingleBucketImpl<K, V> : HashIndexSingleBucket<K, V>, IActiveHashIndexSingleBucket<K, V> where V : class, IIndexableGrain
    {
        public ActiveHashIndexSingleBucketImpl() : base(IndexingConstants.INDEXING_STORAGE_PROVIDER_NAME) { }

        internal override GrainReference GetNextBucket(out IIndexInterface<K, V> nextBucketIndexInterface)
        {
            var nextBucket = this.GrainFactory.GetGrain<IActiveHashIndexSingleBucket<K, V>>(IndexUtils.GetNextIndexBucketIdInChain(this.AsWeaklyTypedReference()));
            nextBucketIndexInterface = nextBucket;
            return nextBucket.AsWeaklyTypedReference();
        }
    }
}
