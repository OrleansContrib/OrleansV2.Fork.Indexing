using Orleans.Concurrency;
using Orleans.Runtime;

namespace Orleans.Indexing
{
    /// <summary>
    /// A simple implementation of a single-grain persistent hash-index
    /// </summary>
    /// <typeparam name="K">type of hash-index key</typeparam>
    /// <typeparam name="V">type of grain that is being indexed</typeparam>
    [Reentrant]
    public class TotalHashIndexSingleBucketImpl<K, V> : HashIndexSingleBucket<K, V>, ITotalHashIndexSingleBucket<K, V> where V : class, IIndexableGrain
    {
        public TotalHashIndexSingleBucketImpl() : base(IndexingConstants.INDEXING_STORAGE_PROVIDER_NAME) { }

        internal override GrainReference GetNextBucket(out IIndexInterface<K, V> nextBucketIndexInterface)
        {
            var nextBucket = GrainFactory.GetGrain<TotalHashIndexSingleBucketImpl<K, V>>(IndexUtils.GetNextIndexBucketIdInChain(this.AsWeaklyTypedReference()));
            nextBucketIndexInterface = nextBucket;
            return nextBucket.AsWeaklyTypedReference();
        }
    }
}
