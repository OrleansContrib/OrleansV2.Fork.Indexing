using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Indexing
{
    /// <summary>
    /// A simple implementation of a direct storage managed index (i.e., without caching)
    /// </summary>
    /// <typeparam name="K">type of hash-index key</typeparam>
    /// <typeparam name="V">type of grain that is being indexed</typeparam>
    [Reentrant]
    //[StatelessWorker]
    //TODO: a bug in OrleansStreams currently prevents streams from working with stateless grains, so this grain cannot be StatelessWorker.
    //TODO: basically, this class does not even need to be a grain, but it's not possible to call a GrainService from a non-grain
    public class DirectStorageManagedIndexImpl<K, V> : Grain, IDirectStorageManagedIndex<K, V> where V : class, IIndexableGrain
    {
        private IGrainStorage _grainStorage;
        private string _grainClassName;

        private string _indexedField;

        // IndexManager (and therefore logger) cannot be set in ctor because Grain activation has not yet set base.Runtime.
        internal SiloIndexManager SiloIndexManager => IndexManager.GetSiloIndexManager(ref __indexManager, base.ServiceProvider);
        private SiloIndexManager __indexManager;

        private ILogger Logger => __logger ?? (__logger = this.SiloIndexManager.LoggerFactory.CreateLoggerWithFullCategoryName<DirectStorageManagedIndexImpl<K, V>>());
        private ILogger __logger;

        public override Task OnActivateAsync()
        {
            var indexName = IndexUtils.GetIndexNameFromIndexGrain(this);
            _indexedField = indexName.Substring(2);
            return base.OnActivateAsync();
        }

        public Task<bool> DirectApplyIndexUpdateBatch(Immutable<IDictionary<IIndexableGrain, IList<IMemberUpdate>>> iUpdates,
                                                        bool isUnique, IndexMetaData idxMetaData, SiloAddress siloAddress = null)
            => Task.FromResult(true);   // The index is maintained by the underlying _grainStorage, when the grain's WriteStateAsync is called by the IndexedState implementation

        public Task<bool> DirectApplyIndexUpdate(IIndexableGrain g, Immutable<IMemberUpdate> iUpdate, bool isUniqueIndex,
                                                 IndexMetaData idxMetaData, SiloAddress siloAddress)
            => Task.FromResult(true);   // The index is maintained by the underlying _grainStorage, when the grain's WriteStateAsync is called by the IndexedState implementation

        public async Task LookupAsync(IOrleansQueryResultStream<V> result, K key)
        {
            var res = await LookupGrainReferences(key);
            await result.OnNextBatchAsync(res);
            await result.OnCompletedAsync();
        }

        private async Task<List<V>> LookupGrainReferences(K key)
        {
            EnsureGrainStorage();

            // Dynamically find its LookupAsync method (currently only CosmosDB supports this). TODO: define IOrleansIndexingStorageProvider?
            dynamic indexableStorageProvider = _grainStorage;

            var qualifiedField = IndexingConstants.UserStatePrefix + _indexedField;
            List<GrainReference> resultReferences = await indexableStorageProvider.LookupAsync<K>(_grainClassName, qualifiedField, key);
            return resultReferences.Select(grain => grain.Cast<V>()).ToList();
        }

        public async Task<V> LookupUniqueAsync(K key)
        {
            var result = new OrleansFirstQueryResultStream<V>();
            var taskCompletionSource = new TaskCompletionSource<V>();
            Task<V> tsk = taskCompletionSource.Task;
            Action<V> responseHandler = taskCompletionSource.SetResult;
            await result.SubscribeAsync(new QueryFirstResultStreamObserver<V>(responseHandler));
            await LookupAsync(result, key);
            return await tsk;
        }

        public Task Dispose() => Task.CompletedTask;

        public Task<bool> IsAvailable() => Task.FromResult(true);

        Task IIndexInterface.LookupAsync(IOrleansQueryResultStream<IIndexableGrain> result, object key) => this.LookupAsync(result.Cast<V>(), (K)key);

        public async Task<IOrleansQueryResult<V>> LookupAsync(K key) => new OrleansQueryResult<V>(await this.LookupGrainReferences(key));

        async Task<IOrleansQueryResult<IIndexableGrain>> IIndexInterface.LookupAsync(object key) => await this.LookupAsync((K)key);

        private void EnsureGrainStorage()
        {
            if (_grainStorage == null)
            {
                var implementation = TypeCodeMapper.GetImplementation(this.SiloIndexManager.GrainTypeResolver, typeof(V));
                if (implementation == null || (_grainClassName = implementation.GrainClass) == null ||
                        !this.SiloIndexManager.CachedTypeResolver.TryResolveType(_grainClassName, out Type grainClassType))
                {
                    throw new IndexException($"The grain implementation class {implementation.GrainClass} for grain" +
                                             " interface {IndexUtils.GetFullTypeName(typeof(V))} was not resolved.");
                }
                _grainStorage = grainClassType.GetGrainStorage(this.SiloIndexManager.ServiceProvider);
            }
        }
    }
}
