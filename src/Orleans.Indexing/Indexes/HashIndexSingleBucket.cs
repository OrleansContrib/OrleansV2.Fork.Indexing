using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Indexing.Facet;
using Orleans.Transactions.Abstractions;
using System.Diagnostics;

namespace Orleans.Indexing
{
    /// <summary>
    /// A simple implementation of a single-grain in-memory hash-index
    /// </summary>
    /// <typeparam name="K">type of hash-index key</typeparam>
    /// <typeparam name="V">type of grain that is being indexed</typeparam>
    [Reentrant]
    public abstract class HashIndexSingleBucket<K, V> : Grain, ITransactionalLookupIndex<K, V>, IHashIndexSingleBucketInterface<K, V> where V : class, IIndexableGrain
    {
        // IndexManager (and therefore logger) cannot be set in ctor because Grain activation has not yet set base.Runtime.
        internal SiloIndexManager SiloIndexManager => IndexManager.GetSiloIndexManager(ref __siloIndexManager, base.ServiceProvider);
        private SiloIndexManager __siloIndexManager;

        private ILogger Logger => __logger ?? (__logger = this.SiloIndexManager.LoggerFactory.CreateLoggerWithFullCategoryName<HashIndexSingleBucket<K, V>>());
        private ILogger __logger;

        private ITransactionalState<HashIndexBucketState<K, V>> maybeTransactionalState;
        private string nonTransactionalStorageProviderName;

        private bool IsTransactional => this.nonTransactionalStorageProviderName == null;

        public HashIndexSingleBucket(string nonTransactionalStorageProviderName)
            => this.nonTransactionalStorageProviderName = nonTransactionalStorageProviderName;

        public HashIndexSingleBucket(ITransactionalState<HashIndexBucketState<K, V>> transactionalState)
            => this.maybeTransactionalState = transactionalState;

        public async override Task OnActivateAsync()
        {
            if (this.maybeTransactionalState == null)
            {
                var storage = this.SiloIndexManager.GetStorageBridge<HashIndexBucketState<K, V>>(this, this.nonTransactionalStorageProviderName);
                var nonTransactionalState = await NonTransactionalState<HashIndexBucketState<K, V>>.CreateAsync(storage);
                this.EnsureStateInitialized(nonTransactionalState.State);
                this.maybeTransactionalState = nonTransactionalState;
            }

            this.write_lock = new AsyncLock();
            this.writeRequestIdGen = 0;
            this.pendingWriteRequests = new HashSet<int>();
            await base.OnActivateAsync();
        }

        private void EnsureStateInitialized(HashIndexBucketState<K, V> state)
        {
            if (state.IndexMap == null)
            {
                state.IndexMap = new Dictionary<K, HashIndexSingleBucketEntry<V>>();

                //TODO: add support for index construction. Currently the Total indexes can only be defined in advance.
                //if (this.State.IndexStatus == IndexStatus.UnderConstruction) { /*Build the index!*/ }
                state.IndexStatus = IndexStatus.Available;
            }
        }

        private async Task<TResult> ReadStateAsync<TResult>(Func<HashIndexBucketState<K, V>, TResult> readFunc)
        {
            return await this.maybeTransactionalState.PerformRead(state =>
            {
                this.EnsureStateInitialized(state);
                return readFunc(state);
            });
        }

        private async Task<TResult> WriteStateAsync<TResult>(Func<HashIndexBucketState<K, V>, TResult> updateFunc)
        {
            return await this.maybeTransactionalState.PerformUpdate(state =>
            {
                this.EnsureStateInitialized(state);
                return updateFunc(state);
            });
        }

        private Task WriteStateAsync(Action<HashIndexBucketState<K, V>> updateFunc)
            => this.WriteStateAsync(state => { updateFunc(state); return true; });

        private Task WriteStateAsync() => this.WriteStateAsync(_ => { });

        #region Reentrant Index Update
        #region Reentrant Index Update Variables

        /// <summary>
        /// This lock is used to queue all the writes to the storage and do them in a single batch, i.e., group commit
        /// 
        /// Works hand-in-hand with pendingWriteRequests and writeRequestIdGen.
        /// </summary>
        private AsyncLock write_lock;

        /// <summary>
        /// Creates a unique ID for each write request to the storage.
        /// 
        /// The values generated by this ID generator are used in pendingWriteRequests
        /// </summary>
        private int writeRequestIdGen;

        /// <summary>
        /// All write requests that are waiting behind write_lock are accumulated in this data structure, and all will be done at once.
        /// </summary>
        private HashSet<int> pendingWriteRequests;

#endregion Reentrant Index Update Variables

        public async Task<bool> DirectApplyIndexUpdateBatch(Immutable<IDictionary<IIndexableGrain, IList<IMemberUpdate>>> iUpdates, bool isUnique, IndexMetaData idxMetaData, SiloAddress siloAddress = null)
        {
            Debug.Assert(!this.IsTransactional, "Batch index update should not be called for transactional indexes");
            this.Logger.Trace($"Started calling DirectApplyIndexUpdateBatch with the following parameters: isUnique = {isUnique}, siloAddress = {siloAddress}, iUpdates = {MemberUpdate.UpdatesToString(iUpdates.Value)}");

            Task directApplyIndexUpdatesNonPersistent(IIndexableGrain g, IList<IMemberUpdate> updates)
                => Task.WhenAll(updates.Select(updt => DirectApplyIndexUpdateNonPersistent(g, updt, isUnique, idxMetaData, siloAddress)));

            await Task.WhenAll(iUpdates.Value.Select(kv => directApplyIndexUpdatesNonPersistent(kv.Key, kv.Value)));
            await PersistIndexNonTransactional();

            this.Logger.Trace($"Finished calling DirectApplyIndexUpdateBatch with the following parameters: isUnique = {isUnique}, siloAddress = {siloAddress}, iUpdates = {MemberUpdate.UpdatesToString(iUpdates.Value)}");
            return true;
        }

        // Note that GetNextBucket() calls GetGrain on an ID that is "next bucket for this grain", so if NextBucket already
        // exists this has the effect of re-obtaining it to return its strongly-typed interface.
        internal abstract GrainReference GetNextBucket(out IIndexInterface<K, V> nextBucketIndexInterface);

        /// <summary>
        /// This method applies a given update to the current index.
        /// </summary>
        /// <param name="updatedGrain">the grain that issued the update</param>
        /// <param name="iUpdate">contains the data for the update</param>
        /// <param name="isUnique">whether this is a unique index that we are updating</param>
        /// <param name="idxMetaData">the index metadata</param>
        /// <param name="siloAddress">the silo address; unused here</param>
        /// <returns>true, if the index update was successful, otherwise false</returns>
        public async Task<bool> DirectApplyIndexUpdate(IIndexableGrain updatedGrain, Immutable<IMemberUpdate> iUpdate, bool isUnique, IndexMetaData idxMetaData, SiloAddress siloAddress)
        {
            if (this.IsTransactional)
            {
                await ApplyIndexUpdateTransactional(updatedGrain, iUpdate.Value, isUnique, idxMetaData, siloAddress);
                return true;
            }
            await DirectApplyIndexUpdateNonPersistent(updatedGrain, iUpdate.Value, isUnique, idxMetaData, siloAddress);
            await PersistIndexNonTransactional();
            return true;
        }

        private async Task DirectApplyIndexUpdateNonPersistent(IIndexableGrain g, IMemberUpdate updt, bool isUniqueIndex, IndexMetaData idxMetaData, SiloAddress siloAddress)
        {
            // The target grain that is updated
            V updatedGrain = g.AsReference<V>(this.SiloIndexManager);

            var nonTransactionalState = (NonTransactionalState<HashIndexBucketState<K, V>>)this.maybeTransactionalState;

            async Task<IIndexInterface<K, V>> getNextBucketAndPersist()
            {
                nonTransactionalState.State.NextBucket = GetNextBucket(out IIndexInterface<K, V> nextBucketIndexInterface);
                await PersistIndexNonTransactional();
                return nextBucketIndexInterface;
            }

            // UpdateBucketState is synchronous; note that no other thread can run concurrently before we reach an await operation,
            // when execution is yielded back to the Orleans scheduler, so no concurrency control mechanism (e.g., locking) is required.
            // 'fixIndexUnavailableOnDelete' indicates whether the index was still unavailable when we received a delete operation.
            if (!HashIndexBucketUtils.UpdateBucketState(updatedGrain, updt, nonTransactionalState.State, isUniqueIndex, idxMetaData, out bool fixIndexUnavailableOnDelete))
            {
                // TODO if the index was still unavailable when we received a delete operation
                //if (fixIndexUnavailableOnDelete) { /*create tombstone*/ }

                // Here we do an await, so we return to the scheduler.
                await (await getNextBucketAndPersist()).DirectApplyIndexUpdate(g, updt.AsImmutable(), isUniqueIndex, idxMetaData, siloAddress);
            }
        }

        private async Task ApplyIndexUpdateTransactional(IIndexableGrain g, IMemberUpdate updt, bool isUniqueIndex, IndexMetaData idxMetaData, SiloAddress siloAddress)
        {
            // The target grain that is updated
            V updatedGrain = g.AsReference<V>(this.SiloIndexManager);

            // TODO performance: If we already have the next bucket, we can pre-check to see if we are going to do any operation in the
            // bucket, and if not, avoid the PerformUpdate for this bucket. However, doing so would involve a PerformRead and then a
            // scheduling point, allowing another operation to change things.

            IIndexInterface<K, V> nextBucketIndexInterface = await this.WriteStateAsync(state =>
            {
                // UpdateBucketState is synchronous; note that no other thread can run concurrently before we reach an await operation,
                // when execution is yielded back to the Orleans scheduler, so no concurrency control mechanism (e.g., locking) is required.
                // 'fixIndexUnavailableOnDelete' indicates whether the index was still unavailable when we received a delete operation.
                if (!HashIndexBucketUtils.UpdateBucketState(updatedGrain, updt, state, isUniqueIndex, idxMetaData, out bool fixIndexUnavailableOnDelete))
                {
                    // TODO if the index was still unavailable when we received a delete operation
                    //if (fixIndexUnavailableOnDelete) { /*create tombstone*/ }

                    state.NextBucket = GetNextBucket(out IIndexInterface<K, V> nextBucketIndexItf);
                    return nextBucketIndexItf;
                }
                return default;
            });

            if (nextBucketIndexInterface != default)
            {
                await nextBucketIndexInterface.DirectApplyIndexUpdate(g, updt.AsImmutable(), isUniqueIndex, idxMetaData, siloAddress);
            }
        }

        /// <summary>
        /// Persists the state of a non-transactional index
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async Task PersistIndexNonTransactional()
        {
            //create a write-request ID, which is used for group commit
            int writeRequestId = ++this.writeRequestIdGen;

            //add the write-request ID to the pending write requests
            this.pendingWriteRequests.Add(writeRequestId);

            //wait before any previous write is done
            using (await this.write_lock.LockAsync())
            {
                // If the write request is not there, it was handled by another worker before we obtained the lock.
                if (this.pendingWriteRequests.Contains(writeRequestId))
                {
                    //clear all pending write requests, as this attempt will do them all.
                    this.pendingWriteRequests.Clear();

                    // Write the index state back to the storage. TODO: What is the best way to handle an index write error?
                    int numRetries = 0;
                    while (true)
                    {
                        try
                        {
                            await this.WriteStateAsync();
                            return;
                        }
                        catch when (numRetries < 3)
                        {
                            ++numRetries;
                            await Task.Delay(100);
                        }
                    }
                }
            }
        }
#endregion Reentrant Index Update

        private Exception LogException(string message, IndexingErrorCode errorCode)
        {
            var e = new Exception(message);
            this.Logger.Error(errorCode, message, e);
            return e;
        }

        public async Task LookupAsync(IOrleansQueryResultStream<V> result, K key)
        {
            this.Logger.Trace($"Streamed index lookup called for key = {key}");

            IIndexInterface<K, V> nextBucket = null;
            HashIndexSingleBucketEntry<V> entry = await this.ReadStateAsync(state =>
            {
                if (state.IndexStatus != IndexStatus.Available)
                {
                    throw LogException("Index is not still available", IndexingErrorCode.IndexingIndexIsNotReadyYet_GrainBucket1);
                }
                if (state.IndexMap.TryGetValue(key, out HashIndexSingleBucketEntry<V> foundEntry))
                {
                    return foundEntry;
                }
                if (state.NextBucket != null)
                {
                    this.GetNextBucket(out var nb);
                    nextBucket = nb;
                }
                return default;
            });

            if (entry != default)
            {
                if (!entry.IsTentative)
                {
                    await result.OnNextBatchAsync(entry.Values);
                }
                await result.OnCompletedAsync();
                return;
            }

            await (nextBucket != null ? nextBucket.LookupAsync(result, key) : result.OnCompletedAsync());
        }

        public async Task<V> LookupUniqueAsync(K key)
        {
            this.Logger.Trace($"Unique index lookup called for key = {key}");

            IIndexInterface<K, V> nextBucket = null;
            V result = await this.ReadStateAsync(state =>
            {
                if (state.IndexStatus != IndexStatus.Available)
                {
                    throw LogException("Index is not still available", IndexingErrorCode.IndexingIndexIsNotReadyYet_GrainBucket2);
                }
                if (state.IndexMap.TryGetValue(key, out HashIndexSingleBucketEntry<V> entry))
                {
                    return (entry.Values.Count == 1 && !entry.IsTentative)
                        ? entry.Values.GetEnumerator().Current
                        : throw LogException($"There are {entry.Values.Count} values for the unique lookup key \"{key}\" on index" +
                                             $" \"{IndexUtils.GetIndexNameFromIndexGrain(this)}\", and the entry is{(entry.IsTentative ? "" : " not")} tentative.",
                                            IndexingErrorCode.IndexingIndexIsNotReadyYet_GrainBucket3);
                }
                if (state.NextBucket != null)
                {
                    this.GetNextBucket(out var nb);
                    nextBucket = nb;
                    return default;
                }
                throw LogException($"The lookup key \"{key}\" does not exist on index \"{IndexUtils.GetIndexNameFromIndexGrain(this)}\".",
                                   IndexingErrorCode.IndexingIndexIsNotReadyYet_GrainBucket4);
            });
            return nextBucket == null ? result
                                      : (await (this.IsTransactional ? ((ITransactionalLookupIndex<K, V>)nextBucket).LookupTransactionalUniqueAsync(key)
                                                                     : ((IHashIndexInterface<K, V>)nextBucket).LookupUniqueAsync(key)));
        }

        public Task Dispose()
        {
            Debug.Assert(!this.IsTransactional, "Dispose should not be called on buckets of a Transactional index");
            this.WriteStateAsync(state =>
            {
                state.IndexStatus = IndexStatus.Disposed;
                state.IndexMap.Clear();
            });
            base.DeactivateOnIdle();
            return Task.CompletedTask;
        }

        public Task<bool> IsAvailable() => Task.FromResult(true); // TODO: add support for index construction: Task.FromResult(this.State.IndexStatus == IndexStatus.Available);

        Task IIndexInterface.LookupAsync(IOrleansQueryResultStream<IIndexableGrain> result, object key) => this.LookupAsync(result.Cast<V>(), (K)key);

        public async Task<IOrleansQueryResult<V>> LookupAsync(K key)
        {
            this.Logger.Trace($"Eager index lookup called for key = {key}");

            IIndexInterface<K, V> nextBucket = null;
            OrleansQueryResult<V> result = await this.ReadStateAsync(state =>
            {
                if (state.IndexStatus != IndexStatus.Available)
                {
                    throw LogException("Index is not still available.", IndexingErrorCode.IndexingIndexIsNotReadyYet_GrainBucket5);
                }
                if (state.IndexMap.TryGetValue(key, out HashIndexSingleBucketEntry<V> entry))
                {
                    return new OrleansQueryResult<V>(entry.IsTentative ? Enumerable.Empty<V>() : entry.Values);
                }
                if (state.NextBucket != null)
                {
                    this.GetNextBucket(out var nb);
                    nextBucket = nb;
                }
                return new OrleansQueryResult<V>(Enumerable.Empty<V>());
            });

            return nextBucket == null ? result
                                      : (await (this.IsTransactional ? ((ITransactionalLookupIndex<K, V>)nextBucket).LookupTransactionalAsync(key)
                                                                     : nextBucket.LookupAsync(key)));
        }

        async Task<IOrleansQueryResult<IIndexableGrain>> IIndexInterface.LookupAsync(object key) => await this.LookupAsync((K)key);

        #region ITransactionalLookupIndex<K,V>
        public Task LookupTransactionalAsync(IOrleansQueryResultStream<V> result, K key) => this.LookupAsync(result, key);
        public Task<IOrleansQueryResult<V>> LookupTransactionalAsync(K key) => this.LookupAsync(key);
        public Task LookupTransactionalAsync(IOrleansQueryResultStream<IIndexableGrain> result, object key) => ((IIndexInterface<K, V>)this).LookupAsync(result, key);
        public Task<IOrleansQueryResult<IIndexableGrain>> LookupTransactionalAsync(object key) => ((IIndexInterface<K, V>)this).LookupAsync(key);
        public Task<V> LookupTransactionalUniqueAsync(K key) => this.LookupUniqueAsync(key);
        #endregion ITransactionalLookupIndex<K,V>
    }
}
