using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;

namespace Orleans.Indexing.Facet
{
    abstract class IndexedStateBase<TGrainState> : IIndexedState<TGrainState> where TGrainState : class, new()
    {
        private protected readonly IServiceProvider ServiceProvider;
        private protected readonly IIndexedStateConfiguration IndexedStateConfig;

        private protected Grain grain;
        private protected IIndexableGrain iIndexableGrain;

        private protected Func<Guid> getWorkflowIdFunc;

        private protected GrainIndexes _grainIndexes;
        private protected bool _hasAnyUniqueIndex;

        public IndexedStateBase(IServiceProvider sp, IIndexedStateConfiguration config)
        {
            this.ServiceProvider = sp;
            this.IndexedStateConfig = config;
        }

        // IndexManager (and therefore logger) cannot be set in ctor because Grain activation has not yet set base.Runtime.
        internal SiloIndexManager SiloIndexManager => IndexManager.GetSiloIndexManager(ref this.__siloIndexManager, this.ServiceProvider);
        private SiloIndexManager __siloIndexManager;

        private protected ILogger Logger => this.__logger ?? (this.__logger = this.SiloIndexManager.LoggerFactory.CreateLoggerWithFullCategoryName(this.GetType()));
        private ILogger __logger;

        private protected SiloAddress BaseSiloAddress => this.SiloIndexManager.SiloAddress;

        #region public API

        public virtual void Attach(ITransactionalState<IndexedGrainStateWrapper<TGrainState>> transactionalState)
            => throw new IndexOperationException("Only Transactional Indexed State can attach an external state implementation.");

        public abstract Task OnActivateAsync(Grain grain, Func<Task> onGrainActivateFunc);

        public abstract Task OnDeactivateAsync(Func<Task> onGrainDeactivateFunc);

        public abstract Task<TResult> PerformRead<TResult>(Func<TGrainState, TResult> readFunction);

        public abstract Task<TResult> PerformUpdate<TResult>(Func<TGrainState, TResult> updateFunction);

        #endregion public API

        private protected void Initialize(Grain grain)
        {
            if (this.grain != null) // Already called
            {
                return;
            }
            this.grain = grain;
            this.iIndexableGrain = this.grain.AsReference<IIndexableGrain>(this.SiloIndexManager);

            if (!GrainIndexes.CreateInstance(this.SiloIndexManager.IndexRegistry, this.grain.GetType(), out this._grainIndexes)
                || !this._grainIndexes.HasAnyIndexes)
            {
                throw new InvalidOperationException("IndexedState should not be used for a Grain class with no indexes");
            }
            this._hasAnyUniqueIndex = this._grainIndexes.HasAnyUniqueIndex;
        }

        /// <summary>
        /// After some changes were made to the grain, and the grain is in a consistent state, this method is called to update the 
        /// indexes defined on this grain type.
        /// </summary>
        /// <remarks>
        /// UpdateIndexes kicks off the sequence that eventually goes through virtual/overridden ApplyIndexUpdates, which in turn calls
        /// writeGrainStateFunc() appropriately to ensure that only the successfully persisted bits are indexed, and the indexes are updated
        /// concurrently while writeGrainStateFunc() is done.
        ///
        /// The only reason that this method can receive a negative result from a call to ApplyIndexUpdates is that the list of indexes
        /// might have changed. In this case, it updates the list of member update and tries again. In the case of a positive result
        /// from ApplyIndexUpdates, the list of before-images is replaced by the list of after-images.
        /// </remarks>
        /// <param name="updateReason">Determines whether this method is called upon activation, deactivation, or still-active state of this grain</param>
        /// <param name="onlyUpdateActiveIndexes">whether only active indexes should be updated</param>
        /// <param name="writeStateIfConstraintsAreNotViolated">whether to write back the state to the storage if no constraint is violated</param>
        private protected async Task<InterfaceToUpdatesMap> UpdateIndexes(IndexUpdateReason updateReason, bool onlyUpdateActiveIndexes, bool writeStateIfConstraintsAreNotViolated)
        {
            // A flag to determine whether only unique indexes were updated
            var onlyUniqueIndexesWereUpdated = this._hasAnyUniqueIndex;

            // Gather the dictionary of indexes to their corresponding updates, grouped by interface
            var interfaceToUpdatesMap = this.GenerateMemberUpdates(updateReason, onlyUpdateActiveIndexes,
                out var updateIndexesEagerly, ref onlyUniqueIndexesWereUpdated, out var numberOfUniqueIndexesUpdated);

            // Apply the updates to the indexes defined on this grain
            await this.ApplyIndexUpdates(interfaceToUpdatesMap, updateIndexesEagerly,
                onlyUniqueIndexesWereUpdated, numberOfUniqueIndexesUpdated, writeStateIfConstraintsAreNotViolated);
            return interfaceToUpdatesMap;
        }

        /// <summary>
        /// Applies a set of updates to the indexes defined on the grain
        /// </summary>
        /// <param name="interfaceToUpdatesMap">the dictionary of indexes to their corresponding updates</param>
        /// <param name="updateIndexesEagerly">whether indexes should be updated eagerly or lazily</param>
        /// <param name="onlyUniqueIndexesWereUpdated">a flag to determine whether only unique indexes were updated</param>
        /// <param name="numberOfUniqueIndexesUpdated">determine the number of updated unique indexes</param>
        /// <param name="writeStateIfConstraintsAreNotViolated">whether writing back
        ///             the state to the storage should be done if no constraint is violated</param>
        private protected abstract Task ApplyIndexUpdates(InterfaceToUpdatesMap interfaceToUpdatesMap,
                                                  bool updateIndexesEagerly, bool onlyUniqueIndexesWereUpdated,
                                                  int numberOfUniqueIndexesUpdated, bool writeStateIfConstraintsAreNotViolated);

        /// <summary>
        /// Eagerly Applies updates to the indexes defined on this grain
        /// </summary>
        /// <param name="interfaceToUpdatesMap">the dictionary of updates for each index of each interface</param>
        /// <param name="updateIndexTypes">indicates whether unique and/or non-unique indexes should be updated</param>
        /// <param name="isTentative">indicates whether updates to indexes should be tentatively done. That is, the update
        ///     won't be visible to readers, but prevents writers from overwriting them and violating constraints</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private protected Task ApplyIndexUpdatesEagerly(InterfaceToUpdatesMap interfaceToUpdatesMap,
                                                    UpdateIndexType updateIndexTypes, bool isTentative = false)
            => Task.WhenAll(interfaceToUpdatesMap.Select(kvp => this.ApplyIndexUpdatesEagerly(kvp.Key, kvp.Value, updateIndexTypes, isTentative)));

        /// <summary>
        /// Eagerly Applies updates to the indexes defined on this grain for a single grain interface type implemented by this grain
        /// </summary>
        /// <param name="grainInterfaceType">a single indexable grain interface type implemented by this grain</param>
        /// <param name="updates">the dictionary of updates for each index</param>
        /// <param name="updateIndexTypes">indicates whether unique and/or non-unique indexes should be updated</param>
        /// <param name="isTentative">indicates whether updates to indexes should be tentatively done. That is, the update
        ///     won't be visible to readers, but prevents writers from overwriting them and violating constraints</param>
        /// <returns></returns>
        private protected Task ApplyIndexUpdatesEagerly(Type grainInterfaceType, IReadOnlyDictionary<string, IMemberUpdate> updates,
                                                        UpdateIndexType updateIndexTypes, bool isTentative)
        {
            var indexInterfaces = this._grainIndexes[grainInterfaceType];
            IEnumerable<Task<bool>> getUpdateTasks()
            {
                foreach (var (indexName, mu) in updates.Where(kvp => kvp.Value.OperationType != IndexOperationType.None))
                {
                    var indexInfo = indexInterfaces.NamedIndexes[indexName];
                    if (updateIndexTypes.HasFlag(indexInfo.MetaData.IsUniqueIndex ? UpdateIndexType.Unique : UpdateIndexType.NonUnique))
                    {
                        // If the caller asks for the update to be tentative, then it will be wrapped inside a MemberUpdateTentative
                        var updateToIndex = isTentative ? new MemberUpdateWithMode(mu, IndexUpdateMode.Tentative) : mu;
                        yield return indexInfo.IndexInterface.ApplyIndexUpdate(this.SiloIndexManager,
                                             this.iIndexableGrain, updateToIndex.AsImmutable(), indexInfo.MetaData, this.BaseSiloAddress);
                    }
                }
            }

            // At the end, because the index update should be eager, we wait for all index update tasks to finish
            return Task.WhenAll(getUpdateTasks());
        }

        private protected void UpdateBeforeImages(InterfaceToUpdatesMap interfaceToUpdatesMap)
            => this._grainIndexes.UpdateBeforeImages(interfaceToUpdatesMap);

        private InterfaceToUpdatesMap GenerateMemberUpdates(IndexUpdateReason updateReason,
                                                            bool onlyUpdateActiveIndexes, out bool updateIndexesEagerly,
                                                            ref bool onlyUniqueIndexesWereUpdated, out int numberOfUniqueIndexesUpdated)
        {
            (string prevIndexName, var prevIndexIsEager) = (null, false);

            var numUniqueIndexes = 0;       // Local vars due to restrictions on local functions accessing ref/out params
            var onlyUniqueIndexes = true;

            IEnumerable<(string indexName, IMemberUpdate mu)> generateNamedMemberUpdates(Type interfaceType, InterfaceIndexes indexes)
            {
                var befImgs = indexes.BeforeImages.Value;
                foreach ((var indexName, var indexInfo) in indexes.NamedIndexes
                                                                  .Where(kvp => !onlyUpdateActiveIndexes || !kvp.Value.IndexInterface.IsTotalIndex())
                                                                  .Select(kvp => (kvp.Key, kvp.Value)))
                {
                    var mu = updateReason == IndexUpdateReason.OnActivate
                                            ? indexInfo.UpdateGenerator.CreateMemberUpdate(befImgs[indexName])
                                            : indexInfo.UpdateGenerator.CreateMemberUpdate(
                                                updateReason == IndexUpdateReason.OnDeactivate ? null : indexes.Properties, befImgs[indexName]);
                    if (mu.OperationType != IndexOperationType.None)
                    {
                        if (prevIndexName != null && prevIndexIsEager != indexInfo.MetaData.IsEager)
                        {
                            throw new InvalidOperationException($"Inconsistent index eagerness specification on grain implementation {this.GetType().Name}," +
                                                                $" interface {interfaceType.Name}, properties {indexes.PropertiesType.FullName}." +
                                                                $" Prior indexes (most recently {prevIndexName}) specified {prevIndexIsEager} while" +
                                                                $" index {indexName} specified {indexInfo.MetaData.IsEager}. This misconfiguration should have been detected on silo startup.");
                        }
                        (prevIndexName, prevIndexIsEager) = (indexName, indexInfo.MetaData.IsEager);

                        if (indexInfo.MetaData.IsUniqueIndex)
                        {
                            ++numUniqueIndexes;
                        }
                        else
                        {
                            onlyUniqueIndexes = false;
                        }
                        yield return (indexName, mu);
                    }
                }
            }

            var interfaceToUpdatesMap = new InterfaceToUpdatesMap(updateReason, this.getWorkflowIdFunc,
                                                                  this._grainIndexes.Select(kvp => (kvp.Key, generateNamedMemberUpdates(kvp.Key, kvp.Value))));
            updateIndexesEagerly = prevIndexName != null ? prevIndexIsEager : false;
            numberOfUniqueIndexesUpdated = numUniqueIndexes;
            onlyUniqueIndexesWereUpdated = onlyUniqueIndexes;
            return interfaceToUpdatesMap;
        }

        // IIndexableGrain methods; these are overridden only by FaultTolerantWorkflowIndexedState.  TODO move to FT only
        public virtual Task<Immutable<HashSet<Guid>>> GetActiveWorkflowIdsSet() => throw new NotImplementedException("GetActiveWorkflowIdsSet");
        public virtual Task RemoveFromActiveWorkflowIds(HashSet<Guid> removedWorkflowIds) => throw new NotImplementedException("RemoveFromActiveWorkflowIds");
    }
}
