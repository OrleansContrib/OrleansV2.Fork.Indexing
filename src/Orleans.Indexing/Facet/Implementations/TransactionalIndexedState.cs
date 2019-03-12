using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;

namespace Orleans.Indexing.Facet
{
    class TransactionalIndexedState<TGrainState> : IndexedStateBase<TGrainState>,
                                                   ITransactionalIndexedState<TGrainState> where TGrainState : class, new()
    {
        ITransactionalState<IndexedGrainStateWrapper<TGrainState>> transactionalState;

        public TransactionalIndexedState(
                IServiceProvider sp,
                IIndexedStateConfiguration config
            ) : base(sp, config)
        {
        }

        #region public API

        public override void Attach(ITransactionalState<IndexedGrainStateWrapper<TGrainState>> transactionalState)
            => this.transactionalState = transactionalState;

        public override Task OnActivateAsync(Grain grain, Func<Task> onGrainActivateFunc)
        {
            base.Logger.Trace($"Activating indexable grain of type {grain.GetType().Name} in silo {this.SiloIndexManager.SiloAddress}.");
            if (this.transactionalState == null)
            {
                throw new IndexOperationException("Transactional Indexed State requires calling Attach() with an additional ITransactionalState<> facet on the grain's constructor.");
            }

            // Our state is "created" via Attach(). State initialization is deferred as we must be in a transaction context to access it.
            base.Initialize(grain);

            // Transactional indexes cannot be active and thus do not call InsertIntoActiveIndexes or RemoveFromActiveIndexes.
            return onGrainActivateFunc();
        }

        public override Task OnDeactivateAsync(Func<Task> onGrainDeactivateFunc)
        {
            base.Logger.Trace($"Deactivating indexable grain of type {this.grain.GetType().Name} in silo {this.SiloIndexManager.SiloAddress}.");

            // Transactional indexes cannot be active and thus do not call InsertIntoActiveIndexes or RemoveFromActiveIndexes.
            return onGrainDeactivateFunc();
        }

        public override Task<TResult> PerformRead<TResult>(Func<TGrainState, TResult> readFunction)
        {
            return this.transactionalState.PerformRead(wrappedState =>
            {
                this.EnsureStateInitialized(wrappedState, forUpdate:false);
                return readFunction(wrappedState.UserState);
            });
        }

        public async override Task<TResult> PerformUpdate<TResult>(Func<TGrainState, TResult> updateFunction)
        {
            // TransactionalState does the grain-state write here as well as the update, then we do athe index updates.
            var result = await this.transactionalState.PerformUpdate(wrappedState =>
            {
                this.EnsureStateInitialized(wrappedState, forUpdate:true);
                var res = updateFunction(wrappedState.UserState);

                // The property values here are ephemeral; they are re-initialized by UpdateBeforeImages in EnsureStateInitialized.
                this._grainIndexes.MapStateToProperties(wrappedState.UserState);
                return res;
            });

            var interfaceToUpdatesMap = await base.UpdateIndexes(IndexUpdateReason.WriteState, onlyUpdateActiveIndexes: false, writeStateIfConstraintsAreNotViolated: true);
            // BeforeImage update is deferred, so we don't have potentially stale values if the transaction is rolled back, e.g. if a different grain's update fails

            return result;
        }

        #endregion public API

        void EnsureStateInitialized(IndexedGrainStateWrapper<TGrainState> wrappedState, bool forUpdate)
        {
            // State initialization is deferred as we must be in a transaction context to access it.
            wrappedState.EnsureNullValues(base._grainIndexes.PropertyNullValues);
            if (forUpdate)
            {
                // Apply the deferred BeforeImage update.
                _grainIndexes.UpdateBeforeImages(wrappedState.UserState, force:true);
            }
        }

        /// <summary>
        /// Applies a set of updates to the indexes defined on the grain
        /// </summary>
        /// <param name="interfaceToUpdatesMap">the dictionary of indexes to their corresponding updates</param>
        /// <param name="updateIndexesEagerly">whether indexes should be updated eagerly or lazily; must always be true for transactional indexes</param>
        /// <param name="onlyUniqueIndexesWereUpdated">a flag to determine whether only unique indexes were updated; unused for transactional indexes</param>
        /// <param name="numberOfUniqueIndexesUpdated">determine the number of updated unique indexes; unused for transactional indexes</param>
        /// <param name="writeStateIfConstraintsAreNotViolated">whether the state should be written to storage if no constraint is violated;
        ///                                                     must always be true for transactional indexes</param>
        private protected override async Task ApplyIndexUpdates(InterfaceToUpdatesMap interfaceToUpdatesMap,
                                                                bool updateIndexesEagerly,
                                                                bool onlyUniqueIndexesWereUpdated,
                                                                int numberOfUniqueIndexesUpdated,
                                                                bool writeStateIfConstraintsAreNotViolated)
        {
            Debug.Assert(writeStateIfConstraintsAreNotViolated, "Transactional index writes must only be called when updating the grain state (not on activation change).");

            // For Transactional, the grain-state write has already been done by the time we get here.
            if (!interfaceToUpdatesMap.IsEmpty)
            {
                Debug.Assert(updateIndexesEagerly, "Transactional indexes cannot be configured to be lazy; this misconfiguration should have been caught in ValidateSingleIndex.");
                IEnumerable<Task> applyUpdates(Type grainInterfaceType, IReadOnlyDictionary<string, IMemberUpdate> updates)
                {
                    var indexInterfaces = this._grainIndexes[grainInterfaceType];
                    foreach (var (indexName, mu) in updates.Where(kvp => kvp.Value.OperationType != IndexOperationType.None))
                    {
                        var indexInfo = indexInterfaces.NamedIndexes[indexName];
                        var updateToIndex = new MemberUpdateOverriddenMode(mu, IndexUpdateMode.Transactional) as IMemberUpdate;
                        yield return indexInfo.IndexInterface.ApplyIndexUpdate(this.SiloIndexManager,
                                                this.iIndexableGrain, updateToIndex.AsImmutable(), indexInfo.MetaData, base.BaseSiloAddress);
                    }
                }

                await Task.WhenAll(interfaceToUpdatesMap.SelectMany(kvp => applyUpdates(kvp.Key, kvp.Value)));
            }
        }
    }
}
