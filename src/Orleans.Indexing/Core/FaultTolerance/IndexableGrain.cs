using Orleans.Concurrency;
using System.Collections.Generic;
using System.Threading.Tasks;
using System;
using System.Reflection;
using System.Linq;

namespace Orleans.Indexing
{
    /// <summary>
    /// IndexableGrain class is the super-class of all fault-tolerant grains that need to have indexing capability.
    /// 
    /// To make a grain indexable, two steps should be taken:
    ///     1- the grain class should extend IndexableGrain
    ///     2- the grain class is responsible for calling UpdateIndexes whenever one or more indexes need to be updated
    ///        
    /// Fault tolerance can be an optional feature for indexing, i.e., IndexableGrain extends IndexableGrainNonFaultTolerant.
    /// By default, indexing is fault tolerant.
    /// 
    /// IndexableGrain creates a wrapper around the State class provided by the actual user-grain that extends it.
    /// It adds the following information to it:
    ///  - a list called activeWorkflowsList to the State, which points to the in-flight indexing workflowsIds.
    ///  - There's a fixed mapping (e.g., a hash function) from a GrainReference to a <see cref="IndexWorkflowQueueGrainService"/>
    ///    instance. Each IndexableGrain G has a property workflowQueue whose value, [grain-type-name + sequence number],
    ///    that identifies the <see cref="IndexWorkflowQueueGrainService"/> grain that processes index updates on G's behalf.
    /// </summary>
    public abstract class IndexableGrain<TState, TProperties> : IndexableGrainNonFaultTolerant<IndexableExtendedState<TState>, TProperties>, IIndexableGrainFaultTolerant
        where TProperties : new()
    {
        protected new TState State
        {
            get => base.State.UserState;
            set => base.State.UserState = value;
        }

        protected override TProperties Properties => DefaultCreatePropertiesFromState();

        internal override IDictionary<Type, IIndexWorkflowQueue> WorkflowQueues
        {
            get => base.State.WorkflowQueues;
            set => base.State.WorkflowQueues = value;
        }

        private bool HasAnyTotalIndex => GetHasAnyTotalIndex();
        private bool? __hasAnyTotalIndex = null;

        public override Task OnActivateAsync()
        {
            // If the list of active work-flows is null or empty we can assume that we did not contact any work-flow
            // queue before in a possible prior activation.
            if (base.State.ActiveWorkflowsSet == null || base.State.ActiveWorkflowsSet.Count() == 0)
            {
                this.WorkflowQueues = null;
                return base.OnActivateAsync();
            }

            // If there are some remaining active work-flows they should be handled first
            PruneWorkflowQueuesForMissingTypes();
            return HandleRemainingWorkflows().ContinueWith(t => Task.WhenAll(PruneActiveWorkflowsSetFromAlreadyHandledWorkflows(t.Result), base.OnActivateAsync()));
        }

        /// <summary>
        /// Applies a set of updates to the indexes defined on the grain
        /// </summary>
        /// <param name="updates">the dictionary of indexes to their corresponding updates</param>
        /// <param name="updateIndexesEagerly">whether indexes should be updated eagerly or lazily</param>
        /// <param name="onlyUniqueIndexesWereUpdated">a flag to determine whether only unique indexes were updated</param>
        /// <param name="numberOfUniqueIndexesUpdated">determine the number of updated unique indexes</param>
        /// <param name="writeStateIfConstraintsAreNotViolated">whether writing back
        ///     the state to the storage should be done if no constraint is violated</param>
        protected override async Task ApplyIndexUpdates(IDictionary<string, IMemberUpdate> updates,
                                                       bool updateIndexesEagerly,
                                                       bool onlyUniqueIndexesWereUpdated,
                                                       int numberOfUniqueIndexesUpdated,
                                                       bool writeStateIfConstraintsAreNotViolated)
        {
            if (this.HasAnyTotalIndex)
            {
                // If there is any update to the indexes we go ahead and update the indexes
                if (updates.Count() > 0)
                {
                    IList<Type> iGrainTypes = GetIIndexableGrainTypes();
                    var thisGrain = this.AsReference<IIndexableGrain>(this.SiloIndexManager);
                    Guid workflowId = GenerateUniqueWorkflowId();

                    if (updateIndexesEagerly)
                    {
                        throw new InvalidOperationException("Fault tolerant indexes cannot be updated eagerly. This misconfiguration should have been detected on silo startup." +
                                                            " Check ApplicationPartsIndexableGrainLoader for the reason.");
                    }

                    // Update the indexes lazily. This is the first step, because its workflow record should be persisted in the workflow-queue first.
                    // The reason for waiting here is to make sure that the workflow record in the workflow queue is correctly persisted.
                    await ApplyIndexUpdatesLazily(updates, iGrainTypes, thisGrain, workflowId);

                    // Apply any unique index updates eagerly.
                    if (numberOfUniqueIndexesUpdated > 0)
                    {
                        // If there is more than one unique index to update, then updates to the unique indexes should be tentative
                        // so they are not visible to readers before making sure that all uniqueness constraints are satisfied.
                        // UniquenessConstraintViolatedExceptions propagate; any tentative records will be removed by WorkflowQueueHandler.
                        await ApplyIndexUpdatesEagerly(iGrainTypes, thisGrain, updates, UpdateIndexType.Unique, updateIndexesTentatively: true);
                    }

                    // Finally, the grain state is persisted if requested.
                    if (writeStateIfConstraintsAreNotViolated)
                    {
                        // There is no constraint violation and the workflow ID can be a part of the list of active workflows.
                        // Here, we add the work-flow to the list of committed/in-flight work-flows
                        AddWorkflowIdToActiveWorkflows(workflowId);
                        await WriteBaseStateAsync();
                    }

                    // If everything was successful, the before images are updated
                    UpdateBeforeImages(updates);
                }
                // Otherwise if there is no update to the indexes, we should write back the state of the grain if requested
                else if (writeStateIfConstraintsAreNotViolated)
                {
                    await WriteBaseStateAsync();
                }
            }
            else // !this.HasAnyTotalIndex
            {
                await base.ApplyIndexUpdates(updates, updateIndexesEagerly, onlyUniqueIndexesWereUpdated, numberOfUniqueIndexesUpdated, writeStateIfConstraintsAreNotViolated);
            }
        }

        /// <summary>
        /// Handles the remaining work-flows of the grain 
        /// </summary>
        /// <returns>the actual list of work-flow record IDs that were available in the queue(s)</returns>
        private Task<IEnumerable<Guid>> HandleRemainingWorkflows()
        {
            // A copy of WorkflowQueues is required, because we want to iterate over it and add/remove elements from/to it.
            var copyOfWorkflowQueues = new Dictionary<Type, IIndexWorkflowQueue>(this.WorkflowQueues);
            var tasks = copyOfWorkflowQueues.Select(wfqEntry => HandleRemainingWorkflows(wfqEntry.Key, wfqEntry.Value));
            return Task.WhenAll(tasks).ContinueWith(t => t.Result.SelectMany(res => res));
        }

        /// <summary>
        /// Handles the remaining work-flows of a specific grain interface of the grain
        /// </summary>
        /// <param name="iGrainType">the grain interface type being indexed</param>
        /// <param name="oldWorkflowQ">the previous work-flow queue responsible for handling the updates</param>
        /// <returns>the actual list of work-flow record IDs that were available in this queue</returns>
        private async Task<IEnumerable<Guid>> HandleRemainingWorkflows(Type iGrainType, IIndexWorkflowQueue oldWorkflowQ)
        {
            // Keeps the reference to the reincarnated work-flow queue, if the original work-flow queue (GrainService) did not respond.
            IIndexWorkflowQueue reincarnatedOldWorkflowQ = null;

            // Keeps the list of work-flow records from the old work-flow queue.
            Immutable<List<IndexWorkflowRecord>> remainingWorkflows;

            // First, we remove the work-flow queue associated with iGrainType (i.e., oldWorkflowQ) so that another call to get the
            // workflow queue for iGrainType gets the new work-flow queue responsible for iGrainType (otherwise oldWorkflowQ is returned).
            this.WorkflowQueues.Remove(iGrainType);
            IIndexWorkflowQueue newWorkflowQ = GetWorkflowQueue(iGrainType);

            // If the same work-flow queue is responsible we just check what work-flow records are still in process
            if (newWorkflowQ.Equals(oldWorkflowQ))
            {
                remainingWorkflows = await oldWorkflowQ.GetRemainingWorkflowsIn(base.State.ActiveWorkflowsSet);
                if (remainingWorkflows.Value != null && remainingWorkflows.Value.Count() > 0)
                {
                    return remainingWorkflows.Value.Select(w => w.WorkflowId);
                }
            }
            else //the work-flow queue responsible for iGrainType has changed
            {
                try
                {
                    // We try to contact the original oldWorkflowQ to get the list of remaining work-flow records
                    // in order to pass their responsibility to newWorkflowQ.
                    remainingWorkflows = await oldWorkflowQ.GetRemainingWorkflowsIn(base.State.ActiveWorkflowsSet);
                }
                catch //the corresponding workflowQ is down, we should ask its reincarnated version
                {
                    // If anything bad happened, it means that oldWorkflowQ is not reachable.
                    // Then we get our hands to reincarnatedOldWorkflowQ to get the list of remaining work-flow records.
                    reincarnatedOldWorkflowQ = await GetReincarnatedWorkflowQueue(oldWorkflowQ);
                    remainingWorkflows = await reincarnatedOldWorkflowQ.GetRemainingWorkflowsIn(base.State.ActiveWorkflowsSet);
                }

                // If any work-flow is remaining unprocessed...
                if (remainingWorkflows.Value != null && remainingWorkflows.Value.Count() > 0)
                {
                    // Give the responsibility of handling the remaining workflow records to the newWorkflowQ.
                    await newWorkflowQ.AddAllToQueue(remainingWorkflows);

                    // Check which was the target old work-flow queue that responded to our request.
                    var targetOldWorkflowQueue = reincarnatedOldWorkflowQ ?? oldWorkflowQ;

                    // It's good that we remove the workflows from the queue, but we really don't have to wait for them.
                    // Worst-case, it will be processed again by the old-queue.
                    targetOldWorkflowQueue.RemoveAllFromQueue(remainingWorkflows).Ignore();
                    return remainingWorkflows.Value.Select(w => w.WorkflowId);
                }
            }
            // If there are no remaining work-flow records, an empty Enumerable is returned.
            return Enumerable.Empty<Guid>();
        }

        private async Task<IIndexWorkflowQueue> GetReincarnatedWorkflowQueue(IIndexWorkflowQueue workflowQ)
        {
            string primaryKey = workflowQ.GetPrimaryKeyString();
            IIndexWorkflowQueue reincarnatedQ = base.GrainFactory.GetGrain<IIndexWorkflowQueue>(primaryKey);
            IIndexWorkflowQueueHandler reincarnatedQHandler = base.GrainFactory.GetGrain<IIndexWorkflowQueueHandler>(primaryKey);
            await Task.WhenAll(reincarnatedQ.Initialize(workflowQ), reincarnatedQHandler.Initialize(workflowQ));
            return reincarnatedQ;
        }

        private Task PruneActiveWorkflowsSetFromAlreadyHandledWorkflows(IEnumerable<Guid> workflowsInProgress)
        {
            var initialSize = base.State.ActiveWorkflowsSet.Count();
            base.State.ActiveWorkflowsSet.Clear();
            foreach (Guid workflowId in workflowsInProgress)
            {
                base.State.ActiveWorkflowsSet.Add(workflowId);
            }
            return (base.State.ActiveWorkflowsSet.Count() != initialSize)
                ? WriteBaseStateAsync()
                : Task.CompletedTask;
        }

        private void PruneWorkflowQueuesForMissingTypes()
        {
            var oldQueues = this.WorkflowQueues;
            this.WorkflowQueues = new Dictionary<Type, IIndexWorkflowQueue>();
            IList<Type> iGrainTypes = GetIIndexableGrainTypes();
            foreach (var grainType in iGrainTypes)
            {
                if (oldQueues.TryGetValue(grainType, out IIndexWorkflowQueue q))
                {
                    this.WorkflowQueues.Add(grainType, q);
                }
            }
        }

        public override Task<Immutable<HashSet<Guid>>> GetActiveWorkflowIdsList()
        {
            var workflowIds = base.State.ActiveWorkflowsSet;

            // Immutable does not prevent items from being added to the hashset; there was a race condition where
            // IndexableGrain.ApplyIndexUpdates adds to the list after IndexWorkflowQueueHandlerBase.HandleWorkflowsUntilPunctuation
            // obtains grainsToActiveWorkflows and thus IndexWorkflowQueueHandlerBase.RemoveFromActiveWorkflowsInGrainsTasks
            // removes the added workflowId, which means that workflowId is not processed. Therefore deep-copy workflows.
            var result = (workflowIds == null) ? new HashSet<Guid>() : new HashSet<Guid>(workflowIds);
            return Task.FromResult(result.AsImmutable());
        }

        public override Task RemoveFromActiveWorkflowIds(HashSet<Guid> removedWorkflowIds)
        {
            if (base.State.ActiveWorkflowsSet != null && base.State.ActiveWorkflowsSet.RemoveWhere(g => removedWorkflowIds.Contains(g)) > 0)
            {
                // TODO: decide whether we need to actually write the state back to the storage or we can leave it for the next WriteStateAsync
                // on the grain itself.
                //return WriteBaseStateAsync();
                return Task.CompletedTask;
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// Adds a workflow ID to the list of active workflows
        /// for this fault-tolerant indexable grain
        /// </summary>
        /// <param name="workflowId">the workflow ID to be added</param>
        private void AddWorkflowIdToActiveWorkflows(Guid workflowId)
        {
            if (base.State.ActiveWorkflowsSet == null)
            {
                base.State.ActiveWorkflowsSet = new HashSet<Guid>();
            }
            base.State.ActiveWorkflowsSet.Add(workflowId);
        }

        /// <summary>
        /// Generates a unique Guid that does not exist in the list of active workflows.
        /// 
        /// Actually, there is a very unlikely possibility that we end up with a duplicate workflow ID in the following scenario:
        /// 1- IndexableGrain G is updated and assigned workflow ID = A
        /// 2- workflow record with ID = A is added to the index workflow queue
        /// 3- G fails and its state (including its active workflow list) is thrown away
        /// 4- G is re-activated and reads it state from storage (which does not include A in its active workflow list)
        /// 5- G gets updated and a new workflow with ID = A is generated for it.
        ///    This ID is assumed to be unique, while it actually is not unique and already exists in the workflow queue.
        /// 
        /// The only way to avoid it is using a centralized unique workflow ID generator, which can be added if necessary.
        /// </summary>
        /// <returns>a new unique workflow ID</returns>
        private Guid GenerateUniqueWorkflowId()
        {
            Guid workflowId = Guid.NewGuid();
            while (base.State.ActiveWorkflowsSet != null && base.State.ActiveWorkflowsSet.Contains(workflowId))
            {
                workflowId = Guid.NewGuid();
            }
            return workflowId;
        }

        private TProperties DefaultCreatePropertiesFromState()
        {
            if (typeof(TProperties).IsAssignableFrom(typeof(TState)))
            {
                return (TProperties)(object)(this.State);
            }

            // Copy named property values from this.State to _props. The set of property names will not change.
            if (this._props == null)
            {
                this._props = new TProperties();
            }
            foreach (PropertyInfo p in typeof(TProperties).GetProperties())
            {
                p.SetValue(this._props, typeof(TState).GetProperty(p.Name).GetValue(this.State));
            }
            return this._props;
        }

        private bool GetHasAnyTotalIndex()
        {
            if (!__hasAnyTotalIndex.HasValue)
            { 
                __hasAnyTotalIndex = this.GetIIndexableGrainTypes()
                                         .Any(iGrainType => base.SiloIndexManager.IndexFactory.GetGrainIndexes(iGrainType).HasAnyTotalIndex);
            }
            return __hasAnyTotalIndex.Value;
        }

        protected override void SetStateToNullValues()
        {
            IndexUtils.SetNullValues(base.State.UserState);
        }
    }

    /// <summary>
    /// IndexableExtendedState{TState} is a wrapper around a user-defined state, TState, which adds the necessary
    /// information for fault-tolerant indexing
    /// </summary>
    /// <typeparam name="TState">the type of user state</typeparam>
    [Serializable]
    public class IndexableExtendedState<TState>
    {
        internal HashSet<Guid> ActiveWorkflowsSet = null;
        internal IDictionary<Type, IIndexWorkflowQueue> WorkflowQueues = null;

        public TState UserState = (TState)Activator.CreateInstance(typeof(TState));
    }

    /// <summary>
    /// This stateless IndexableGrainNonFaultTolerant is the super class of all stateless indexable-grains.
    /// 
    /// Having a stateless fault-tolerant indexable-grain is meaningless,
    /// so it is the same as having a stateless non-fault-tolerant indexable grain
    /// </summary>
    public abstract class IndexableGrain<TProperties> : IndexableGrainNonFaultTolerant<TProperties> where TProperties : new()
    {
    }
}
