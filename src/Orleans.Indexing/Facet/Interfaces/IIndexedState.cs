using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.Transactions.Abstractions;

namespace Orleans.Indexing.Facet
{
    /// <summary>
    /// The base interface definition for a class that implements the indexing facet of a grain.
    /// </summary>
    /// <typeparam name="TGrainState">The state implementation class of a <see cref="Grain{TGrainState}"/>.</typeparam>
    public interface IIndexedState<TGrainState> where TGrainState : new()
    {
        /// <summary>
        /// Attaches an <see cref="ITransactionalState{TState}"/> instance from the Grain's constructor.
        /// For transactional indexes only.
        /// </summary>
        void Attach(ITransactionalState<IndexedGrainStateWrapper<TGrainState>> transactionalState);

        /// <summary>
        /// The <see cref="Grain.OnActivateAsync()"/> implementation must call this; in turn, this calls
        /// <paramref name="onGrainActivateFunc"/>, in which the grain implementation does any additional activation logic needed.
        /// </summary>
        /// <param name="grain">The grain to manage indexes for</param>
        /// <param name="onGrainActivateFunc">If <paramref name="grain"/> implements custom activation logic, it supplies
        ///     a lambda to do so here, or may simply pass "() => Task.CompletedTask". It is called in parallel with
        ///     inserting grain indexes into the silo index collections.</param>
        Task OnActivateAsync(Grain grain, Func<Task> onGrainActivateFunc);

        /// <summary>
        /// The <see cref="Grain.OnDeactivateAsync()"/> implementation must call this; in turn, this
        /// calls <paramref name="onGrainDeactivateFunc"/>, in which the grain implementation should
        /// do any additional deactivation logic, if desired.
        /// </summary>
        /// <param name="onGrainDeactivateFunc">If the grain implements custom deactivation logic, it supplies
        ///     a lambda to do so here, or may simply pass "() => Task.CompletedTask". It is called in parallel with
        ///     removing grain indexes from the silo index collections.</param>
        Task OnDeactivateAsync(Func<Task> onGrainDeactivateFunc);

        /// <summary>
        /// Reads the grain state, which resets the value of all indexed and non-indexed properties.
        /// </summary>
        Task<TResult> PerformRead<TResult>(Func<TGrainState, TResult> readFunction);

        /// <summary>
        /// Executes <paramref name="updateFunction"/> then writes the grain state and the index entries for all indexed interfaces
        /// defined on the grain.
        /// </summary>
        Task<TResult> PerformUpdate<TResult>(Func<TGrainState, TResult> updateFunction);

        /// <summary>
        /// This method returns the set of active workflow IDs for a fault-tolerant Total Index
        /// </summary>
        Task<Immutable<HashSet<Guid>>> GetActiveWorkflowIdsSet();

        /// <summary>
        /// This method removes a workflow ID from the list of active workflow IDs for a fault-tolerant Total Index
        /// </summary>
        Task RemoveFromActiveWorkflowIds(HashSet<Guid> removedWorkflowId);
    }
}
