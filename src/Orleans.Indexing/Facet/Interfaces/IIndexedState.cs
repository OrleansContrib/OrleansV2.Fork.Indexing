using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Concurrency;

namespace Orleans.Indexing.Facet
{
    /// <summary>
    /// The base interface definition for a class that implements the indexing facet of a grain.
    /// </summary>
    /// <typeparam name="TGrainState">The state implementation class of a <see cref="Grain{TGrainState}"/>.</typeparam>
    public interface IIndexedState<TGrainState> where TGrainState : new()
    {
        /// <summary>
        /// The persistent state of the grain; includes values for indexed and non-indexed properties.
        /// </summary>
        TGrainState State { get; }

        /// <summary>
        /// The <see cref="Grain.OnActivateAsync()"/> implementation must call this; in turn, this calls
        /// <paramref name="onGrainActivateFunc"/>, in which the grain implementation does any additional activation logic needed.
        /// </summary>
        /// <param name="grain">The grain to manage indexes for</param>
        /// <param name="onGrainActivateFunc">If <paramref name="grain"/> implements custom activation logic, it supplies
        ///     a lambda to do so here, or may simply pass "() => Task.CompletedTask". It is called in parallel with
        ///     inserting grain indexes into the silo index collections and later during <see cref="WriteAsync()"/>.</param>
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
        /// Reads current grain state from the storage provider. Erases any updates to indexed and non-indexed properties.
        /// </summary>
        Task ReadAsync();

        /// <summary>
        /// Coordinates the writing of all indexed interfaces defined on the grain. It will retrieve this from cached
        /// per-grain-class list of indexes and properties to do the mapping, and maps the State structure to the various
        /// TProperties structures. It includes the grain state update in the workflow appropriately.
        /// </summary>
        Task WriteAsync();

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
