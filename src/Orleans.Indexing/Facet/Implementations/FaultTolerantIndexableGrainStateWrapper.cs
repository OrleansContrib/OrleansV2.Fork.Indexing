using System;
using System.Collections.Generic;
using System.Text;

namespace Orleans.Indexing.Facet
{
    /// <summary>
    /// A wrapper around a user-defined state, TState, which adds the necessary information for fault-tolerant indexing
    /// </summary>
    /// <typeparam name="TGrainState">the type of user state</typeparam>
    [Serializable]
    public class FaultTolerantIndexableGrainStateWrapper<TGrainState> : IndexableGrainStateWrapper<TGrainState>
    {
        /// <summary>
        /// Points to the in-flight indexing workflowsIds
        /// </summary>
        internal HashSet<Guid> ActiveWorkflowsSet = null;

        /// There's a fixed mapping (e.g., a hash function) from a GrainReference to an <see cref="IndexWorkflowQueueGrainService"/>
        /// instance. Each Indexable Grain Interface IG has a property workflowQueue whose value, [grain-interface-type-name + sequence number],
        /// identifies the <see cref="IndexWorkflowQueueGrainService"/> grain that processes index updates on IG's behalf.
        internal IDictionary<Type, IIndexWorkflowQueue> WorkflowQueues = null;
    }
}
