using BenchmarkGrainInterfaces.Indexing;
using Orleans.Indexing;
using Orleans.Indexing.Facet;

namespace BenchmarkGrains.Indexing
{
    public class IndexingGrainWorkflowFT_PK : IndexingGrainBase, IIndexingGrainWorkflowFT_PK
    {
        public IndexingGrainWorkflowFT_PK(
            [FaultTolerantWorkflowIndexedState(IndexUtils.IndexedGrainStateName)]
            IIndexedState<IndexingState> indexedState)
            : base(indexedState) { }
    }
}
