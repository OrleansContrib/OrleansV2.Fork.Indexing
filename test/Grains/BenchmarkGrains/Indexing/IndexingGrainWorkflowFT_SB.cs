using BenchmarkGrainInterfaces.Indexing;
using Orleans.Indexing;
using Orleans.Indexing.Facet;

namespace BenchmarkGrains.Indexing
{
    public class IndexingGrainWorkflowFT_SB : IndexingGrainBase, IIndexingGrainWorkflowFT_SB
    {
        public IndexingGrainWorkflowFT_SB(
            [FaultTolerantWorkflowIndexedState(IndexUtils.IndexedGrainStateName)]
            IIndexedState<IndexingState> indexedState)
            : base(indexedState) { }
    }
}
