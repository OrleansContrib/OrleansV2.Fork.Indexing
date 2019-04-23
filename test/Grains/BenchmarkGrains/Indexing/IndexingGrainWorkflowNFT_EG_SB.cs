using BenchmarkGrainInterfaces.Indexing;
using Orleans.Indexing;
using Orleans.Indexing.Facet;

namespace BenchmarkGrains.Indexing
{
    public class IndexingGrainWorkflowNFT_EG_SB : IndexingGrainBase, IIndexingGrainWorkflowNFT_EG_SB
    {
        public IndexingGrainWorkflowNFT_EG_SB(
            [NonFaultTolerantWorkflowIndexedState(IndexUtils.IndexedGrainStateName)]
            IIndexedState<IndexingState> indexedState)
            : base(indexedState) { }
    }
}
