using BenchmarkGrainInterfaces.Indexing;
using Orleans.Indexing;
using Orleans.Indexing.Facet;

namespace BenchmarkGrains.Indexing
{
    public class IndexingGrainWorkflowNFT_LZ_SB : IndexingGrainBase, IIndexingGrainWorkflowNFT_LZ_SB
    {
        public IndexingGrainWorkflowNFT_LZ_SB(
            [NonFaultTolerantWorkflowIndexedState(IndexUtils.IndexedGrainStateName)]
        IIndexedState<IndexingState> indexedState)
            : base(indexedState) { }
    }
}
