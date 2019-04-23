using BenchmarkGrainInterfaces.Indexing;
using Orleans.Indexing;
using Orleans.Indexing.Facet;

namespace BenchmarkGrains.Indexing
{
    public class IndexingGrainWorkflowNFT_LZ_PK : IndexingGrainBase, IIndexingGrainWorkflowNFT_LZ_PK
    {
        public IndexingGrainWorkflowNFT_LZ_PK(
            [NonFaultTolerantWorkflowIndexedState(IndexUtils.IndexedGrainStateName)]
        IIndexedState<IndexingState> indexedState)
            : base(indexedState) { }
    }
}
