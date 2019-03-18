using BenchmarkGrainInterfaces.Indexing;
using Orleans.Indexing;
using Orleans.Indexing.Facet;

namespace BenchmarkGrains.Indexing
{
    public class IndexingGrainWorkflowNFT_EG_PK : IndexingGrainBase, IIndexingGrainWorkflowNFT_EG_PK
    {
        public IndexingGrainWorkflowNFT_EG_PK(
            [NonFaultTolerantWorkflowIndexedState(IndexUtils.IndexedGrainStateName)]
            IIndexedState<IndexingState> indexedState)
            : base(indexedState) { }
    }
}
