using System.Threading.Tasks;
using BenchmarkGrainInterfaces.Indexing;
using Orleans.Indexing;
using Orleans.Indexing.Facet;

namespace BenchmarkGrains.Indexing
{
    public class IndexingGrainTransactional_SB : IndexingGrainBase, IIndexingGrainTransactional_SB
    {
        public IndexingGrainTransactional_SB(
            [TransactionalIndexedState(IndexUtils.IndexedGrainStateName)]
            IIndexedState<IndexingState> indexedState)
            : base(indexedState) { }

        public Task WriteStateAsyncTxn() => base.WriteStateAsync();
    }
}
