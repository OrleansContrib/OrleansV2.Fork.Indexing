using System.Threading.Tasks;
using BenchmarkGrainInterfaces.Indexing;
using Orleans.Indexing;
using Orleans.Indexing.Facet;

namespace BenchmarkGrains.Indexing
{
    public class IndexingGrainTransactional_PK : IndexingGrainBase, IIndexingGrainTransactional_PK
    {
        public IndexingGrainTransactional_PK(
            [TransactionalIndexedState(IndexUtils.IndexedGrainStateName)]
            IIndexedState<IndexingState> indexedState)
            : base(indexedState) { }

        public Task WriteStateAsyncTxn() => base.WriteStateAsync();
    }
}
