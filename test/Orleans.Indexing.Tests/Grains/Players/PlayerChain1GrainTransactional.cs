using Orleans.Indexing.Facet;
using Orleans.Transactions.Abstractions;

namespace Orleans.Indexing.Tests
{
    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    public class PlayerChain1GrainTransactional : PlayerGrainTransactional<PlayerGrainState>, IPlayerChain1GrainTransactional
    {
        public PlayerChain1GrainTransactional(
            [TransactionalIndexedState(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<PlayerGrainState> indexedState,
            [TransactionalState(IndexingTestConstants.TestGrainState, IndexingConstants.INDEXING_STORAGE_PROVIDER_NAME)]
            ITransactionalState<IndexedGrainStateWrapper<PlayerGrainState>> transactionalState)
            : base(indexedState, transactionalState) { }
    }
}
