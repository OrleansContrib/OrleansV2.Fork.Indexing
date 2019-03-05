using Orleans.Indexing.Facet;

namespace Orleans.Indexing.Tests
{
    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    public class Player4GrainNonFaultTolerantLazy : PlayerGrainNonFaultTolerant<PlayerGrainState>, IPlayer4GrainNonFaultTolerantLazy
    {
        public Player4GrainNonFaultTolerantLazy(
            [NonFaultTolerantWorkflowIndexedState(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<PlayerGrainState> indexedState)
            : base(indexedState) { }
    }
}
