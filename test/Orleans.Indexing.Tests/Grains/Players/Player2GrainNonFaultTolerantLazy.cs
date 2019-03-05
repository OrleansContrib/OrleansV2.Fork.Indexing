using Orleans.Indexing.Facet;

namespace Orleans.Indexing.Tests
{
    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    public class Player2GrainNonFaultTolerantLazy : PlayerGrainNonFaultTolerant<PlayerGrainState>, IPlayer2GrainNonFaultTolerantLazy
    {
        public Player2GrainNonFaultTolerantLazy(
            [NonFaultTolerantWorkflowIndexedState(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<PlayerGrainState> indexedState)
            : base(indexedState) { }
    }
}
