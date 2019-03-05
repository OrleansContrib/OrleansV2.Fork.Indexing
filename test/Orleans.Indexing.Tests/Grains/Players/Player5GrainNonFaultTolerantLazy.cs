using Orleans.Indexing.Facet;

namespace Orleans.Indexing.Tests
{
    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    public class Player5GrainNonFaultTolerantLazy : PlayerGrainNonFaultTolerant<PlayerGrainState>, IPlayer5GrainNonFaultTolerantLazy
    {
        public Player5GrainNonFaultTolerantLazy(
            [NonFaultTolerantWorkflowIndexedState(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<PlayerGrainState> indexedState)
            : base(indexedState) { }
    }
}
