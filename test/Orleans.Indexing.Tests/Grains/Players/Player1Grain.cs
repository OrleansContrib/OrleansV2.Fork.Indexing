using Orleans.Indexing.Facet;
using Orleans.Providers;

namespace Orleans.Indexing.Tests
{
#if ALLOW_FT_ACTIVE
    /// <summary>
    /// A simple grain that represents a player in a game
    /// </summary>
    public class Player1Grain : PlayerGrainFaultTolerant<PlayerGrainState>, IPlayer1Grain
    {
        public Player1Grain(
            [FaultTolerantWorkflowIndexedState(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<PlayerGrainState> indexedState)
            : base(indexedState) { }
    }
#endif // ALLOW_FT_ACTIVE
}
