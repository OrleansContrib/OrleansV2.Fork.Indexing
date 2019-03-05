using Orleans.Indexing.Facet;
using Orleans.Providers;

namespace Orleans.Indexing.Tests
{
#if ALLOW_FT_ACTIVE
    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    public class Player2Grain : PlayerGrainFaultTolerant<PlayerGrainState>, IPlayer2Grain
    {
        public Player2Grain(
            [FaultTolerantWorkflowIndexedState(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<PlayerGrainState> indexedState)
            : base(indexedState) { }
    }
#endif // ALLOW_FT_ACTIVE
}
