using Orleans.Indexing.Facet;
using Orleans.Providers;

namespace Orleans.Indexing.Tests
{
#if ALLOW_FT_ACTIVE
    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    public class Player5Grain : PlayerGrainFaultTolerant<PlayerGrainState>, IPlayer5Grain
    {
        public Player5Grain(
            [FaultTolerantWorkflowIndexedState(IndexingTestConstants.GrainStore)]
            IIndexedState<PlayerGrainState> indexedState)
            : base(indexedState) { }
    }
#endif // ALLOW_FT_ACTIVE
}
