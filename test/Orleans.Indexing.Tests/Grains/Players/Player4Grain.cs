using Orleans.Indexing.Facet;
using Orleans.Providers;

namespace Orleans.Indexing.Tests
{
#if ALLOW_FT_ACTIVE
    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    public class Player4Grain : PlayerGrainFaultTolerant<PlayerGrainState>, IPlayer4Grain
    {
        public Player4Grain(
            [FaultTolerantWorkflowIndexedState(IndexingTestConstants.GrainStore)]
            IIndexedState<PlayerGrainState> indexedState)
            : base(indexedState) { }
    }
#endif // ALLOW_FT_ACTIVE
}
