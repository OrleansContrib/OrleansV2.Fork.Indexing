using Orleans.Indexing.Facet;
using Orleans.Providers;

namespace Orleans.Indexing.Tests
{
    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    public class Player3Grain : PlayerGrainFaultTolerant<PlayerGrainState>, IPlayer3Grain
    {
        public Player3Grain(
            [FaultTolerantWorkflowIndexedState(IndexingTestConstants.GrainStore)]
            IIndexedState<PlayerGrainState> indexedState)
            : base(indexedState) { }
    }
}
