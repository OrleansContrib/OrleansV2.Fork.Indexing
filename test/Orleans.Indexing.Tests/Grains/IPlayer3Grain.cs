using System;
using Orleans.Indexing;

namespace Orleans.Indexing.Tests
{
    [Serializable]
    public class Player3Properties : IPlayerProperties
    {
        public int Score { get; set; }

        [TotalIndex(TotalIndexType.HashIndexPartitionedByKeyHash)]
        public string Location { get; set; }
    }

    public interface IPlayer3Grain : IPlayerGrain, IIndexableGrain<Player3Properties>
    {
    }
}
