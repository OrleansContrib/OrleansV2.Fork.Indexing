using System;
using Orleans.Indexing;

namespace Orleans.Indexing.Tests
{
    [Serializable]
    public class Player1PropertiesNonFaultTolerant : IPlayerProperties
    {
        public int Score { get; set; }

        [ActiveIndex(IsEager = true)]
        public string Location { get; set; }
    }

    public interface IPlayer1GrainNonFaultTolerant : IPlayerGrain, IIndexableGrain<Player1PropertiesNonFaultTolerant>
    {
    }
}
