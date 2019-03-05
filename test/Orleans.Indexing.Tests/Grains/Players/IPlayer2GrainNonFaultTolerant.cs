namespace Orleans.Indexing.Tests
{
    public class Player2PropertiesNonFaultTolerant : IPlayerProperties
    {
        public int Score { get; set; }

        [ActiveIndex(ActiveIndexType.HashIndexPartitionedBySilo, IsEager = true)]
        public string Location { get; set; }
    }

    public interface IPlayer2GrainNonFaultTolerant : IPlayerGrain, IIndexableGrain<Player2PropertiesNonFaultTolerant>
    {
    }
}
