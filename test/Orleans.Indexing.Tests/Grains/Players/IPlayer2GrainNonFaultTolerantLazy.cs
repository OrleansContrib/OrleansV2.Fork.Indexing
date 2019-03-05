namespace Orleans.Indexing.Tests
{
    public class Player2PropertiesNonFaultTolerantLazy : IPlayerProperties
    {
        public int Score { get; set; }

        [ActiveIndex(ActiveIndexType.HashIndexPartitionedBySilo/*, IsEager = false*/)]
        public string Location { get; set; }
    }

    public interface IPlayer2GrainNonFaultTolerantLazy : IPlayerGrain, IIndexableGrain<Player2PropertiesNonFaultTolerantLazy>
    {
    }
}
