namespace Orleans.Indexing.Tests
{
    public class Player5PropertiesNonFaultTolerantLazy : IPlayerProperties
    {
        [Index(typeof(IActiveHashIndexSingleBucket<int, IPlayer5GrainNonFaultTolerantLazy>)/*, IsEager = false*/, IsUnique = false, NullValue = "0")]
        public int Score { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, IPlayer5GrainNonFaultTolerantLazy>)/*, IsEager = false*/, IsUnique = false)]
        public string Location { get; set; }
    }

    public interface IPlayer5GrainNonFaultTolerantLazy : IPlayerGrain, IIndexableGrain<Player5PropertiesNonFaultTolerantLazy>
    {
    }
}
