namespace Orleans.Indexing.Tests
{
    public class Player5PropertiesNonFaultTolerant : IPlayerProperties
    {
        [Index(typeof(IActiveHashIndexSingleBucket<int, IPlayer5GrainNonFaultTolerant>), IsEager = true, IsUnique = false, NullValue = "0")]
        public int Score { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, IPlayer5GrainNonFaultTolerant>), IsEager = true, IsUnique = false)]
        public string Location { get; set; }
    }

    public interface IPlayer5GrainNonFaultTolerant : IPlayerGrain, IIndexableGrain<Player5PropertiesNonFaultTolerant>
    {
    }
}
