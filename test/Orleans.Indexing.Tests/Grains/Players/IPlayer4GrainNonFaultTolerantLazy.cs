namespace Orleans.Indexing.Tests
{
    public class Player4PropertiesNonFaultTolerantLazy : IPlayerProperties
    {
        public int Score { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, IPlayer4GrainNonFaultTolerantLazy>)/*, IsEager = false*/, IsUnique = true)]
        public string Location { get; set; }
    }

    public interface IPlayer4GrainNonFaultTolerantLazy : IPlayerGrain, IIndexableGrain<Player4PropertiesNonFaultTolerantLazy>
    {
    }
}
