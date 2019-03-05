namespace Orleans.Indexing.Tests
{
    public class Player4PropertiesNonFaultTolerant : IPlayerProperties
    {
        public int Score { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, IPlayer4GrainNonFaultTolerant>), IsEager = true, IsUnique = true)]
        public string Location { get; set; }
    }

    public interface IPlayer4GrainNonFaultTolerant : IPlayerGrain, IIndexableGrain<Player4PropertiesNonFaultTolerant>
    {
    }
}
