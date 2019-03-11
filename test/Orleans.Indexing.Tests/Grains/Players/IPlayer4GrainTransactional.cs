namespace Orleans.Indexing.Tests
{
    public class Player4PropertiesTransactional : IPlayerProperties
    {
        public int Score { get; set; }

        [Index(typeof(TotalHashIndexPartitionedPerKey<string, IPlayer4GrainTransactional>), IsEager = true, IsUnique = true)]
        public string Location { get; set; }
    }

    public interface IPlayer4GrainTransactional : IPlayerGrain, IIndexableGrain<Player4PropertiesTransactional>
    {
    }
}
