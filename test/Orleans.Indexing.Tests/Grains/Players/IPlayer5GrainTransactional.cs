namespace Orleans.Indexing.Tests
{
    public class Player5PropertiesTransactional : IPlayerProperties
    {
        [Index(typeof(ITotalHashIndexSingleBucket<int, IPlayer5GrainTransactional>), IsEager = true, IsUnique = false, NullValue = "0")]
        public int Score { get; set; }

        [Index(typeof(TotalHashIndexPartitionedPerKey<string, IPlayer5GrainTransactional>), IsEager = true, IsUnique = false)]
        public string Location { get; set; }
    }

    public interface IPlayer5GrainTransactional : IPlayerGrainTransactional, IIndexableGrain<Player5PropertiesTransactional>
    {
    }
}
