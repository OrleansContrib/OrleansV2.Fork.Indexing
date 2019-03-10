namespace Orleans.Indexing.Tests
{
    public class Player3PropertiesTransactional : IPlayerProperties
    {
        public int Score { get; set; }

        [TotalIndex(TotalIndexType.HashIndexPartitionedByKeyHash, IsEager = true)]
        public string Location { get; set; }
    }

    public interface IPlayer3GrainTransactional : IPlayerGrainTransactional, IIndexableGrain<Player3PropertiesTransactional>
    {
    }
}
