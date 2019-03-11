namespace Orleans.Indexing.Tests
{
    public class PlayerChain1PropertiesTransactional : IPlayerProperties
    {
        [TotalIndex(IsEager = true, NullValue = "0")]
        public int Score { get; set; }
        
        [TotalIndex(TotalIndexType.HashIndexSingleBucket, IsEager = true, MaxEntriesPerBucket = 5)]
        public string Location { get; set; }
    }

    public interface IPlayerChain1GrainTransactional : IPlayerGrainTransactional, IIndexableGrain<PlayerChain1PropertiesTransactional>
    {
    }
}
