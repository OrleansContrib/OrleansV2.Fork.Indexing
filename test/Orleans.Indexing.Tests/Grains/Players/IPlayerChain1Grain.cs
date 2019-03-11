namespace Orleans.Indexing.Tests
{
    public class PlayerChain1Properties : IPlayerProperties
    {
        [Index(IsEager = true, NullValue = "0")]
        public int Score { get; set; }
        
        [TotalIndex(TotalIndexType.HashIndexSingleBucket, IsEager = true, MaxEntriesPerBucket = 5)]
        public string Location { get; set; }
    }

    public interface IPlayerChain1Grain : IPlayerGrain, IIndexableGrain<PlayerChain1Properties>
    {
    }
}
