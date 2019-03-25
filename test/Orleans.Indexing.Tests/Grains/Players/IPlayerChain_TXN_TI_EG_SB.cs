namespace Orleans.Indexing.Tests
{
    public class PlayerChain1PropertiesTransactional : IPlayerProperties
    {
        [TotalIndex(IsEager = true, NullValue = "0")]
        public int Score { get; set; }
        
        [TotalIndex(TotalIndexType.HashIndexSingleBucket, IsEager = true, MaxEntriesPerBucket = 5)]
        public string Location { get; set; }
    }

    public interface IPlayerChain_TXN_TI_EG_SB : IPlayerGrainTransactional, IIndexableGrain<PlayerChain1PropertiesTransactional>
    {
    }
}
