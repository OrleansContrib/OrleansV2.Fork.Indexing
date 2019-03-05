namespace Orleans.Indexing.Tests
{
#if ALLOW_FT_ACTIVE
    public class Player5Properties : IPlayerProperties
    {
        [Index(typeof(IActiveHashIndexSingleBucket<int, IPlayer5Grain>)/*, IsEager = false*/, IsUnique = true, NullValue = "0")]
        public int Score { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, IPlayer5Grain>)/*, IsEager = false*/, IsUnique = true)]
        public string Location { get; set; }
    }

    public interface IPlayer5Grain : IPlayerGrain, IIndexableGrain<Player5Properties>   // TODO: Currently not used in any tests
    {
    }
#endif // ALLOW_FT_ACTIVE
}
