namespace Orleans.Indexing.Tests
{
#if ALLOW_FT_ACTIVE
    public class Player4Properties : IPlayerProperties
    {
        public int Score { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, IPlayer4Grain>)/*, IsEager = false*/, IsUnique = true)]
        public string Location { get; set; }
    }

    public interface IPlayer4Grain : IPlayerGrain, IIndexableGrain<Player4Properties>
    {
    }
#endif // ALLOW_FT_ACTIVE
}
