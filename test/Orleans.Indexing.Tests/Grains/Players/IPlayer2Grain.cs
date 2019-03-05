namespace Orleans.Indexing.Tests
{
#if ALLOW_FT_ACTIVE
    public class Player2Properties : IPlayerProperties
    {
        public int Score { get; set; }

        [ActiveIndex(ActiveIndexType.HashIndexPartitionedBySilo)]
        public string Location { get; set; }
    }

    public interface IPlayer2Grain : IPlayerGrain, IIndexableGrain<Player2Properties>
    {
    }
#endif // ALLOW_FT_ACTIVE
}
