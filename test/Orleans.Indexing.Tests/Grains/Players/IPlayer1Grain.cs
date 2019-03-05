namespace Orleans.Indexing.Tests
{
#if ALLOW_FT_ACTIVE
    public class Player1Properties : IPlayerProperties
    {
        public int Score { get; set; }

        [TotalIndex]
        public string Location { get; set; }
    }

    public interface IPlayer1Grain : IPlayerGrain, IIndexableGrain<Player1Properties>
    {
    }
#endif // ALLOW_FT_ACTIVE
}
