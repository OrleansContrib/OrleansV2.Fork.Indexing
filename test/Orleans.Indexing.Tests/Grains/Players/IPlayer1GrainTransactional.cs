namespace Orleans.Indexing.Tests
{
    public class Player1PropertiesTransactional : IPlayerProperties
    {
        public int Score { get; set; }

        [TotalIndex(IsEager = true)]
        public string Location { get; set; }
    }

    public interface IPlayer1GrainTransactional : IPlayerGrainTransactional, IIndexableGrain<Player1PropertiesTransactional>
    {
    }
}
