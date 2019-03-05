namespace Orleans.Indexing.Tests
{
    public class Player1PropertiesNonFaultTolerant : IPlayerProperties
    {
        public int Score { get; set; }

        [ActiveIndex(IsEager = true)]
        public string Location { get; set; }
    }

    public interface IPlayer1GrainNonFaultTolerant : IPlayerGrain, IIndexableGrain<Player1PropertiesNonFaultTolerant>
    {
    }
}
