namespace Orleans.Indexing.Tests
{
    public class Player1PropertiesNonFaultTolerantLazy : IPlayerProperties
    {
        public int Score { get; set; }

        [ActiveIndex/*(IsEager = false)*/]
        public string Location { get; set; }
    }

    public interface IPlayer1GrainNonFaultTolerantLazy : IPlayerGrain, IIndexableGrain<Player1PropertiesNonFaultTolerantLazy>
    {
    }
}
