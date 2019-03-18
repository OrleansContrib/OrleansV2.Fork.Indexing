namespace BenchmarkGrainInterfaces.Indexing
{
    // Marker interface only; the Grain interface must carry the Transaction annotation
    public interface IIndexingGrainTransactional : IIndexingGrain
    {
    }
}
