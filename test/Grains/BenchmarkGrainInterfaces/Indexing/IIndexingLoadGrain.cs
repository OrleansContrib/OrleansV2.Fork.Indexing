using System;
using System.Threading.Tasks;
using Orleans;

namespace BenchmarkGrainInterfaces.Indexing
{
    public interface IIndexingLoadGrain<TGrainInterface> : IGrainWithGuidKey where TGrainInterface : IIndexingGrain, IGrainWithIntegerKey
    {
        Task Generate(int run, int numGrainsPerRunner, int numPropertiesPerGrain, int concurrentGrainsPerRun);

        Task<IndexingReport> TryGetReport();
    }
}
