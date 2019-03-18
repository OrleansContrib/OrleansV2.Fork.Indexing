using System.Threading.Tasks;
using Orleans;
using System;

namespace BenchmarkGrainInterfaces.Indexing
{
    /// <summary>
    /// Interface for the stateless worker grain that dispatches grain requests
    /// </summary>
    public interface IIndexingRootGrain<TGrainInterface> : IGrainWithGuidKey where TGrainInterface : IIndexingGrain, IGrainWithIntegerKey
    {
        Task Start(int grainId, int numPropertiesPerGrain) ;
    }
}
