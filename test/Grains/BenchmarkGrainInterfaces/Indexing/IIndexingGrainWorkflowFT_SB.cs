using Orleans;
using Orleans.Indexing;

namespace BenchmarkGrainInterfaces.Indexing
{
    public interface IIndexingGrainWorkflowFT_SB : IIndexableGrain<IndexingPropertiesWorkflowFT_SB>, IIndexingGrain, IGrainWithIntegerKey
    {
    }
}
