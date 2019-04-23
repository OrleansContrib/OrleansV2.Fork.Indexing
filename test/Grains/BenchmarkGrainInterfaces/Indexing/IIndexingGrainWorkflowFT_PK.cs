using Orleans;
using Orleans.Indexing;

namespace BenchmarkGrainInterfaces.Indexing
{
    public interface IIndexingGrainWorkflowFT_PK : IIndexableGrain<IndexingPropertiesWorkflowFT_PK>, IIndexingGrain, IGrainWithIntegerKey
    {
    }
}
