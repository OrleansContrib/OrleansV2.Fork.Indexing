using Orleans;
using Orleans.Indexing;

namespace BenchmarkGrainInterfaces.Indexing
{
    public interface IIndexingGrainWorkflowNFT_EG_SB : IIndexableGrain<IndexingPropertiesWorkflowNFT_EG_SB>, IIndexingGrain, IGrainWithIntegerKey
    {
    }
}
