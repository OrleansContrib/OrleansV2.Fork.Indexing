using Orleans;
using Orleans.Indexing;

namespace BenchmarkGrainInterfaces.Indexing
{
    public interface IIndexingGrainWorkflowNFT_LZ_SB : IIndexableGrain<IndexingPropertiesWorkflowNFT_LZ_SB>, IIndexingGrain, IGrainWithIntegerKey
    {
    }
}
