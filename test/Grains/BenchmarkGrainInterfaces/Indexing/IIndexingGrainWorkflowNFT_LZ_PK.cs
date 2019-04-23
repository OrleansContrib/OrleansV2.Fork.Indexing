using Orleans;
using Orleans.Indexing;

namespace BenchmarkGrainInterfaces.Indexing
{
    public interface IIndexingGrainWorkflowNFT_LZ_PK : IIndexableGrain<IndexingPropertiesWorkflowNFT_LZ_PK>, IIndexingGrain, IGrainWithIntegerKey
    {
    }
}
