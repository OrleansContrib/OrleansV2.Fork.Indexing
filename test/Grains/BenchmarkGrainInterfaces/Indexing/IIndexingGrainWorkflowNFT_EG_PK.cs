using Orleans;
using Orleans.Indexing;

namespace BenchmarkGrainInterfaces.Indexing
{
    public interface IIndexingGrainWorkflowNFT_EG_PK : IIndexableGrain<IndexingPropertiesWorkflowNFT_EG_PK>, IIndexingGrain, IGrainWithIntegerKey
    {
    }
}
