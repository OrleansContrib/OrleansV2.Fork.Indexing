using System.Threading.Tasks;
using Orleans;
using Orleans.Indexing;

namespace BenchmarkGrainInterfaces.Indexing
{
    public interface IIndexingGrainTransactional_SB : IIndexableGrain<IndexingPropertiesTransactional_SB>, IIndexingGrainTransactional, IGrainWithIntegerKey
    {
        // Must override to get the transactional annotation
        [Transaction(TransactionOption.CreateOrJoin)]
        Task WriteStateTxnAsync();
    }
}
