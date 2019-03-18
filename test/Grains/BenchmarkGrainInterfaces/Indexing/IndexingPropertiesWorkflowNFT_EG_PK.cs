using Orleans.Indexing;

namespace BenchmarkGrainInterfaces.Indexing
{
    public class IndexingPropertiesWorkflowNFT_EG_PK
    {
        [TotalIndex(TotalIndexType.HashIndexPartitionedByKeyHash, IsEager = true, IsUnique = false, NullValue = IndexingBenchmarkConstants.NullIdString)]
        public int Id1 { get; set; }

        [TotalIndex(TotalIndexType.HashIndexPartitionedByKeyHash, IsEager = true, IsUnique = false, NullValue = IndexingBenchmarkConstants.NullIdString)]
        public int Id2 { get; set; }

        [TotalIndex(TotalIndexType.HashIndexPartitionedByKeyHash, IsEager = true, IsUnique = false, NullValue = IndexingBenchmarkConstants.NullIdString)]
        public int Id3 { get; set; }

        [TotalIndex(TotalIndexType.HashIndexPartitionedByKeyHash, IsEager = true, IsUnique = false, NullValue = IndexingBenchmarkConstants.NullIdString)]
        public int Id4 { get; set; }
    }
}
