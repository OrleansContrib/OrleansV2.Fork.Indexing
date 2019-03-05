using Orleans.Indexing.Facet;
using Orleans.Providers;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    #region PartitionedPerKey

    // NFT only; FT cannot be configured to be Eager.

    public class NFT_Props_UIUSNINS_DSMI_EG_PK : ITestMultiIndexProperties
    {
        [StorageManagedIndex(IsEager = true, IsUnique = true, NullValue = "-1")]
        public int UniqueInt { get; set; }

        [StorageManagedIndex(IsEager = true, IsUnique = true)]
        public string UniqueString { get; set; }

        [StorageManagedIndex(IsEager = true, IsUnique = false, NullValue = "0")]
        public int NonUniqueInt { get; set; }

        [StorageManagedIndex(IsEager = true, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }

    public interface INFT_Grain_UIUSNINS_DSMI_EG_PK : ITestMultiIndexGrain, IIndexableGrain<NFT_Props_UIUSNINS_DSMI_EG_PK>
    {
    }

    [StorageProvider(ProviderName = IndexingTestConstants.CosmosDBGrainStorage)]
    public class NFT_Grain_UIUSNINS_DSMI_EG_PK : TestMultiIndexGrainNonFaultTolerant<TestMultiIndexState>, INFT_Grain_UIUSNINS_DSMI_EG_PK
    {
        public NFT_Grain_UIUSNINS_DSMI_EG_PK(
            [NonFaultTolerantWorkflowIndexedState(IndexingTestConstants.CosmosDBGrainStorage)]
            IIndexedState<TestMultiIndexState> indexedState)
            : base(indexedState) { }
    }
    #endregion // PartitionedPerKey

    #region PartitionedPerSilo

    // NFT only; FT cannot be configured to be Eager.

    public class NFT_Props_UIUSNINS_DSMI_EG_PS : ITestMultiIndexProperties
    {
        [StorageManagedIndex(IsEager = true, IsUnique = true, NullValue = "-1")]
        public int UniqueInt { get; set; }

        [StorageManagedIndex(IsEager = true, IsUnique = true)]
        public string UniqueString { get; set; }

        [StorageManagedIndex(IsEager = true, IsUnique = false, NullValue = "0")]
        public int NonUniqueInt { get; set; }

        [StorageManagedIndex(IsEager = true, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }

    public interface INFT_Grain_UIUSNINS_DSMI_EG_PS : ITestMultiIndexGrain, IIndexableGrain<NFT_Props_UIUSNINS_DSMI_EG_PS>
    {
    }

    [StorageProvider(ProviderName = IndexingTestConstants.CosmosDBGrainStorage)]
    public class NFT_Grain_UIUSNINS_DSMI_EG_PS : TestMultiIndexGrainNonFaultTolerant<TestMultiIndexState>, INFT_Grain_UIUSNINS_DSMI_EG_PS
    {
        public NFT_Grain_UIUSNINS_DSMI_EG_PS(
            [NonFaultTolerantWorkflowIndexedState(IndexingTestConstants.CosmosDBGrainStorage)]
            IIndexedState<TestMultiIndexState> indexedState)
            : base(indexedState) { }
    }
    #endregion // PartitionedPerSilo

    #region SingleBucket

    // NFT only; FT cannot be configured to be Eager.

    public class NFT_Props_UIUSNINS_DSMI_EG_SB : ITestMultiIndexProperties
    {
        [StorageManagedIndex(IsEager = true, IsUnique = true, NullValue = "-1")]
        public int UniqueInt { get; set; }

        [StorageManagedIndex(IsEager = true, IsUnique = true)]
        public string UniqueString { get; set; }

        [StorageManagedIndex(IsEager = true, IsUnique = false, NullValue = "0")]
        public int NonUniqueInt { get; set; }

        [StorageManagedIndex(IsEager = true, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }

    public interface INFT_Grain_UIUSNINS_DSMI_EG_SB : ITestMultiIndexGrain, IIndexableGrain<NFT_Props_UIUSNINS_DSMI_EG_SB>
    {
    }

    [StorageProvider(ProviderName = IndexingTestConstants.CosmosDBGrainStorage)]
    public class NFT_Grain_UIUSNINS_DSMI_EG_SB : TestMultiIndexGrainNonFaultTolerant<TestMultiIndexState>, INFT_Grain_UIUSNINS_DSMI_EG_SB
    {
        public NFT_Grain_UIUSNINS_DSMI_EG_SB(
            [NonFaultTolerantWorkflowIndexedState(IndexingTestConstants.CosmosDBGrainStorage)]
            IIndexedState<TestMultiIndexState> indexedState)
            : base(indexedState) { }
    }
    #endregion // SingleBucket

    public abstract class MultiIndex_DSMI_EG_Runner : IndexingTestRunnerBase
    {
        protected MultiIndex_DSMI_EG_Runner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_UIUSNINS_DSMI_EG_PK()
        {
            await base.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_DSMI_EG_PK, NFT_Props_UIUSNINS_DSMI_EG_PK>();
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_UIUSNINS_DSMI_EG_PS()
        {
            await base.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_DSMI_EG_PS, NFT_Props_UIUSNINS_DSMI_EG_PS>();
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_UIUSNINS_DSMI_EG_SB()
        {
            await base.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_DSMI_EG_SB, NFT_Props_UIUSNINS_DSMI_EG_SB>();
        }
    }
}
