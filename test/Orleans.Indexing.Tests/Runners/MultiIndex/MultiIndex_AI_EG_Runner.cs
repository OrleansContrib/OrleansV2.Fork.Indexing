using Orleans.Indexing.Facet;
using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    #region PartitionedPerKey

    // NFT only; FT cannot be configured to be Eager.

    public class NFT_Props_UIUSNINS_AI_EG_PK : ITestMultiIndexProperties
    {
        [Index(typeof(ActiveHashIndexPartitionedPerKey<int, INFT_Grain_UIUSNINS_AI_EG_PK>), IsEager = true, IsUnique = true, NullValue = "-1")]
        public int UniqueInt { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, INFT_Grain_UIUSNINS_AI_EG_PK>), IsEager = true, IsUnique = true)]
        public string UniqueString { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<int, INFT_Grain_UIUSNINS_AI_EG_PK>), IsEager = true, IsUnique = false, NullValue = "0")]
        public int NonUniqueInt { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, INFT_Grain_UIUSNINS_AI_EG_PK>), IsEager = true, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }

    public interface INFT_Grain_UIUSNINS_AI_EG_PK : ITestMultiIndexGrain, IIndexableGrain<NFT_Props_UIUSNINS_AI_EG_PK>
    {
    }

    public class NFT_Grain_UIUSNINS_AI_EG_PK : TestMultiIndexGrainNonFaultTolerant<TestMultiIndexState>, INFT_Grain_UIUSNINS_AI_EG_PK
    {
        public NFT_Grain_UIUSNINS_AI_EG_PK(
            [NonFaultTolerantWorkflowIndexedState(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<TestMultiIndexState> indexedState)
            : base(indexedState) { }
    }
    #endregion // PartitionedPerKey

    #region PartitionedPerSilo

    // NFT only; FT cannot be configured to be Eager.

    public class NFT_Props_UIUSNINS_AI_EG_PS : ITestMultiIndexProperties
    {
        [Index(typeof(IActiveHashIndexPartitionedPerSilo<int, INFT_Grain_UIUSNINS_AI_EG_PS>), IsEager = true, IsUnique = false, NullValue = "-1")]  // PerSilo cannot be Unique
        public int UniqueInt { get; set; }

        [Index(typeof(IActiveHashIndexPartitionedPerSilo<string, INFT_Grain_UIUSNINS_AI_EG_PS>), IsEager = true, IsUnique = false)] // PerSilo cannot be Unique
        public string UniqueString { get; set; }

        [Index(typeof(IActiveHashIndexPartitionedPerSilo<int, INFT_Grain_UIUSNINS_AI_EG_PS>), IsEager = true, IsUnique = false, NullValue = "0")]
        public int NonUniqueInt { get; set; }

        [Index(typeof(IActiveHashIndexPartitionedPerSilo<string, INFT_Grain_UIUSNINS_AI_EG_PS>), IsEager = true, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }

    public interface INFT_Grain_UIUSNINS_AI_EG_PS : ITestMultiIndexGrain, IIndexableGrain<NFT_Props_UIUSNINS_AI_EG_PS>
    {
    }

    public class NFT_Grain_UIUSNINS_AI_EG_PS : TestMultiIndexGrainNonFaultTolerant<TestMultiIndexState>, INFT_Grain_UIUSNINS_AI_EG_PS
    {
        public NFT_Grain_UIUSNINS_AI_EG_PS(
            [NonFaultTolerantWorkflowIndexedState(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<TestMultiIndexState> indexedState)
            : base(indexedState) { }
    }
    #endregion // PartitionedPerSilo

    #region SingleBucket

    // NFT only; FT cannot be configured to be Eager.

    public class NFT_Props_UIUSNINS_AI_EG_SB : ITestMultiIndexProperties
    {
        [Index(typeof(IActiveHashIndexSingleBucket<int, INFT_Grain_UIUSNINS_AI_EG_SB>), IsEager = true, IsUnique = true, NullValue = "-1")]
        public int UniqueInt { get; set; }

        [Index(typeof(IActiveHashIndexSingleBucket<string, INFT_Grain_UIUSNINS_AI_EG_SB>), IsEager = true, IsUnique = true)]
        public string UniqueString { get; set; }

        [Index(typeof(IActiveHashIndexSingleBucket<int, INFT_Grain_UIUSNINS_AI_EG_SB>), IsEager = true, IsUnique = false, NullValue = "0")]
        public int NonUniqueInt { get; set; }

        [Index(typeof(IActiveHashIndexSingleBucket<string, INFT_Grain_UIUSNINS_AI_EG_SB>), IsEager = true, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }

    public interface INFT_Grain_UIUSNINS_AI_EG_SB : ITestMultiIndexGrain, IIndexableGrain<NFT_Props_UIUSNINS_AI_EG_SB>
    {
    }

    public class NFT_Grain_UIUSNINS_AI_EG_SB : TestMultiIndexGrainNonFaultTolerant<TestMultiIndexState>, INFT_Grain_UIUSNINS_AI_EG_SB
    {
        public NFT_Grain_UIUSNINS_AI_EG_SB(
            [NonFaultTolerantWorkflowIndexedState(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<TestMultiIndexState> indexedState)
            : base(indexedState) { }
    }
    #endregion // SingleBucket

    public abstract class MultiIndex_AI_EG_Runner : IndexingTestRunnerBase
    {
        protected MultiIndex_AI_EG_Runner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_UIUSNINS_AI_EG_PK()
        {
            await base.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_AI_EG_PK, NFT_Props_UIUSNINS_AI_EG_PK>();
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_UIUSNINS_AI_EG_PS()
        {
            await base.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_AI_EG_PS, NFT_Props_UIUSNINS_AI_EG_PS>();
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_UIUSNINS_AI_EG_SB()
        {
            await base.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_AI_EG_SB, NFT_Props_UIUSNINS_AI_EG_SB>();
        }

        internal static Func<IndexingTestRunnerBase, int, Task>[] GetAllTestTasks()
        {
            return new Func<IndexingTestRunnerBase, int, Task>[]
            {
                (baseRunner, intAdjust) => baseRunner.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_AI_EG_PK, NFT_Props_UIUSNINS_AI_EG_PK>(intAdjust),
                (baseRunner, intAdjust) => baseRunner.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_AI_EG_PS, NFT_Props_UIUSNINS_AI_EG_PS>(intAdjust),
                (baseRunner, intAdjust) => baseRunner.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_AI_EG_SB, NFT_Props_UIUSNINS_AI_EG_SB>(intAdjust)
            };
        }
    }
}
