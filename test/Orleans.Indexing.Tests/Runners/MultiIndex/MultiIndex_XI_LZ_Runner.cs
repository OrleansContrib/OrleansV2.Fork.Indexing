using Orleans.Indexing.Facet;
using Orleans.Providers;
using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    #region PartitionedPerKey
#if ALLOW_FT_ACTIVE
    public class FT_Props_UIUSNINS_XI_LZ_PK : ITestMultiIndexProperties
    {
        [Index(typeof(ActiveHashIndexPartitionedPerKey<int, IFT_Grain_UIUSNINS_XI_LZ_PK>), IsEager = false, IsUnique = true, NullValue = "0")]
        public int UniqueInt { get; set; }

        [Index(typeof(TotalHashIndexPartitionedPerKey<string, IFT_Grain_UIUSNINS_XI_LZ_PK>), IsEager = false, IsUnique = true)]
        public string UniqueString { get; set; }

        [Index(typeof(TotalHashIndexPartitionedPerKey<int, IFT_Grain_UIUSNINS_XI_LZ_PK>), IsEager = false, IsUnique = false, NullValue = "-1")]
        public int NonUniqueInt { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, IFT_Grain_UIUSNINS_XI_LZ_PK>), IsEager = false, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Props_UIUSNINS_XI_LZ_PK : ITestMultiIndexProperties
    {
        [Index(typeof(ActiveHashIndexPartitionedPerKey<int, INFT_Grain_UIUSNINS_XI_LZ_PK>), IsEager = false, IsUnique = true, NullValue = "-1")]
        public int UniqueInt { get; set; }

        [Index(typeof(TotalHashIndexPartitionedPerKey<string, INFT_Grain_UIUSNINS_XI_LZ_PK>), IsEager = false, IsUnique = true)]
        public string UniqueString { get; set; }

        [Index(typeof(TotalHashIndexPartitionedPerKey<int, INFT_Grain_UIUSNINS_XI_LZ_PK>), IsEager = false, IsUnique = false, NullValue = "0")]
        public int NonUniqueInt { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, INFT_Grain_UIUSNINS_XI_LZ_PK>), IsEager = false, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }

#if ALLOW_FT_ACTIVE
    public interface IFT_Grain_UIUSNINS_XI_LZ_PK : ITestMultiIndexGrain, IIndexableGrain<FT_Props_UIUSNINS_XI_LZ_PK>
    {
    }
#endif // ALLOW_FT_ACTIVE

    public interface INFT_Grain_UIUSNINS_XI_LZ_PK : ITestMultiIndexGrain, IIndexableGrain<NFT_Props_UIUSNINS_XI_LZ_PK>
    {
    }

#if ALLOW_FT_ACTIVE
    public class FT_Grain_UIUSNINS_XI_LZ_PK : TestMultiIndexGrainFaultTolerant<TestMultiIndexState>, IFT_Grain_UIUSNINS_XI_LZ_PK
    {
        public FT_Grain_UIUSNINS_XI_LZ_PK(
            [FaultTolerantWorkflowIndexedState(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<TestMultiIndexState> indexedState)
            : base(indexedState) { }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Grain_UIUSNINS_XI_LZ_PK : TestMultiIndexGrainNonFaultTolerant<TestMultiIndexState>, INFT_Grain_UIUSNINS_XI_LZ_PK
    {
        public NFT_Grain_UIUSNINS_XI_LZ_PK(
            [NonFaultTolerantWorkflowIndexedState(IndexingConstants.IndexedGrainStateName, IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<TestMultiIndexState> indexedState)
            : base(indexedState) { }
    }
    #endregion // PartitionedPerKey

    #region PartitionedPerSilo

    // None; Total indexes cannot be specified as partitioned per silo.

    #endregion // PartitionedPerSilo

    #region SingleBucket
#if ALLOW_FT_ACTIVE
    public class FT_Props_UIUSNINS_XI_LZ_SB : ITestMultiIndexProperties
    {
        [Index(typeof(IActiveHashIndexSingleBucket<int, IFT_Grain_UIUSNINS_XI_LZ_SB>), IsEager = false, IsUnique = true, NullValue = "0")]
        public int UniqueInt { get; set; }

        [Index(typeof(ITotalHashIndexSingleBucket<string, IFT_Grain_UIUSNINS_XI_LZ_SB>), IsEager = false, IsUnique = true)]
        public string UniqueString { get; set; }

        [Index(typeof(ITotalHashIndexSingleBucket<int, IFT_Grain_UIUSNINS_XI_LZ_SB>), IsEager = false, IsUnique = false, NullValue = "-1")]
        public int NonUniqueInt { get; set; }

        [Index(typeof(IActiveHashIndexSingleBucket<string, IFT_Grain_UIUSNINS_XI_LZ_SB>), IsEager = false, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Props_UIUSNINS_XI_LZ_SB : ITestMultiIndexProperties
    {
        [Index(typeof(IActiveHashIndexSingleBucket<int, INFT_Grain_UIUSNINS_XI_LZ_SB>), IsEager = false, IsUnique = true, NullValue = "-1")]
        public int UniqueInt { get; set; }

        [Index(typeof(ITotalHashIndexSingleBucket<string, INFT_Grain_UIUSNINS_XI_LZ_SB>), IsEager = false, IsUnique = true)]
        public string UniqueString { get; set; }

        [Index(typeof(ITotalHashIndexSingleBucket<int, INFT_Grain_UIUSNINS_XI_LZ_SB>), IsEager = false, IsUnique = false, NullValue = "0")]
        public int NonUniqueInt { get; set; }

        [Index(typeof(IActiveHashIndexSingleBucket<string, INFT_Grain_UIUSNINS_XI_LZ_SB>), IsEager = false, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }

#if ALLOW_FT_ACTIVE
    public interface IFT_Grain_UIUSNINS_XI_LZ_SB : ITestMultiIndexGrain, IIndexableGrain<FT_Props_UIUSNINS_XI_LZ_SB>
    {
    }
#endif // ALLOW_FT_ACTIVE

    public interface INFT_Grain_UIUSNINS_XI_LZ_SB : ITestMultiIndexGrain, IIndexableGrain<NFT_Props_UIUSNINS_XI_LZ_SB>
    {
    }

#if ALLOW_FT_ACTIVE
    public class FT_Grain_UIUSNINS_XI_LZ_SB : TestMultiIndexGrainFaultTolerant<TestMultiIndexState>, IFT_Grain_UIUSNINS_XI_LZ_SB
    {
        public FT_Grain_UIUSNINS_XI_LZ_SB(
            [FaultTolerantWorkflowIndexedState(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<TestMultiIndexState> indexedState)
            : base(indexedState) { }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Grain_UIUSNINS_XI_LZ_SB : TestMultiIndexGrainNonFaultTolerant<TestMultiIndexState>, INFT_Grain_UIUSNINS_XI_LZ_SB
    {
        public NFT_Grain_UIUSNINS_XI_LZ_SB(
            [NonFaultTolerantWorkflowIndexedState(IndexingConstants.IndexedGrainStateName, IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<TestMultiIndexState> indexedState)
            : base(indexedState) { }
    }
    #endregion // SingleBucket

    public abstract class MultiIndex_XI_LZ_Runner : IndexingTestRunnerBase
    {
        protected MultiIndex_XI_LZ_Runner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

#if ALLOW_FT_ACTIVE
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_FT_Grain_UIUSNINS_XI_LZ_PK()
        {
            await base.TestIndexesWithDeactivations<IFT_Grain_UIUSNINS_XI_LZ_PK, FT_Props_UIUSNINS_XI_LZ_PK>();
        }
#endif // ALLOW_FT_ACTIVE

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_UIUSNINS_XI_LZ_PK()
        {
            await base.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_XI_LZ_PK, NFT_Props_UIUSNINS_XI_LZ_PK>();
        }

#if ALLOW_FT_ACTIVE
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_FT_Grain_UIUSNINS_XI_LZ_SB()
        {
            await base.TestIndexesWithDeactivations<IFT_Grain_UIUSNINS_XI_LZ_SB, FT_Props_UIUSNINS_XI_LZ_SB>();
        }
#endif // ALLOW_FT_ACTIVE

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_UIUSNINS_XI_LZ_SB()
        {
            await base.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_XI_LZ_SB, NFT_Props_UIUSNINS_XI_LZ_SB>();
        }

        internal static Func<IndexingTestRunnerBase, int, Task>[] GetAllTestTasks()
        {
            return new Func<IndexingTestRunnerBase, int, Task>[]
            {
#if ALLOW_FT_ACTIVE
                (baseRunner, intAdjust) => baseRunner.TestIndexesWithDeactivations<IFT_Grain_UIUSNINS_XI_LZ_PK, FT_Props_UIUSNINS_XI_LZ_PK>(intAdjust),
#endif // ALLOW_FT_ACTIVE
                (baseRunner, intAdjust) => baseRunner.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_XI_LZ_PK, NFT_Props_UIUSNINS_XI_LZ_PK>(intAdjust),
#if ALLOW_FT_ACTIVE
                (baseRunner, intAdjust) => baseRunner.TestIndexesWithDeactivations<IFT_Grain_UIUSNINS_XI_LZ_SB, FT_Props_UIUSNINS_XI_LZ_SB>(intAdjust),
#endif // ALLOW_FT_ACTIVE
                (baseRunner, intAdjust) => baseRunner.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_XI_LZ_SB, NFT_Props_UIUSNINS_XI_LZ_SB>(intAdjust)
            };
        }
    }
}
