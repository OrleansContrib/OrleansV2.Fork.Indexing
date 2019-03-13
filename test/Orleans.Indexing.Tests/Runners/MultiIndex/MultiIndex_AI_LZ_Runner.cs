using Orleans.Indexing.Facet;
using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    #region PartitionedPerKey
#if ALLOW_FT_ACTIVE
    public class FT_Props_UIUSNINS_AI_LZ_PK : ITestMultiIndexProperties
    {
        [Index(typeof(ActiveHashIndexPartitionedPerKey<int, IFT_Grain_UIUSNINS_AI_LZ_PK>), IsEager = false, IsUnique = true, NullValue = "0")]
        public int UniqueInt { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, IFT_Grain_UIUSNINS_AI_LZ_PK>), IsEager = false, IsUnique = true)]
        public string UniqueString { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<int, IFT_Grain_UIUSNINS_AI_LZ_PK>), IsEager = false, IsUnique = false, NullValue = "-1")]
        public int NonUniqueInt { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, IFT_Grain_UIUSNINS_AI_LZ_PK>), IsEager = false, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }
#endif // ALLOW_FT_ACTIVE
    public class NFT_Props_UIUSNINS_AI_LZ_PK : ITestMultiIndexProperties
    {
        [Index(typeof(ActiveHashIndexPartitionedPerKey<int, INFT_Grain_UIUSNINS_AI_LZ_PK>), IsEager = false, IsUnique = true, NullValue = "-1")]
        public int UniqueInt { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, INFT_Grain_UIUSNINS_AI_LZ_PK>), IsEager = false, IsUnique = true)]
        public string UniqueString { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<int, INFT_Grain_UIUSNINS_AI_LZ_PK>), IsEager = false, IsUnique = false, NullValue = "0")]
        public int NonUniqueInt { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, INFT_Grain_UIUSNINS_AI_LZ_PK>), IsEager = false, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }

#if ALLOW_FT_ACTIVE
    public interface IFT_Grain_UIUSNINS_AI_LZ_PK : ITestMultiIndexGrain, IIndexableGrain<FT_Props_UIUSNINS_AI_LZ_PK>
    {
    }
#endif // ALLOW_FT_ACTIVE

    public interface INFT_Grain_UIUSNINS_AI_LZ_PK : ITestMultiIndexGrain, IIndexableGrain<NFT_Props_UIUSNINS_AI_LZ_PK>
    {
    }

#if ALLOW_FT_ACTIVE
    public class FT_Grain_UIUSNINS_AI_LZ_PK : TestMultiIndexGrainFaultTolerant<TestMultiIndexState>, IFT_Grain_UIUSNINS_AI_LZ_PK
    {
        public FT_Grain_UIUSNINS_AI_LZ_PK(
            [FaultTolerantWorkflowIndexedState(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<TestMultiIndexState> indexedState)
            : base(indexedState) { }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Grain_UIUSNINS_AI_LZ_PK : TestMultiIndexGrainNonFaultTolerant<TestMultiIndexState>, INFT_Grain_UIUSNINS_AI_LZ_PK
    {
        public NFT_Grain_UIUSNINS_AI_LZ_PK(
            [NonFaultTolerantWorkflowIndexedState(IndexingConstants.IndexedGrainStateName, IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<TestMultiIndexState> indexedState)
            : base(indexedState) { }
    }
    #endregion // PartitionedPerKey

    #region PartitionedPerSilo
#if ALLOW_FT_ACTIVE
    public class FT_Props_UIUSNINS_AI_LZ_PS : ITestMultiIndexProperties
    {
        [Index(typeof(IActiveHashIndexPartitionedPerSilo<int, IFT_Grain_UIUSNINS_AI_LZ_PS>), IsEager = false, IsUnique = false, NullValue = "0")]   // PerSilo cannot be Unique
        public int UniqueInt { get; set; }

        [Index(typeof(IActiveHashIndexPartitionedPerSilo<string, IFT_Grain_UIUSNINS_AI_LZ_PS>), IsEager = false, IsUnique = false)] // PerSilo cannot be Unique
        public string UniqueString { get; set; }

        [Index(typeof(IActiveHashIndexPartitionedPerSilo<int, IFT_Grain_UIUSNINS_AI_LZ_PS>), IsEager = false, IsUnique = false, NullValue = "-1")]
        public int NonUniqueInt { get; set; }

        [Index(typeof(IActiveHashIndexPartitionedPerSilo<string, IFT_Grain_UIUSNINS_AI_LZ_PS>), IsEager = false, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Props_UIUSNINS_AI_LZ_PS : ITestMultiIndexProperties
    {
        [Index(typeof(IActiveHashIndexPartitionedPerSilo<int, INFT_Grain_UIUSNINS_AI_LZ_PS>), IsEager = false, IsUnique = false, NullValue = "-1")] // PerSilo cannot be Unique
        public int UniqueInt { get; set; }

        [Index(typeof(IActiveHashIndexPartitionedPerSilo<string, INFT_Grain_UIUSNINS_AI_LZ_PS>), IsEager = false, IsUnique = false)]    // PerSilo cannot be Unique
        public string UniqueString { get; set; }

        [Index(typeof(IActiveHashIndexPartitionedPerSilo<int, INFT_Grain_UIUSNINS_AI_LZ_PS>), IsEager = false, IsUnique = false, NullValue = "0")]
        public int NonUniqueInt { get; set; }

        [Index(typeof(IActiveHashIndexPartitionedPerSilo<string, INFT_Grain_UIUSNINS_AI_LZ_PS>), IsEager = false, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }

#if ALLOW_FT_ACTIVE
    public interface IFT_Grain_UIUSNINS_AI_LZ_PS : ITestMultiIndexGrain, IIndexableGrain<FT_Props_UIUSNINS_AI_LZ_PS>
    {
    }
#endif // ALLOW_FT_ACTIVE

    public interface INFT_Grain_UIUSNINS_AI_LZ_PS : ITestMultiIndexGrain, IIndexableGrain<NFT_Props_UIUSNINS_AI_LZ_PS>
    {
    }

#if ALLOW_FT_ACTIVE
    public class FT_Grain_UIUSNINS_AI_LZ_PS : TestMultiIndexGrainFaultTolerant<TestMultiIndexState>, IFT_Grain_UIUSNINS_AI_LZ_PS
    {
        public FT_Grain_UIUSNINS_AI_LZ_PS(
            [FaultTolerantWorkflowIndexedState(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<TestMultiIndexState> indexedState)
            : base(indexedState) { }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Grain_UIUSNINS_AI_LZ_PS : TestMultiIndexGrainNonFaultTolerant<TestMultiIndexState>, INFT_Grain_UIUSNINS_AI_LZ_PS
    {
        public NFT_Grain_UIUSNINS_AI_LZ_PS(
            [NonFaultTolerantWorkflowIndexedState(IndexingConstants.IndexedGrainStateName, IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<TestMultiIndexState> indexedState)
            : base(indexedState) { }
    }
    #endregion // PartitionedPerSilo

    #region SingleBucket
#if ALLOW_FT_ACTIVE
    public class FT_Props_UIUSNINS_AI_LZ_SB : ITestMultiIndexProperties
    {
        [Index(typeof(IActiveHashIndexSingleBucket<int, IFT_Grain_UIUSNINS_AI_LZ_SB>), IsEager = false, IsUnique = true, NullValue = "0")]
        public int UniqueInt { get; set; }

        [Index(typeof(IActiveHashIndexSingleBucket<string, IFT_Grain_UIUSNINS_AI_LZ_SB>), IsEager = false, IsUnique = true)]
        public string UniqueString { get; set; }

        [Index(typeof(IActiveHashIndexSingleBucket<int, IFT_Grain_UIUSNINS_AI_LZ_SB>), IsEager = false, IsUnique = false, NullValue = "-1")]
        public int NonUniqueInt { get; set; }

        [Index(typeof(IActiveHashIndexSingleBucket<string, IFT_Grain_UIUSNINS_AI_LZ_SB>), IsEager = false, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Props_UIUSNINS_AI_LZ_SB : ITestMultiIndexProperties
    {
        [Index(typeof(IActiveHashIndexSingleBucket<int, INFT_Grain_UIUSNINS_AI_LZ_SB>), IsEager = false, IsUnique = true, NullValue = "-1")]
        public int UniqueInt { get; set; }

        [Index(typeof(IActiveHashIndexSingleBucket<string, INFT_Grain_UIUSNINS_AI_LZ_SB>), IsEager = false, IsUnique = true)]
        public string UniqueString { get; set; }

        [Index(typeof(IActiveHashIndexSingleBucket<int, INFT_Grain_UIUSNINS_AI_LZ_SB>), IsEager = false, IsUnique = false, NullValue = "0")]
        public int NonUniqueInt { get; set; }

        [Index(typeof(IActiveHashIndexSingleBucket<string, INFT_Grain_UIUSNINS_AI_LZ_SB>), IsEager = false, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }

#if ALLOW_FT_ACTIVE
    public interface IFT_Grain_UIUSNINS_AI_LZ_SB : ITestMultiIndexGrain, IIndexableGrain<FT_Props_UIUSNINS_AI_LZ_SB>
    {
    }
#endif // ALLOW_FT_ACTIVE

    public interface INFT_Grain_UIUSNINS_AI_LZ_SB : ITestMultiIndexGrain, IIndexableGrain<NFT_Props_UIUSNINS_AI_LZ_SB>
    {
    }

#if ALLOW_FT_ACTIVE
    public class FT_Grain_UIUSNINS_AI_LZ_SB : TestMultiIndexGrainFaultTolerant<TestMultiIndexState>, IFT_Grain_UIUSNINS_AI_LZ_SB
    {
        public FT_Grain_UIUSNINS_AI_LZ_SB(
            [FaultTolerantWorkflowIndexedState(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<TestMultiIndexState> indexedState)
            : base(indexedState) { }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Grain_UIUSNINS_AI_LZ_SB : TestMultiIndexGrainNonFaultTolerant<TestMultiIndexState>, INFT_Grain_UIUSNINS_AI_LZ_SB
    {
        public NFT_Grain_UIUSNINS_AI_LZ_SB(
            [NonFaultTolerantWorkflowIndexedState(IndexingConstants.IndexedGrainStateName, IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<TestMultiIndexState> indexedState)
            : base(indexedState) { }
    }
#endregion // SingleBucket

    public abstract class MultiIndex_AI_LZ_Runner: IndexingTestRunnerBase
    {
        protected MultiIndex_AI_LZ_Runner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

#if ALLOW_FT_ACTIVE
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_FT_Grain_UIUSNINS_AI_LZ_PK()
        {
            await base.TestIndexesWithDeactivations<IFT_Grain_UIUSNINS_AI_LZ_PK, FT_Props_UIUSNINS_AI_LZ_PK>();
        }
#endif // ALLOW_FT_ACTIVE

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_UIUSNINS_AI_LZ_PK()
        {
            await base.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_AI_LZ_PK, NFT_Props_UIUSNINS_AI_LZ_PK>();
        }

#if ALLOW_FT_ACTIVE
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_FT_Grain_UIUSNINS_AI_LZ_PS()
        {
            await base.TestIndexesWithDeactivations<IFT_Grain_UIUSNINS_AI_LZ_PS, FT_Props_UIUSNINS_AI_LZ_PS>();
        }
#endif // ALLOW_FT_ACTIVE

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_UIUSNINS_AI_LZ_PS()
        {
            await base.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_AI_LZ_PS, NFT_Props_UIUSNINS_AI_LZ_PS>();
        }

#if ALLOW_FT_ACTIVE
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_FT_Grain_UIUSNINS_AI_LZ_SB()
        {
            await base.TestIndexesWithDeactivations<IFT_Grain_UIUSNINS_AI_LZ_SB, FT_Props_UIUSNINS_AI_LZ_SB>();
        }
#endif // ALLOW_FT_ACTIVE

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_UIUSNINS_AI_LZ_SB()
        {
            await base.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_AI_LZ_SB, NFT_Props_UIUSNINS_AI_LZ_SB>();
        }

        internal static Func<IndexingTestRunnerBase, int, Task>[] GetAllTestTasks()
        {
            return new Func<IndexingTestRunnerBase, int, Task>[]
            {
#if ALLOW_FT_ACTIVE
                (baseRunner, intAdjust) => baseRunner.TestIndexesWithDeactivations<IFT_Grain_UIUSNINS_AI_LZ_PK, FT_Props_UIUSNINS_AI_LZ_PK>(intAdjust),
#endif // ALLOW_FT_ACTIVE
                (baseRunner, intAdjust) => baseRunner.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_AI_LZ_PK, NFT_Props_UIUSNINS_AI_LZ_PK>(intAdjust),
#if ALLOW_FT_ACTIVE
                (baseRunner, intAdjust) => baseRunner.TestIndexesWithDeactivations<IFT_Grain_UIUSNINS_AI_LZ_PS, FT_Props_UIUSNINS_AI_LZ_PS>(intAdjust),
#endif // ALLOW_FT_ACTIVE
                (baseRunner, intAdjust) => baseRunner.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_AI_LZ_PS, NFT_Props_UIUSNINS_AI_LZ_PS>(intAdjust),
#if ALLOW_FT_ACTIVE
                (baseRunner, intAdjust) => baseRunner.TestIndexesWithDeactivations<IFT_Grain_UIUSNINS_AI_LZ_SB, FT_Props_UIUSNINS_AI_LZ_SB>(intAdjust),
#endif // ALLOW_FT_ACTIVE
                (baseRunner, intAdjust) => baseRunner.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_AI_LZ_SB, NFT_Props_UIUSNINS_AI_LZ_SB>(intAdjust)
            };
        }
    }
}
