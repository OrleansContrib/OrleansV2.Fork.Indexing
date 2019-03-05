using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Orleans.Indexing.Facet;
using System.Collections.Generic;

namespace Orleans.Indexing.Tests.MultiInterface
{
    #region PartitionedPerKey

#if ALLOW_FT_ACTIVE
    public class FT_Props_Person_AI_LZ_PK : IPersonProperties
    {
        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, IFT_Grain_Person_AI_LZ_PK>), IsEager = false, IsUnique = true)]
        public string Name { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<int, IFT_Grain_Person_AI_LZ_PK>), IsEager = false, IsUnique = false, NullValue = "0")]
        public int Age { get; set; }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Props_Person_AI_LZ_PK : IPersonProperties
    {
        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, INFT_Grain_Person_AI_LZ_PK>), IsEager = false, IsUnique = true)]
        public string Name { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<int, INFT_Grain_Person_AI_LZ_PK>), IsEager = false, IsUnique = false, NullValue = "0")]
        public int Age { get; set; }
    }

#if ALLOW_FT_ACTIVE
    public class FT_Props_Job_AI_LZ_PK : IJobProperties
    {
        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, IFT_Grain_Job_AI_LZ_PK>), IsEager = false, IsUnique = true)]
        public string Title { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, IFT_Grain_Job_AI_LZ_PK>), IsEager = false, IsUnique = false)]
        public string Department { get; set; }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Props_Job_AI_LZ_PK : IJobProperties
    {
        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, INFT_Grain_Job_AI_LZ_PK>), IsEager = false, IsUnique = true)]
        public string Title { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, INFT_Grain_Job_AI_LZ_PK>), IsEager = false, IsUnique = false)]
        public string Department { get; set; }
    }

#if ALLOW_FT_ACTIVE
    public class FT_Props_Employee_AI_LZ_PK : IEmployeeProperties
    {
        [Index(typeof(ActiveHashIndexPartitionedPerKey<int, IFT_Grain_Employee_AI_LZ_PK>), IsEager = false, IsUnique = true, NullValue = "-1")]
        public int EmployeeId { get; set; }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Props_Employee_AI_LZ_PK : IEmployeeProperties
    {
        [Index(typeof(ActiveHashIndexPartitionedPerKey<int, INFT_Grain_Employee_AI_LZ_PK>), IsEager = false, IsUnique = true, NullValue = "-1")]
        public int EmployeeId { get; set; }
    }

#if ALLOW_FT_ACTIVE
    public interface IFT_Grain_Person_AI_LZ_PK : IIndexableGrain<FT_Props_Person_AI_LZ_PK>, IPersonGrain, IGrainWithIntegerKey
    {
    }
#endif // ALLOW_FT_ACTIVE

    public interface INFT_Grain_Person_AI_LZ_PK : IIndexableGrain<NFT_Props_Person_AI_LZ_PK>, IPersonGrain, IGrainWithIntegerKey
    {
    }

#if ALLOW_FT_ACTIVE
    public interface IFT_Grain_Job_AI_LZ_PK : IIndexableGrain<FT_Props_Job_AI_LZ_PK>, IJobGrain, IGrainWithIntegerKey
    {
    }
#endif // ALLOW_FT_ACTIVE

    public interface INFT_Grain_Job_AI_LZ_PK : IIndexableGrain<NFT_Props_Job_AI_LZ_PK>, IJobGrain, IGrainWithIntegerKey
    {
    }

#if ALLOW_FT_ACTIVE
    public interface IFT_Grain_Employee_AI_LZ_PK : IIndexableGrain<FT_Props_Employee_AI_LZ_PK>, IEmployeeGrain, IGrainWithIntegerKey
    {
    }
#endif // ALLOW_FT_ACTIVE

    public interface INFT_Grain_Employee_AI_LZ_PK : IIndexableGrain<NFT_Props_Employee_AI_LZ_PK>, IEmployeeGrain, IGrainWithIntegerKey
    {
    }

#if ALLOW_FT_ACTIVE
    public class FT_Grain_Employee_AI_LZ_PK : TestEmployeeGrain<EmployeeGrainState>,
                                              IFT_Grain_Person_AI_LZ_PK, IFT_Grain_Job_AI_LZ_PK, IFT_Grain_Employee_AI_LZ_PK
    {
        public FT_Grain_Employee_AI_LZ_PK(
            [FaultTolerantWorkflowIndexedState(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<EmployeeGrainState> indexedState)
            : base(indexedState) { }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Grain_Employee_AI_LZ_PK : TestEmployeeGrain<EmployeeGrainState>,
                                               INFT_Grain_Person_AI_LZ_PK, INFT_Grain_Job_AI_LZ_PK, INFT_Grain_Employee_AI_LZ_PK
    {
        public NFT_Grain_Employee_AI_LZ_PK(
            [NonFaultTolerantWorkflowIndexedState(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<EmployeeGrainState> indexedState)
            : base(indexedState) { }
    }
    #endregion // PartitionedPerKey

    #region PartitionedPerSilo

#if ALLOW_FT_ACTIVE
    public class FT_Props_Person_AI_LZ_PS : IPersonProperties
    {
        [Index(typeof(IActiveHashIndexPartitionedPerSilo<string, IFT_Grain_Person_AI_LZ_PS>), IsEager = false, IsUnique = false)]   // PerSilo cannot be Unique
        public string Name { get; set; }

        [Index(typeof(IActiveHashIndexPartitionedPerSilo<int, IFT_Grain_Person_AI_LZ_PS>), IsEager = false, IsUnique = false, NullValue = "0")]
        public int Age { get; set; }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Props_Person_AI_LZ_PS : IPersonProperties
    {
        [Index(typeof(IActiveHashIndexPartitionedPerSilo<string, INFT_Grain_Person_AI_LZ_PS>), IsEager = false, IsUnique = false)]  // PerSilo cannot be Unique
        public string Name { get; set; }

        [Index(typeof(IActiveHashIndexPartitionedPerSilo<int, INFT_Grain_Person_AI_LZ_PS>), IsEager = false, IsUnique = false, NullValue = "0")]
        public int Age { get; set; }
    }

#if ALLOW_FT_ACTIVE
    public class FT_Props_Job_AI_LZ_PS : IJobProperties
    {
        [Index(typeof(IActiveHashIndexPartitionedPerSilo<string, IFT_Grain_Job_AI_LZ_PS>), IsEager = false, IsUnique = false)]  // PerSilo cannot be Unique
        public string Title { get; set; }

        [Index(typeof(IActiveHashIndexPartitionedPerSilo<string, IFT_Grain_Job_AI_LZ_PS>), IsEager = false, IsUnique = false)]
        public string Department { get; set; }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Props_Job_AI_LZ_PS : IJobProperties
    {
        [Index(typeof(IActiveHashIndexPartitionedPerSilo<string, INFT_Grain_Job_AI_LZ_PS>), IsEager = false, IsUnique = false)] // PerSilo cannot be Unique
        public string Title { get; set; }

        [Index(typeof(IActiveHashIndexPartitionedPerSilo<string, INFT_Grain_Job_AI_LZ_PS>), IsEager = false, IsUnique = false)]
        public string Department { get; set; }
    }

#if ALLOW_FT_ACTIVE
    public class FT_Props_Employee_AI_LZ_PS : IEmployeeProperties
    {
        [Index(typeof(IActiveHashIndexPartitionedPerSilo<int, IFT_Grain_Employee_AI_LZ_PS>), IsEager = false, IsUnique = false, NullValue = "-1")]  // PerSilo cannot be Unique
        public int EmployeeId { get; set; }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Props_Employee_AI_LZ_PS : IEmployeeProperties
    {
        [Index(typeof(IActiveHashIndexPartitionedPerSilo<int, INFT_Grain_Employee_AI_LZ_PS>), IsEager = false, IsUnique = false, NullValue = "-1")] // PerSilo cannot be Unique
        public int EmployeeId { get; set; }
    }

#if ALLOW_FT_ACTIVE
    public interface IFT_Grain_Person_AI_LZ_PS : IIndexableGrain<FT_Props_Person_AI_LZ_PS>, IPersonGrain, IGrainWithIntegerKey
    {
    }
#endif // ALLOW_FT_ACTIVE

    public interface INFT_Grain_Person_AI_LZ_PS : IIndexableGrain<NFT_Props_Person_AI_LZ_PS>, IPersonGrain, IGrainWithIntegerKey
    {
    }

#if ALLOW_FT_ACTIVE
    public interface IFT_Grain_Job_AI_LZ_PS : IIndexableGrain<FT_Props_Job_AI_LZ_PS>, IJobGrain, IGrainWithIntegerKey
    {
    }
#endif // ALLOW_FT_ACTIVE

    public interface INFT_Grain_Job_AI_LZ_PS : IIndexableGrain<NFT_Props_Job_AI_LZ_PS>, IJobGrain, IGrainWithIntegerKey
    {
    }

#if ALLOW_FT_ACTIVE
    public interface IFT_Grain_Employee_AI_LZ_PS : IIndexableGrain<FT_Props_Employee_AI_LZ_PS>, IEmployeeGrain, IGrainWithIntegerKey
    {
    }
#endif // ALLOW_FT_ACTIVE

    public interface INFT_Grain_Employee_AI_LZ_PS : IIndexableGrain<NFT_Props_Employee_AI_LZ_PS>, IEmployeeGrain, IGrainWithIntegerKey
    {
    }

#if ALLOW_FT_ACTIVE
    public class FT_Grain_Employee_AI_LZ_PS : TestEmployeeGrain<EmployeeGrainState>,
                                              IFT_Grain_Person_AI_LZ_PS, IFT_Grain_Job_AI_LZ_PS, IFT_Grain_Employee_AI_LZ_PS
    {
        public FT_Grain_Employee_AI_LZ_PS(
            [FaultTolerantWorkflowIndexedState(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<EmployeeGrainState> indexedState)
            : base(indexedState) { }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Grain_Employee_AI_LZ_PS : TestEmployeeGrain<EmployeeGrainState>,
                                               INFT_Grain_Person_AI_LZ_PS, INFT_Grain_Job_AI_LZ_PS, INFT_Grain_Employee_AI_LZ_PS
    {
        public NFT_Grain_Employee_AI_LZ_PS(
            [NonFaultTolerantWorkflowIndexedState(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<EmployeeGrainState> indexedState)
            : base(indexedState) { }
    }
    #endregion // PartitionedPerSilo

    #region SingleBucket

#if ALLOW_FT_ACTIVE
    public class FT_Props_Person_AI_LZ_SB : IPersonProperties
    {
        [Index(typeof(IActiveHashIndexSingleBucket<string, IFT_Grain_Person_AI_LZ_SB>), IsEager = false, IsUnique = false)]  // PerSilo cannot be Unique
        public string Name { get; set; }

        [Index(typeof(IActiveHashIndexSingleBucket<int, IFT_Grain_Person_AI_LZ_SB>), IsEager = false, IsUnique = false, NullValue = "0")]
        public int Age { get; set; }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Props_Person_AI_LZ_SB : IPersonProperties
    {
        [Index(typeof(IActiveHashIndexSingleBucket<string, INFT_Grain_Person_AI_LZ_SB>), IsEager = false, IsUnique = true)]
        public string Name { get; set; }

        [Index(typeof(IActiveHashIndexSingleBucket<int, INFT_Grain_Person_AI_LZ_SB>), IsEager = false, IsUnique = false, NullValue = "0")]
        public int Age { get; set; }
    }

#if ALLOW_FT_ACTIVE
    public class FT_Props_Job_AI_LZ_SB : IJobProperties
    {
        [Index(typeof(IActiveHashIndexSingleBucket<string, IFT_Grain_Job_AI_LZ_SB>), IsEager = false, IsUnique = true)]
        public string Title { get; set; }

        [Index(typeof(IActiveHashIndexSingleBucket<string, IFT_Grain_Job_AI_LZ_SB>), IsEager = false, IsUnique = false)]
        public string Department { get; set; }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Props_Job_AI_LZ_SB : IJobProperties
    {
        [Index(typeof(IActiveHashIndexSingleBucket<string, INFT_Grain_Job_AI_LZ_SB>), IsEager = false, IsUnique = true)]
        public string Title { get; set; }

        [Index(typeof(IActiveHashIndexSingleBucket<string, INFT_Grain_Job_AI_LZ_SB>), IsEager = false, IsUnique = false)]
        public string Department { get; set; }
    }

#if ALLOW_FT_ACTIVE
    public class FT_Props_Employee_AI_LZ_SB : IEmployeeProperties
    {
        [Index(typeof(IActiveHashIndexSingleBucket<int, IFT_Grain_Employee_AI_LZ_SB>), IsEager = false, IsUnique = true, NullValue = "-1")]
        public int EmployeeId { get; set; }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Props_Employee_AI_LZ_SB : IEmployeeProperties
    {
        [Index(typeof(IActiveHashIndexSingleBucket<int, INFT_Grain_Employee_AI_LZ_SB>), IsEager = false, IsUnique = true, NullValue = "-1")]
        public int EmployeeId { get; set; }
    }

#if ALLOW_FT_ACTIVE
    public interface IFT_Grain_Person_AI_LZ_SB : IIndexableGrain<FT_Props_Person_AI_LZ_SB>, IPersonGrain, IGrainWithIntegerKey
    {
    }
#endif // ALLOW_FT_ACTIVE

    public interface INFT_Grain_Person_AI_LZ_SB : IIndexableGrain<NFT_Props_Person_AI_LZ_SB>, IPersonGrain, IGrainWithIntegerKey
    {
    }

#if ALLOW_FT_ACTIVE
    public interface IFT_Grain_Job_AI_LZ_SB : IIndexableGrain<FT_Props_Job_AI_LZ_SB>, IJobGrain, IGrainWithIntegerKey
    {
    }
#endif // ALLOW_FT_ACTIVE

    public interface INFT_Grain_Job_AI_LZ_SB : IIndexableGrain<NFT_Props_Job_AI_LZ_SB>, IJobGrain, IGrainWithIntegerKey
    {
    }

#if ALLOW_FT_ACTIVE
    public interface IFT_Grain_Employee_AI_LZ_SB : IIndexableGrain<FT_Props_Employee_AI_LZ_SB>, IEmployeeGrain, IGrainWithIntegerKey
    {
    }
#endif // ALLOW_FT_ACTIVE

    public interface INFT_Grain_Employee_AI_LZ_SB : IIndexableGrain<NFT_Props_Employee_AI_LZ_SB>, IEmployeeGrain, IGrainWithIntegerKey
    {
    }

#if ALLOW_FT_ACTIVE
    public class FT_Grain_Employee_AI_LZ_SB : TestEmployeeGrain<EmployeeGrainState>,
                                              IFT_Grain_Person_AI_LZ_SB, IFT_Grain_Job_AI_LZ_SB, IFT_Grain_Employee_AI_LZ_SB
    {
        public FT_Grain_Employee_AI_LZ_SB(
            [FaultTolerantWorkflowIndexedState(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<EmployeeGrainState> indexedState)
            : base(indexedState) { }
    }
#endif // ALLOW_FT_ACTIVE

    public class NFT_Grain_Employee_AI_LZ_SB : TestEmployeeGrain<EmployeeGrainState>,
                                               INFT_Grain_Person_AI_LZ_SB, INFT_Grain_Job_AI_LZ_SB, INFT_Grain_Employee_AI_LZ_SB
    {
        public NFT_Grain_Employee_AI_LZ_SB(
            [NonFaultTolerantWorkflowIndexedState(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
            IIndexedState<EmployeeGrainState> indexedState)
            : base(indexedState) { }
    }
    #endregion // SingleBucket

    public abstract class MultiInterface_AI_LZ_Runner : IndexingTestRunnerBase
    {
        protected MultiInterface_AI_LZ_Runner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

#if ALLOW_FT_ACTIVE
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_FT_Grain_Employee_AI_LZ_PK()
        {
            await base.TestEmployeeIndexesWithDeactivations<IFT_Grain_Person_AI_LZ_PK, FT_Props_Person_AI_LZ_PK,
                                                            IFT_Grain_Job_AI_LZ_PK, FT_Props_Job_AI_LZ_PK,
                                                            IFT_Grain_Employee_AI_LZ_PK, FT_Props_Employee_AI_LZ_PK>();
        }
#endif // ALLOW_FT_ACTIVE

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_Employee_AI_LZ_PK()
        {
            await base.TestEmployeeIndexesWithDeactivations<INFT_Grain_Person_AI_LZ_PK, NFT_Props_Person_AI_LZ_PK,
                                                            INFT_Grain_Job_AI_LZ_PK, NFT_Props_Job_AI_LZ_PK,
                                                            INFT_Grain_Employee_AI_LZ_PK, NFT_Props_Employee_AI_LZ_PK>();
        }

#if ALLOW_FT_ACTIVE
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_FT_Grain_Employee_AI_LZ_PS()
        {
            await base.TestEmployeeIndexesWithDeactivations<IFT_Grain_Person_AI_LZ_PS, FT_Props_Person_AI_LZ_PS,
                                                            IFT_Grain_Job_AI_LZ_PS, FT_Props_Job_AI_LZ_PS,
                                                            IFT_Grain_Employee_AI_LZ_PS, FT_Props_Employee_AI_LZ_PS>();
        }
#endif // ALLOW_FT_ACTIVE

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_Employee_AI_LZ_PS()
        {
            await base.TestEmployeeIndexesWithDeactivations<INFT_Grain_Person_AI_LZ_PS, NFT_Props_Person_AI_LZ_PS,
                                                            INFT_Grain_Job_AI_LZ_PS, NFT_Props_Job_AI_LZ_PS,
                                                            INFT_Grain_Employee_AI_LZ_PS, NFT_Props_Employee_AI_LZ_PS>();
        }

#if ALLOW_FT_ACTIVE
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_FT_Grain_Employee_AI_LZ_SB()
        {
            await base.TestEmployeeIndexesWithDeactivations<IFT_Grain_Person_AI_LZ_SB, FT_Props_Person_AI_LZ_SB,
                                                            IFT_Grain_Job_AI_LZ_SB, FT_Props_Job_AI_LZ_SB,
                                                            IFT_Grain_Employee_AI_LZ_SB, FT_Props_Employee_AI_LZ_SB>();
        }
#endif // ALLOW_FT_ACTIVE

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_Employee_AI_LZ_SB()
        {
            await base.TestEmployeeIndexesWithDeactivations<INFT_Grain_Person_AI_LZ_SB, NFT_Props_Person_AI_LZ_SB,
                                                            INFT_Grain_Job_AI_LZ_SB, NFT_Props_Job_AI_LZ_SB,
                                                            INFT_Grain_Employee_AI_LZ_SB, NFT_Props_Employee_AI_LZ_SB>();
        }

        internal static IEnumerable<Func<IndexingTestRunnerBase, int, Task>> GetAllTestTasks(TestIndexPartitionType testIndexTypes)
        {
            if (testIndexTypes.HasFlag(TestIndexPartitionType.PerKeyHash))
            {
#if ALLOW_FT_ACTIVE
                yield return (baseRunner, intAdjust) => baseRunner.TestEmployeeIndexesWithDeactivations<
                                                            IFT_Grain_Person_AI_LZ_PK, FT_Props_Person_AI_LZ_PK,
                                                            IFT_Grain_Job_AI_LZ_PK, FT_Props_Job_AI_LZ_PK,
                                                            IFT_Grain_Employee_AI_LZ_PK, FT_Props_Employee_AI_LZ_PK>(intAdjust);
#endif // ALLOW_FT_ACTIVE
                yield return (baseRunner, intAdjust) => baseRunner.TestEmployeeIndexesWithDeactivations<
                                                            INFT_Grain_Person_AI_LZ_PK, NFT_Props_Person_AI_LZ_PK,
                                                            INFT_Grain_Job_AI_LZ_PK, NFT_Props_Job_AI_LZ_PK,
                                                            INFT_Grain_Employee_AI_LZ_PK, NFT_Props_Employee_AI_LZ_PK>(intAdjust);
            }
            if (testIndexTypes.HasFlag(TestIndexPartitionType.PerSilo))
            {
#if ALLOW_FT_ACTIVE
                yield return (baseRunner, intAdjust) => baseRunner.TestEmployeeIndexesWithDeactivations<
                                                            IFT_Grain_Person_AI_LZ_PS, FT_Props_Person_AI_LZ_PS,
                                                            IFT_Grain_Job_AI_LZ_PS, FT_Props_Job_AI_LZ_PS,
                                                            IFT_Grain_Employee_AI_LZ_PS, FT_Props_Employee_AI_LZ_PS>(intAdjust);
#endif // ALLOW_FT_ACTIVE
                yield return (baseRunner, intAdjust) => baseRunner.TestEmployeeIndexesWithDeactivations<
                                                            INFT_Grain_Person_AI_LZ_PS, NFT_Props_Person_AI_LZ_PS,
                                                            INFT_Grain_Job_AI_LZ_PS, NFT_Props_Job_AI_LZ_PS,
                                                            INFT_Grain_Employee_AI_LZ_PS, NFT_Props_Employee_AI_LZ_PS>(intAdjust);
            }
            if (testIndexTypes.HasFlag(TestIndexPartitionType.SingleBucket))
            {
#if ALLOW_FT_ACTIVE
                yield return (baseRunner, intAdjust) => baseRunner.TestEmployeeIndexesWithDeactivations<
                                                            IFT_Grain_Person_AI_LZ_SB, FT_Props_Person_AI_LZ_SB,
                                                            IFT_Grain_Job_AI_LZ_SB, FT_Props_Job_AI_LZ_SB,
                                                            IFT_Grain_Employee_AI_LZ_SB, FT_Props_Employee_AI_LZ_SB>(intAdjust);
#endif // ALLOW_FT_ACTIVE
                yield return (baseRunner, intAdjust) => baseRunner.TestEmployeeIndexesWithDeactivations<
                                                            INFT_Grain_Person_AI_LZ_SB, NFT_Props_Person_AI_LZ_SB,
                                                            INFT_Grain_Job_AI_LZ_SB, NFT_Props_Job_AI_LZ_SB,
                                                            INFT_Grain_Employee_AI_LZ_SB, NFT_Props_Employee_AI_LZ_SB>(intAdjust);
            }
        }
    }
}
