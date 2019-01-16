using Orleans.Providers;
using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Orleans.Indexing.Facet;
using System.Collections.Generic;

namespace Orleans.Indexing.Tests.MultiInterface
{
    #region PartitionedPerKey

    // NFT only; FT cannot be configured to be Eager.

    [Serializable]
    public class NFT_Props_Person_AI_EG_PK : IPersonProperties
    {
        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, INFT_Grain_Person_AI_EG_PK>), IsEager = true, IsUnique = true)]
        public string Name { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<int, INFT_Grain_Person_AI_EG_PK>), IsEager = true, IsUnique = false, NullValue = "0")]
        public int Age { get; set; }
    }

    [Serializable]
    public class NFT_Props_Job_AI_EG_PK : IJobProperties
    {
        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, INFT_Grain_Job_AI_EG_PK>), IsEager = true, IsUnique = true)]
        public string Title { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, INFT_Grain_Job_AI_EG_PK>), IsEager = true, IsUnique = false)]
        public string Department { get; set; }
    }

    [Serializable]
    public class NFT_Props_Employee_AI_EG_PK : IEmployeeProperties
    {
        [Index(typeof(ActiveHashIndexPartitionedPerKey<int, INFT_Grain_Employee_AI_EG_PK>), IsEager = true, IsUnique = true, NullValue = "-1")]
        public int EmployeeId { get; set; }
    }

    public interface INFT_Grain_Person_AI_EG_PK : IIndexableGrain<NFT_Props_Person_AI_EG_PK>, IPersonGrain, IGrainWithIntegerKey
    {
    }

    public interface INFT_Grain_Job_AI_EG_PK : IIndexableGrain<NFT_Props_Job_AI_EG_PK>, IJobGrain, IGrainWithIntegerKey
    {
    }

    public interface INFT_Grain_Employee_AI_EG_PK : IIndexableGrain<NFT_Props_Employee_AI_EG_PK>, IEmployeeGrain, IGrainWithIntegerKey
    {
    }

    [StorageProvider(ProviderName = IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
    public class NFT_Grain_Employee_AI_EG_PK : TestEmployeeGrain<EmployeeGrainState, IndexableGrainStateWrapper<EmployeeGrainState>>,
                                               INFT_Grain_Person_AI_EG_PK, INFT_Grain_Job_AI_EG_PK, INFT_Grain_Employee_AI_EG_PK
    {
        public NFT_Grain_Employee_AI_EG_PK(
            [NonFaultTolerantWorkflowIndexWriter]
            IIndexWriter<EmployeeGrainState> indexWriter)
            : base(indexWriter) { }
    }
    #endregion // PartitionedPerKey

    #region PartitionedPerSilo

    // NFT only; FT cannot be configured to be Eager.

    [Serializable]
    public class NFT_Props_Person_AI_EG_PS : IPersonProperties
    {
        [Index(typeof(IActiveHashIndexPartitionedPerSilo<string, INFT_Grain_Person_AI_EG_PS>), IsEager = true, IsUnique = true)]
        public string Name { get; set; }

        [Index(typeof(IActiveHashIndexPartitionedPerSilo<int, INFT_Grain_Person_AI_EG_PS>), IsEager = true, IsUnique = false, NullValue = "0")]
        public int Age { get; set; }
    }

    [Serializable]
    public class NFT_Props_Job_AI_EG_PS : IJobProperties
    {
        [Index(typeof(IActiveHashIndexPartitionedPerSilo<string, INFT_Grain_Job_AI_EG_PS>), IsEager = true, IsUnique = true)]
        public string Title { get; set; }

        [Index(typeof(IActiveHashIndexPartitionedPerSilo<string, INFT_Grain_Job_AI_EG_PS>), IsEager = true, IsUnique = false)]
        public string Department { get; set; }
    }

    [Serializable]
    public class NFT_Props_Employee_AI_EG_PS : IEmployeeProperties
    {
        [Index(typeof(IActiveHashIndexPartitionedPerSilo<int, INFT_Grain_Employee_AI_EG_PS>), IsEager = true, IsUnique = true, NullValue = "-1")]
        public int EmployeeId { get; set; }
    }

    public interface INFT_Grain_Person_AI_EG_PS : IIndexableGrain<NFT_Props_Person_AI_EG_PS>, IPersonGrain, IGrainWithIntegerKey
    {
    }

    public interface INFT_Grain_Job_AI_EG_PS : IIndexableGrain<NFT_Props_Job_AI_EG_PS>, IJobGrain, IGrainWithIntegerKey
    {
    }

    public interface INFT_Grain_Employee_AI_EG_PS : IIndexableGrain<NFT_Props_Employee_AI_EG_PS>, IEmployeeGrain, IGrainWithIntegerKey
    {
    }

    [StorageProvider(ProviderName = IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
    public class NFT_Grain_Employee_AI_EG_PS : TestEmployeeGrain<EmployeeGrainState, IndexableGrainStateWrapper<EmployeeGrainState>>,
                                               INFT_Grain_Person_AI_EG_PS, INFT_Grain_Job_AI_EG_PS, INFT_Grain_Employee_AI_EG_PS
    {
        public NFT_Grain_Employee_AI_EG_PS(
            [NonFaultTolerantWorkflowIndexWriter]
            IIndexWriter<EmployeeGrainState> indexWriter)
            : base(indexWriter) { }
    }
    #endregion // PartitionedPerSilo

    #region SingleBucket

    // NFT only; FT cannot be configured to be Eager.

    [Serializable]
    public class NFT_Props_Person_AI_EG_SB : IPersonProperties
    {
        [Index(typeof(IActiveHashIndexSingleBucket<string, INFT_Grain_Person_AI_EG_SB>), IsEager = true, IsUnique = true)]
        public string Name { get; set; }

        [Index(typeof(IActiveHashIndexSingleBucket<int, INFT_Grain_Person_AI_EG_SB>), IsEager = true, IsUnique = false, NullValue = "0")]
        public int Age { get; set; }
    }

    [Serializable]
    public class NFT_Props_Job_AI_EG_SB : IJobProperties
    {
        [Index(typeof(IActiveHashIndexSingleBucket<string, INFT_Grain_Job_AI_EG_SB>), IsEager = true, IsUnique = true)]
        public string Title { get; set; }

        [Index(typeof(IActiveHashIndexSingleBucket<string, INFT_Grain_Job_AI_EG_SB>), IsEager = true, IsUnique = false)]
        public string Department { get; set; }
    }

    [Serializable]
    public class NFT_Props_Employee_AI_EG_SB : IEmployeeProperties
    {
        [Index(typeof(IActiveHashIndexSingleBucket<int, INFT_Grain_Employee_AI_EG_SB>), IsEager = true, IsUnique = true, NullValue = "-1")]
        public int EmployeeId { get; set; }
    }

    public interface INFT_Grain_Person_AI_EG_SB : IIndexableGrain<NFT_Props_Person_AI_EG_SB>, IPersonGrain, IGrainWithIntegerKey
    {
    }

    public interface INFT_Grain_Job_AI_EG_SB : IIndexableGrain<NFT_Props_Job_AI_EG_SB>, IJobGrain, IGrainWithIntegerKey
    {
    }

    public interface INFT_Grain_Employee_AI_EG_SB : IIndexableGrain<NFT_Props_Employee_AI_EG_SB>, IEmployeeGrain, IGrainWithIntegerKey
    {
    }

    [StorageProvider(ProviderName = IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
    public class NFT_Grain_Employee_AI_EG_SB : TestEmployeeGrain<EmployeeGrainState, IndexableGrainStateWrapper<EmployeeGrainState>>,
                                               INFT_Grain_Person_AI_EG_SB, INFT_Grain_Job_AI_EG_SB, INFT_Grain_Employee_AI_EG_SB
    {
        public NFT_Grain_Employee_AI_EG_SB(
            [NonFaultTolerantWorkflowIndexWriter]
            IIndexWriter<EmployeeGrainState> indexWriter)
            : base(indexWriter) { }
    }
    #endregion // SingleBucket

    public abstract class MultiInterface_AI_EG_Runner : IndexingTestRunnerBase
    {
        protected MultiInterface_AI_EG_Runner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_Employee_AI_EG_PK()
        {
            await base.TestEmployeeIndexesWithDeactivations<INFT_Grain_Person_AI_EG_PK, NFT_Props_Person_AI_EG_PK,
                                                            INFT_Grain_Job_AI_EG_PK, NFT_Props_Job_AI_EG_PK,
                                                            INFT_Grain_Employee_AI_EG_PK, NFT_Props_Employee_AI_EG_PK>();
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_Employee_AI_EG_PS()
        {
            await base.TestEmployeeIndexesWithDeactivations<INFT_Grain_Person_AI_EG_PS, NFT_Props_Person_AI_EG_PS,
                                                            INFT_Grain_Job_AI_EG_PS, NFT_Props_Job_AI_EG_PS,
                                                            INFT_Grain_Employee_AI_EG_PS, NFT_Props_Employee_AI_EG_PS>();
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_Employee_AI_EG_SB()
        {
            await base.TestEmployeeIndexesWithDeactivations<INFT_Grain_Person_AI_EG_SB, NFT_Props_Person_AI_EG_SB,
                                                            INFT_Grain_Job_AI_EG_SB, NFT_Props_Job_AI_EG_SB,
                                                            INFT_Grain_Employee_AI_EG_SB, NFT_Props_Employee_AI_EG_SB>();
        }

        internal static IEnumerable<Func<IndexingTestRunnerBase, int, Task>> GetAllTestTasks(TestIndexPartitionType testIndexTypes)
        {
            if (testIndexTypes.HasFlag(TestIndexPartitionType.PerKeyHash))
            {
                yield return (baseRunner, intAdjust) => baseRunner.TestEmployeeIndexesWithDeactivations<
                                                            INFT_Grain_Person_AI_EG_PK, NFT_Props_Person_AI_EG_PK,
                                                            INFT_Grain_Job_AI_EG_PK, NFT_Props_Job_AI_EG_PK,
                                                            INFT_Grain_Employee_AI_EG_PK, NFT_Props_Employee_AI_EG_PK>(intAdjust);
            }
            if (testIndexTypes.HasFlag(TestIndexPartitionType.PerSilo))
            {
                yield return (baseRunner, intAdjust) => baseRunner.TestEmployeeIndexesWithDeactivations<
                                                            INFT_Grain_Person_AI_EG_PS, NFT_Props_Person_AI_EG_PS,
                                                            INFT_Grain_Job_AI_EG_PS, NFT_Props_Job_AI_EG_PS,
                                                            INFT_Grain_Employee_AI_EG_PS, NFT_Props_Employee_AI_EG_PS>(intAdjust);
            }
            if (testIndexTypes.HasFlag(TestIndexPartitionType.SingleBucket))
            {
                yield return (baseRunner, intAdjust) => baseRunner.TestEmployeeIndexesWithDeactivations<
                                                            INFT_Grain_Person_AI_EG_SB, NFT_Props_Person_AI_EG_SB,
                                                            INFT_Grain_Job_AI_EG_SB, NFT_Props_Job_AI_EG_SB,
                                                            INFT_Grain_Employee_AI_EG_SB, NFT_Props_Employee_AI_EG_SB>(intAdjust);
            }
        }
    }
}
