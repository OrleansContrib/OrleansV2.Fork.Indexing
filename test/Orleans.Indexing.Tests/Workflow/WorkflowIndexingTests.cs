using Xunit.Abstractions;
using Xunit;
using Orleans.Indexing.Tests.MultiInterface;

namespace Orleans.Indexing.Tests
{
    [TestCategory("BVT"), TestCategory("Indexing")]
    public class SimpleIndexingSingleSiloTestsWf : SimpleIndexingSingleSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public SimpleIndexingSingleSiloTestsWf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class SimpleIndexingTwoSiloTestsWf : SimpleIndexingTwoSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public SimpleIndexingTwoSiloTestsWf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class ChainedBucketIndexingSingleSiloTestsWf : ChainedBucketIndexingSingleSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public ChainedBucketIndexingSingleSiloTestsWf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class ChainedBucketIndexingTwoSiloTestsWf : ChainedBucketIndexingTwoSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public ChainedBucketIndexingTwoSiloTestsWf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class FaultTolerantIndexingSingleSiloTestsWf : FaultTolerantIndexingSingleSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public FaultTolerantIndexingSingleSiloTestsWf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class FaultTolerantIndexingTwoSiloTestsWf : FaultTolerantIndexingTwoSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public FaultTolerantIndexingTwoSiloTestsWf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class LazyIndexingSingleSiloTestsWf : LazyIndexingSingleSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public LazyIndexingSingleSiloTestsWf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class LazyIndexingTwoSiloTestsWf : LazyIndexingTwoSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public LazyIndexingTwoSiloTestsWf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class NoIndexingTestsWf : NoIndexingRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public NoIndexingTestsWf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    #region MultiIndex

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiIndex_AI_EG_Wf : MultiIndex_AI_EG_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiIndex_AI_EG_Wf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiIndex_AI_LZ_Wf : MultiIndex_AI_LZ_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiIndex_AI_LZ_Wf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiIndex_TI_EG_Wf : MultiIndex_TI_EG_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiIndex_TI_EG_Wf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiIndex_TI_LZ_Wf : MultiIndex_TI_LZ_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiIndex_TI_LZ_Wf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiIndex_XI_EG_Wf : MultiIndex_XI_EG_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiIndex_XI_EG_Wf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiIndex_XI_LZ_Wf : MultiIndex_XI_LZ_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiIndex_XI_LZ_Wf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiIndex_All_Wf : MultiIndex_All_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiIndex_All_Wf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    #endregion MultiIndex

    #region MultiInterface

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiInterface_AI_EG_Wf : MultiInterface_AI_EG_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiInterface_AI_EG_Wf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiInterface_AI_LZ_Wf : MultiInterface_AI_LZ_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiInterface_AI_LZ_Wf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiInterface_TI_EG_Wf : MultiInterface_TI_EG_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiInterface_TI_EG_Wf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiInterface_TI_LZ_Wf : MultiInterface_TI_LZ_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiInterface_TI_LZ_Wf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiInterface_XI_EG_Wf : MultiInterface_XI_EG_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiInterface_XI_EG_Wf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiInterface_XI_LZ_Wf : MultiInterface_XI_LZ_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiInterface_XI_LZ_Wf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiInterface_All_Wf : MultiInterface_All_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiInterface_All_Wf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    #endregion MultiInterface

    #region DirectStorageManagedIndexes

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiIndex_DSMI_EG_Wf : MultiIndex_DSMI_EG_Runner, IClassFixture<WorkflowDSMIEGIndexingFixture>
    {
        public MultiIndex_DSMI_EG_Wf(WorkflowDSMIEGIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiIndex_DSMI_LZ_Wf : MultiIndex_DSMI_LZ_Runner, IClassFixture<WorkflowDSMILZIndexingFixture>
    {
        public MultiIndex_DSMI_LZ_Wf(WorkflowDSMILZIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    #endregion // DirectStorageManagedIndexes
}
