using Xunit.Abstractions;
using Xunit;
using Orleans.Indexing.Tests.MultiInterface;

namespace Orleans.Indexing.Tests
{
    [TestCategory("BVT"), TestCategory("Indexing")]
    public class SimpleIndexingSingleSiloTests : SimpleIndexingSingleSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public SimpleIndexingSingleSiloTests(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class SimpleIndexingTwoSiloTests : SimpleIndexingTwoSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public SimpleIndexingTwoSiloTests(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class ChainedBucketIndexingSingleSiloTests : ChainedBucketIndexingSingleSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public ChainedBucketIndexingSingleSiloTests(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class ChainedBucketIndexingTwoSiloTests : ChainedBucketIndexingTwoSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public ChainedBucketIndexingTwoSiloTests(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class FaultTolerantIndexingSingleSiloTests : FaultTolerantIndexingSingleSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public FaultTolerantIndexingSingleSiloTests(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class FaultTolerantIndexingTwoSiloTests : FaultTolerantIndexingTwoSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public FaultTolerantIndexingTwoSiloTests(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class LazyIndexingSingleSiloTests : LazyIndexingSingleSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public LazyIndexingSingleSiloTests(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class LazyIndexingTwoSiloTests : LazyIndexingTwoSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public LazyIndexingTwoSiloTests(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class NoIndexingTests : NoIndexingRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public NoIndexingTests(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    #region MultiIndex

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiIndex_AI_EG : MultiIndex_AI_EG_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiIndex_AI_EG(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiIndex_AI_LZ : MultiIndex_AI_LZ_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiIndex_AI_LZ(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiIndex_TI_EG : MultiIndex_TI_EG_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiIndex_TI_EG(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiIndex_TI_LZ : MultiIndex_TI_LZ_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiIndex_TI_LZ(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiIndex_XI_EG : MultiIndex_XI_EG_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiIndex_XI_EG(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiIndex_XI_LZ : MultiIndex_XI_LZ_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiIndex_XI_LZ(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiIndex_All : MultiIndex_All_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiIndex_All(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    #endregion MultiIndex

    #region MultiInterface

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiInterface_AI_EG : MultiInterface_AI_EG_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiInterface_AI_EG(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiInterface_AI_LZ : MultiInterface_AI_LZ_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiInterface_AI_LZ(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiInterface_TI_EG : MultiInterface_TI_EG_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiInterface_TI_EG(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiInterface_TI_LZ : MultiInterface_TI_LZ_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiInterface_TI_LZ(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiInterface_XI_EG : MultiInterface_XI_EG_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiInterface_XI_EG(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiInterface_XI_LZ : MultiInterface_XI_LZ_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiInterface_XI_LZ(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiInterface_All : MultiInterface_All_Runner, IClassFixture<WorkflowIndexingFixture>
    {
        public MultiInterface_All(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    #endregion MultiInterface

    #region DirectStorageManagedIndexes

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiIndex_DSMI_EG : MultiIndex_DSMI_EG_Runner, IClassFixture<WorkflowDSMIEGIndexingFixture>
    {
        public MultiIndex_DSMI_EG(WorkflowDSMIEGIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class MultiIndex_DSMI_LZ : MultiIndex_DSMI_LZ_Runner, IClassFixture<WorkflowDSMILZIndexingFixture>
    {
        public MultiIndex_DSMI_LZ(WorkflowDSMILZIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    #endregion // DirectStorageManagedIndexes
}
