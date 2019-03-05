using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    using ITC = IndexingTestConstants;

    public abstract class ChainedBucketIndexingTwoSiloRunner : IndexingTestRunnerBase
    {
        protected ChainedBucketIndexingTwoSiloRunner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

        /// <summary>
        /// Tests basic functionality of ActiveHashIndexPartitionedPerSiloImpl with 2 Silos
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup3()
        {
            await base.StartAndWaitForSecondSilo();

            IPlayer2GrainNonFaultTolerant p1 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(1);
            await p1.SetLocation(ITC.Seattle);

            IPlayer2GrainNonFaultTolerant p2 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(2);
            IPlayer2GrainNonFaultTolerant p3 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(3);

            await p2.SetLocation(ITC.Seattle);
            await p3.SetLocation(ITC.SanFrancisco);

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer2GrainNonFaultTolerant>(ITC.LocationProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCount<IPlayer2GrainNonFaultTolerant, Player2PropertiesNonFaultTolerant>(location);

            Assert.Equal(2, await getLocationCount(ITC.Seattle));

            await p2.Deactivate();
            Thread.Sleep(1000);
            Assert.Equal(1, await getLocationCount(ITC.Seattle));

            p2 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(2);
            Assert.Equal(ITC.Seattle, await p2.GetLocation());
            Assert.Equal(2, await getLocationCount(ITC.Seattle));
        }
    }
}
