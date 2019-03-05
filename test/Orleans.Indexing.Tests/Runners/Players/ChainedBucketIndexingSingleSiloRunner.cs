using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    using ITC = IndexingTestConstants;

    public abstract class ChainedBucketIndexingSingleSiloRunner : IndexingTestRunnerBase
    {
        protected ChainedBucketIndexingSingleSiloRunner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

        /// <summary>
        /// Tests basic functionality of HashIndexSingleBucket with chained buckets
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup1()
        {
            IPlayerChain1Grain p1 = base.GetGrain<IPlayerChain1Grain>(1);
            await p1.SetLocation(ITC.Seattle);

            IPlayerChain1Grain p2 = base.GetGrain<IPlayerChain1Grain>(2);
            IPlayerChain1Grain p3 = base.GetGrain<IPlayerChain1Grain>(3);
            IPlayerChain1Grain p4 = base.GetGrain<IPlayerChain1Grain>(4);
            IPlayerChain1Grain p5 = base.GetGrain<IPlayerChain1Grain>(5);
            IPlayerChain1Grain p6 = base.GetGrain<IPlayerChain1Grain>(6);
            IPlayerChain1Grain p7 = base.GetGrain<IPlayerChain1Grain>(7);
            IPlayerChain1Grain p8 = base.GetGrain<IPlayerChain1Grain>(8);
            IPlayerChain1Grain p9 = base.GetGrain<IPlayerChain1Grain>(9);
            IPlayerChain1Grain p10 = base.GetGrain<IPlayerChain1Grain>(10);

            await p2.SetLocation(ITC.SanJose);
            await p3.SetLocation(ITC.SanFrancisco);
            await p4.SetLocation(ITC.Bellevue);
            await p5.SetLocation(ITC.Redmond);
            await p6.SetLocation(ITC.Kirkland);
            await p7.SetLocation(ITC.Kirkland);
            await p8.SetLocation(ITC.Kirkland);
            await p9.SetLocation(ITC.Seattle);
            await p10.SetLocation(ITC.Kirkland);

            var locIdx = await base.GetAndWaitForIndex<string, IPlayerChain1Grain>(ITC.LocationProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCount<IPlayerChain1Grain, PlayerChain1Properties>(location);

            Assert.Equal(2, await getLocationCount(ITC.Seattle));
            Assert.Equal(4, await getLocationCount(ITC.Kirkland));

            await p8.Deactivate();
            await p9.Deactivate();
            Thread.Sleep(1000);

            Assert.Equal(1, await getLocationCount(ITC.Seattle));
            Assert.Equal(3, await getLocationCount(ITC.Kirkland));

            p10 = base.GetGrain<IPlayerChain1Grain>(10);
            Assert.Equal(ITC.Kirkland, await p10.GetLocation());
        }

        /// <summary>
        /// Tests basic functionality of ActiveHashIndexPartitionedPerSiloImpl with 1 Silo
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup2()
        {
            IPlayer2GrainNonFaultTolerant p1 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(1);
            await p1.SetLocation(ITC.Tehran);

            IPlayer2GrainNonFaultTolerant p2 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(2);
            IPlayer2GrainNonFaultTolerant p3 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(3);

            await p2.SetLocation(ITC.Tehran);
            await p3.SetLocation(ITC.Yazd);

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer2GrainNonFaultTolerant>(ITC.LocationProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCount<IPlayer2GrainNonFaultTolerant, Player2PropertiesNonFaultTolerant>(location);

            Assert.Equal(2, await getLocationCount(ITC.Tehran));

            await p2.Deactivate();
            Thread.Sleep(1000);
            Assert.Equal(1, await getLocationCount(ITC.Tehran));

            p2 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(2);
            Assert.Equal(ITC.Tehran, await p2.GetLocation());
            Assert.Equal(2, await getLocationCount(ITC.Tehran));
        }

        /// <summary>
        /// Tests basic functionality of HashIndexPartitionedPerKey
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup4()
        {
            IPlayer3GrainNonFaultTolerant p1 = base.GetGrain<IPlayer3GrainNonFaultTolerant>(1);
            await p1.SetLocation(ITC.Seattle);

            IPlayer3GrainNonFaultTolerant p2 = base.GetGrain<IPlayer3GrainNonFaultTolerant>(2);
            IPlayer3GrainNonFaultTolerant p3 = base.GetGrain<IPlayer3GrainNonFaultTolerant>(3);

            await p2.SetLocation(ITC.Seattle);
            await p3.SetLocation(ITC.SanFrancisco);

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer3GrainNonFaultTolerant>(ITC.LocationProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCount<IPlayer3GrainNonFaultTolerant, Player3PropertiesNonFaultTolerant>(location);

            Assert.Equal(2, await getLocationCount(ITC.Seattle));

            await p2.Deactivate();
            Thread.Sleep(1000);
            Assert.Equal(1, await getLocationCount(ITC.Seattle));

            p2 = base.GetGrain<IPlayer3GrainNonFaultTolerant>(2);
            Assert.Equal(ITC.Seattle, await p2.GetLocation());
            Assert.Equal(2, await getLocationCount(ITC.Seattle));
        }
    }
}
