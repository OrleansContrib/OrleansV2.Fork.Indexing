using System.Linq;
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
            var p1 = base.GetGrain<IPlayerChain1Grain>(1);
            await p1.SetLocation(ITC.Seattle);

            // MaxEntriesPerBucket == 5
            var p2 = base.GetGrain<IPlayerChain1Grain>(2);
            var p3 = base.GetGrain<IPlayerChain1Grain>(3);
            var p4 = base.GetGrain<IPlayerChain1Grain>(4);
            var p5 = base.GetGrain<IPlayerChain1Grain>(5);
            var p6 = base.GetGrain<IPlayerChain1Grain>(6);
            var p7 = base.GetGrain<IPlayerChain1Grain>(7);
            var p8 = base.GetGrain<IPlayerChain1Grain>(8);
            var p9 = base.GetGrain<IPlayerChain1Grain>(9);
            var p10 = base.GetGrain<IPlayerChain1Grain>(10);

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
            Thread.Sleep(ITC.DelayUntilIndexesAreUpdatedLazily);

            Assert.Equal(1, await getLocationCount(ITC.Seattle));
            Assert.Equal(3, await getLocationCount(ITC.Kirkland));

            p10 = base.GetGrain<IPlayerChain1Grain>(10);
            Assert.Equal(ITC.Kirkland, await p10.GetLocation());

            p8 = base.GetGrain<IPlayerChain1Grain>(8);
            p9 = base.GetGrain<IPlayerChain1Grain>(9);
            Assert.Equal(ITC.Kirkland, await p8.GetLocation());     // Must call a method first before it is activated (and inserted into active indexes)
            Assert.Equal(ITC.Seattle, await p9.GetLocation());
            Assert.Equal(2, await getLocationCount(ITC.Seattle));
            Assert.Equal(4, await getLocationCount(ITC.Kirkland));

            // Test updates
            await p2.SetLocation(ITC.Yazd);

            Assert.Equal(0, await getLocationCount(ITC.SanJose));
            Assert.Equal(1, await getLocationCount(ITC.Yazd));
        }

        /// <summary>
        /// Tests basic functionality of ActiveHashIndexPartitionedPerSiloImpl with 1 Silo
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup2()
        {
            var p1 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(1);
            await p1.SetLocation(ITC.Tehran);

            var p2 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(2);
            var p3 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(3);

            await p2.SetLocation(ITC.Tehran);
            await p3.SetLocation(ITC.Yazd);

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer2GrainNonFaultTolerant>(ITC.LocationProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCount<IPlayer2GrainNonFaultTolerant, Player2PropertiesNonFaultTolerant>(location);

            Assert.Equal(2, await getLocationCount(ITC.Tehran));

            await p2.Deactivate();
            Thread.Sleep(ITC.DelayUntilIndexesAreUpdatedLazily);
            Assert.Equal(1, await getLocationCount(ITC.Tehran));

            p2 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(2);
            Thread.Sleep(ITC.DelayUntilIndexesAreUpdatedLazily);
            Assert.Equal(ITC.Tehran, await p2.GetLocation());
            Assert.Equal(2, await getLocationCount(ITC.Tehran));
        }

        /// <summary>
        /// Tests basic functionality of HashIndexSingleBucket with chained buckets
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexUpdate3()
        {
            // MaxEntriesPerBucket == 5
            var grains = (await Task.WhenAll(Enumerable.Range(0, 20).Select(async ii =>
            {
                var grain = base.GetGrain<IPlayerChain1Grain>(ii);
                await grain.SetLocation(ITC.Seattle);
                return grain;
            }))).ToArray();

            var locIdx = await base.GetAndWaitForIndex<string, IPlayerChain1Grain>(ITC.LocationProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCount<IPlayerChain1Grain, PlayerChain1Properties>(location);

            Assert.Equal(20, await getLocationCount(ITC.Seattle));

            for (var ii = 19; ii >= 9; ii -= 2)
            {
                await grains[ii].SetLocation(ITC.Redmond);
            }

            Assert.Equal(14, await getLocationCount(ITC.Seattle));
            Assert.Equal(6, await getLocationCount(ITC.Redmond));
        }

        /// <summary>
        /// Tests basic functionality of HashIndexSingleBucket with chained buckets
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing"), TestCategory("TransactionalIndexing")]
        public async Task Test_Indexing_IndexUpdate3_Txn()
        {
            // MaxEntriesPerBucket == 5
            var grains = (await Task.WhenAll(Enumerable.Range(0, 20).Select(async ii =>
            {
                var grain = base.GetGrain<IPlayerChain1GrainTransactional>(ii);
                await grain.SetLocation(ITC.Seattle);
                return grain;
            }))).ToArray();

            var locIdx = await base.GetAndWaitForIndex<string, IPlayerChain1GrainTransactional>(ITC.LocationProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCountTxn<IPlayerChain1GrainTransactional, PlayerChain1PropertiesTransactional>(location);

            Assert.Equal(20, await getLocationCount(ITC.Seattle));

            for (var ii = 19; ii >= 9; ii -= 2)
            {
                await grains[ii].SetLocation(ITC.Redmond);
            }

            Assert.Equal(14, await getLocationCount(ITC.Seattle));
            Assert.Equal(6, await getLocationCount(ITC.Redmond));
        }

        /// <summary>
        /// Tests basic functionality of HashIndexPartitionedPerKey
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup4()
        {
            var p1 = base.GetGrain<IPlayer3GrainNonFaultTolerant>(1);
            await p1.SetLocation(ITC.Seattle);

            var p2 = base.GetGrain<IPlayer3GrainNonFaultTolerant>(2);
            var p3 = base.GetGrain<IPlayer3GrainNonFaultTolerant>(3);

            await p2.SetLocation(ITC.Seattle);
            await p3.SetLocation(ITC.SanFrancisco);

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer3GrainNonFaultTolerant>(ITC.LocationProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCount<IPlayer3GrainNonFaultTolerant, Player3PropertiesNonFaultTolerant>(location);

            Assert.Equal(2, await getLocationCount(ITC.Seattle));

            await p2.Deactivate();
            Thread.Sleep(ITC.DelayUntilIndexesAreUpdatedLazily);
            Assert.Equal(1, await getLocationCount(ITC.Seattle));

            p2 = base.GetGrain<IPlayer3GrainNonFaultTolerant>(2);
            Assert.Equal(ITC.Seattle, await p2.GetLocation());
            Assert.Equal(2, await getLocationCount(ITC.Seattle));

            // Test updates
            await p2.SetLocation(ITC.SanFrancisco);
            Assert.Equal(1, await getLocationCount(ITC.Seattle));
            Assert.Equal(2, await getLocationCount(ITC.SanFrancisco));
        }

        /// <summary>
        /// Tests basic functionality of HashIndexPartitionedPerKey
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing"), TestCategory("TransactionalIndexing")]
        public async Task Test_Indexing_IndexLookup4_Txn()
        {
            var p1 = base.GetGrain<IPlayer3GrainTransactional>(1);
            await p1.SetLocation(ITC.Seattle);

            var p2 = base.GetGrain<IPlayer3GrainTransactional>(2);
            var p3 = base.GetGrain<IPlayer3GrainTransactional>(3);

            await p2.SetLocation(ITC.Seattle);
            await p3.SetLocation(ITC.SanFrancisco);

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer3GrainTransactional>(ITC.LocationProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCountTxn<IPlayer3GrainTransactional, Player3PropertiesTransactional>(location);

            Assert.Equal(2, await getLocationCount(ITC.Seattle));

            await p2.Deactivate();
            Thread.Sleep(ITC.DelayUntilIndexesAreUpdatedLazily);
            Assert.Equal(2, await getLocationCount(ITC.Seattle));   // Transactional indexes are Total, so the count remains 2

            p2 = base.GetGrain<IPlayer3GrainTransactional>(2);
            Assert.Equal(ITC.Seattle, await p2.GetLocation());
            Assert.Equal(2, await getLocationCount(ITC.Seattle));

            // Test updates
            await p2.SetLocation(ITC.SanFrancisco);
            Assert.Equal(1, await getLocationCount(ITC.Seattle));
            Assert.Equal(2, await getLocationCount(ITC.SanFrancisco));
        }
    }
}
