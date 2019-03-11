using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    using ITC = IndexingTestConstants;

    public abstract class FaultTolerantIndexingTwoSiloRunner : IndexingTestRunnerBase
    {
        protected FaultTolerantIndexingTwoSiloRunner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

#if ALLOW_FT_ACTIVE
        /// <summary>
        /// Tests basic functionality of HashIndexSingleBucket
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup1()
        {
            IPlayer1Grain p1 = base.GetGrain<IPlayer1Grain>(1);
            await p1.SetLocation(ITC.Seattle);

            IPlayer1Grain p2 = base.GetGrain<IPlayer1Grain>(2);
            IPlayer1Grain p3 = base.GetGrain<IPlayer1Grain>(3);

            await p2.SetLocation(ITC.Seattle);
            await p3.SetLocation(ITC.SanFrancisco);

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer1Grain>(ITC.LocationProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCount<IPlayer1Grain, Player1Properties>(location, ITC.DelayUntilIndexesAreUpdatedLazily);

            Assert.Equal(2, await getLocationCount(ITC.Seattle));

            await p2.Deactivate();
            await Task.Delay(ITC.DelayUntilIndexesAreUpdatedLazily);

            Assert.Equal(2, await getLocationCount(ITC.Seattle));

            p2 = base.GetGrain<IPlayer1Grain>(2);
            Assert.Equal(ITC.Seattle, await p2.GetLocation());
            Assert.Equal(2, await getLocationCount(ITC.Seattle));
        }

        /// <summary>
        /// Tests basic functionality of ActiveHashIndexPartitionedPerSiloImpl with 1 Silo
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup2()
        {
            IPlayer2Grain p1 = base.GetGrain<IPlayer2Grain>(1);
            await p1.SetLocation(ITC.Tehran);

            IPlayer2Grain p2 = base.GetGrain<IPlayer2Grain>(2);
            IPlayer2Grain p3 = base.GetGrain<IPlayer2Grain>(3);

            await p2.SetLocation(ITC.Tehran);
            await p3.SetLocation(ITC.Yazd);

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer2Grain>(ITC.LocationProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCount<IPlayer2Grain, Player2Properties>(location, ITC.DelayUntilIndexesAreUpdatedLazily);

            base.Output.WriteLine("Before check 1");
            Assert.Equal(2, await getLocationCount(ITC.Tehran));

            await p2.Deactivate();

            await Task.Delay(ITC.DelayUntilIndexesAreUpdatedLazily);

            base.Output.WriteLine("Before check 2");
            Assert.Equal(1, await getLocationCount(ITC.Tehran));

            p2 = base.GetGrain<IPlayer2Grain>(2);
            base.Output.WriteLine("Before check 3");
            Assert.Equal(ITC.Tehran, await p2.GetLocation());

            base.Output.WriteLine("Before check 4");
            Assert.Equal(2, await getLocationCount(ITC.Tehran));
            base.Output.WriteLine("Done.");
        }
#endif // ALLOW_FT_ACTIVE

        /// <summary>
        /// Tests basic functionality of HashIndexPartitionedPerKey
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup4()
        {
            IPlayer3Grain p1 = base.GetGrain<IPlayer3Grain>(1);
            await p1.SetLocation(ITC.Seattle);

            IPlayer3Grain p2 = base.GetGrain<IPlayer3Grain>(2);
            IPlayer3Grain p3 = base.GetGrain<IPlayer3Grain>(3);

            await p2.SetLocation(ITC.Seattle);
            await p3.SetLocation(ITC.SanFrancisco);

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer3Grain>(ITC.LocationProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCount<IPlayer3Grain, Player3Properties>(location, ITC.DelayUntilIndexesAreUpdatedLazily);

            Assert.Equal(2, await getLocationCount(ITC.Seattle));

            await p2.Deactivate();
            await Task.Delay(ITC.DelayUntilIndexesAreUpdatedLazily);

            Assert.Equal(2, await getLocationCount(ITC.Seattle));

            p2 = base.GetGrain<IPlayer3Grain>(2);
            Assert.Equal(ITC.Seattle, await p2.GetLocation());

            Assert.Equal(2, await getLocationCount(ITC.Seattle));
        }

        /// <summary>
        /// Tests basic transactional functionality of HashIndexPartitionedPerKey
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

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCountTxn<IPlayer3GrainTransactional, Player3PropertiesTransactional>(location, ITC.DelayUntilIndexesAreUpdatedLazily);

            Assert.Equal(2, await getLocationCount(ITC.Seattle));

            await p2.Deactivate();
            await Task.Delay(ITC.DelayUntilIndexesAreUpdatedLazily);
            Assert.Equal(2, await getLocationCount(ITC.Seattle));   // Transactional indexes are always Total, so the count remains 2

            p2 = base.GetGrain<IPlayer3GrainTransactional>(2);
            Assert.Equal(ITC.Seattle, await p2.GetLocation());
            Assert.Equal(2, await getLocationCount(ITC.Seattle));
        }

        /// <summary>
        /// Tests basic functionality of HashIndexPartitionedPerKey
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup5()
        {
            //await base.StartAndWaitForSecondSilo();

            IPlayer3Grain p1 = base.GetGrain<IPlayer3Grain>(1);
            await p1.SetLocation(ITC.Seattle);

            IPlayer3Grain p2 = base.GetGrain<IPlayer3Grain>(2);
            IPlayer3Grain p3 = base.GetGrain<IPlayer3Grain>(3);
            IPlayer3Grain p4 = base.GetGrain<IPlayer3Grain>(4);
            IPlayer3Grain p5 = base.GetGrain<IPlayer3Grain>(5);

            await p2.SetLocation(ITC.Seattle);
            await p3.SetLocation(ITC.SanFrancisco);
            await p4.SetLocation(ITC.Tehran);
            await p5.SetLocation(ITC.Yazd);

            for (int i = 0; i < 100; ++i)
            {
                var tasks = new List<Task>();
                for (int j = 0; j < 10; ++j)
                {
                    p1 = base.GetGrain<IPlayer3Grain>(j);
                    tasks.Add(p1.SetLocation(ITC.Yazd + i + "-" + j));
                }
                await Task.WhenAll(tasks);
            }
        }
    }
}
