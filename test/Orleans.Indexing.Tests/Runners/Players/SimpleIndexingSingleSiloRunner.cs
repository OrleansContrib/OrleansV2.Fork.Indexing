using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using System.Threading;

namespace Orleans.Indexing.Tests
{
    using ITC = IndexingTestConstants;

    public abstract class SimpleIndexingSingleSiloRunner : IndexingTestRunnerBase
    {
        protected SimpleIndexingSingleSiloRunner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

        /// <summary>
        /// Tests basic functionality of HashIndexSingleBucket
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup1()
        {
            var p1 = base.GetGrain<IPlayer1GrainNonFaultTolerant>(1);
            await p1.SetLocation(ITC.Seattle);

            var p2 = base.GetGrain<IPlayer1GrainNonFaultTolerant>(2);
            var p3 = base.GetGrain<IPlayer1GrainNonFaultTolerant>(3);

            await p2.SetLocation(ITC.Seattle);
            await p3.SetLocation(ITC.SanFrancisco);

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer1GrainNonFaultTolerant>(ITC.LocationProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCount<IPlayer1GrainNonFaultTolerant, Player1PropertiesNonFaultTolerant>(location);

            Assert.Equal(2, await getLocationCount(ITC.Seattle));

            await p2.Deactivate();
            Thread.Sleep(1000);
            Assert.Equal(1, await getLocationCount(ITC.Seattle));

            p2 = base.GetGrain<IPlayer1GrainNonFaultTolerant>(2);
            Assert.Equal(ITC.Seattle, await p2.GetLocation());
            Assert.Equal(2, await getLocationCount(ITC.Seattle));
        }

        /// <summary>
        /// Tests basic functionality of Transactional HashIndexSingleBucket
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup1_Txn()
        {
            var p1 = base.GetGrain<IPlayer1GrainTransactional>(1);
            await p1.SetLocation(ITC.Seattle);

            var p2 = base.GetGrain<IPlayer1GrainTransactional>(2);
            var p3 = base.GetGrain<IPlayer1GrainTransactional>(3);

            await p2.SetLocation(ITC.Seattle);
            await p3.SetLocation(ITC.SanFrancisco);

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer1GrainTransactional>(ITC.LocationProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCountTxn<IPlayer1GrainTransactional, Player1PropertiesNonFaultTolerant>(location);

            Assert.Equal(2, await getLocationCount(ITC.Seattle));

            await p2.Deactivate();
            Thread.Sleep(1000);
            Assert.Equal(2, await getLocationCount(ITC.Seattle));   // Transactional indexes are always Total, so the count remains 2

            p2 = base.GetGrain<IPlayer1GrainTransactional>(2);
            Assert.Equal(ITC.Seattle, await p2.GetLocation());
            Assert.Equal(2, await getLocationCount(ITC.Seattle));
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

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexDelete_Player4_UQ_NFT()
        {
            var p1 = base.GetGrain<IPlayer4GrainNonFaultTolerant>(1);
            await p1.SetLocation(ITC.Seattle);

            var p2 = base.GetGrain<IPlayer4GrainNonFaultTolerant>(2);
            await p2.SetLocation(ITC.Redmond);

            var p3 = base.GetGrain<IPlayer4GrainNonFaultTolerant>(3);
            await p3.SetLocation(ITC.SanFrancisco);

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer4GrainNonFaultTolerant>(ITC.LocationProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCount<IPlayer4GrainNonFaultTolerant, Player4PropertiesNonFaultTolerant>(location);

            Assert.Equal(1, await getLocationCount(ITC.Seattle));
            Assert.Equal(1, await getLocationCount(ITC.Redmond));

            await p2.Deactivate();
            Thread.Sleep(1000);

            Assert.Equal(1, await getLocationCount(ITC.Seattle));
            Assert.Equal(0, await getLocationCount(ITC.Redmond));

            p2 = base.GetGrain<IPlayer4GrainNonFaultTolerant>(2);
            Assert.Equal(ITC.Redmond, await p2.GetLocation());

            Assert.Equal(1, await getLocationCount(ITC.Seattle));
            Assert.Equal(1, await getLocationCount(ITC.Redmond));
        }
    }
}
