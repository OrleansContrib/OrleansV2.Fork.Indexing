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
            Thread.Sleep(ITC.DelayUntilIndexesAreUpdatedLazily);
            Assert.Equal(1, await getLocationCount(ITC.Seattle));

            p2 = base.GetGrain<IPlayer1GrainNonFaultTolerant>(2);
            Assert.Equal(ITC.Seattle, await p2.GetLocation());
            Assert.Equal(2, await getLocationCount(ITC.Seattle));
        }

        /// <summary>
        /// Tests basic functionality of Transactional HashIndexSingleBucket
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing"), TestCategory("TransactionalIndexing")]
        public async Task Test_Indexing_IndexLookup1_Txn()
        {
            var p1 = base.GetGrain<IPlayer1GrainTransactional>(1);
            await p1.SetLocation(ITC.Seattle);

            var p2 = base.GetGrain<IPlayer1GrainTransactional>(2);
            var p3 = base.GetGrain<IPlayer1GrainTransactional>(3);

            await p2.SetLocation(ITC.Seattle);
            await p3.SetLocation(ITC.SanFrancisco);

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer1GrainTransactional>(ITC.LocationProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCountTxn<IPlayer1GrainTransactional, Player1PropertiesTransactional>(location);

            Assert.Equal(2, await getLocationCount(ITC.Seattle));

            await p2.Deactivate();
            Thread.Sleep(ITC.DelayUntilIndexesAreUpdatedLazily);
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
            Thread.Sleep(1000);
            Assert.Equal(1, await getLocationCount(ITC.Seattle));

            p2 = base.GetGrain<IPlayer3GrainNonFaultTolerant>(2);
            Assert.Equal(ITC.Seattle, await p2.GetLocation());
            Assert.Equal(2, await getLocationCount(ITC.Seattle));
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
            Thread.Sleep(1000);
            Assert.Equal(2, await getLocationCount(ITC.Seattle));   // Transactional indexes are always Total, so the count remains 2

            p2 = base.GetGrain<IPlayer3GrainTransactional>(2);
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

        /// <summary>
        /// Tests basic functionality of HashIndexPartitionedPerKey with two indexes
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup5_NFT_Eager()
        {
            await lookup5<IPlayer5GrainNonFaultTolerant, Player5PropertiesNonFaultTolerant>();
        }

        /// <summary>
        /// Tests basic functionality of HashIndexPartitionedPerKey with two indexes
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup5_NFT_Lazy()
        {
            await lookup5<IPlayer5GrainNonFaultTolerantLazy, Player5PropertiesNonFaultTolerantLazy>();
        }

        private async Task lookup5<TIGrain, TProperties>() where TIGrain : IGrainWithIntegerKey, IPlayerGrain, IIndexableGrain
                                                           where TProperties : IPlayerProperties
        { 
            var p1 = base.GetGrain<TIGrain>(1);
            await p1.SetLocation(ITC.Seattle);
            await p1.SetScore(42);

            var p2 = base.GetGrain<TIGrain>(2);
            var p3 = base.GetGrain<TIGrain>(3);

            await p2.SetLocation(ITC.Seattle);
            await p2.SetScore(34);
            await p3.SetLocation(ITC.SanFrancisco);
            await p3.SetScore(34);

            var locIdx = await base.GetAndWaitForIndex<string, TIGrain>(ITC.LocationProperty);
            var scoreIdx = await base.GetAndWaitForIndex<int, TIGrain>(ITC.ScoreProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCount<TIGrain, TProperties>(location);
            Task<int> getScoreCount(int score) => this.GetPlayerScoreCount<TIGrain, TProperties>(score);

            Assert.Equal(2, await getLocationCount(ITC.Seattle));
            Assert.Equal(2, await getScoreCount(34));

            await p2.Deactivate();
            Thread.Sleep(1000);
            Assert.Equal(1, await getLocationCount(ITC.Seattle));
            Assert.Equal(1, await getScoreCount(34));

            p2 = base.GetGrain<TIGrain>(2);
            Assert.Equal(ITC.Seattle, await p2.GetLocation());
            Assert.Equal(2, await getLocationCount(ITC.Seattle));
            Assert.Equal(2, await getScoreCount(34));

            // Test updates
            await p2.SetLocation(ITC.SanFrancisco);
            await p2.SetScore(42);

            Assert.Equal(1, await getLocationCount(ITC.Seattle));
            Assert.Equal(1, await getScoreCount(34));

            Assert.Equal(2, await getLocationCount(ITC.SanFrancisco));
            Assert.Equal(2, await getScoreCount(42));
        }

        /// <summary>
        /// Tests basic functionality of HashIndexPartitionedPerKey with two indexes
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup5_Txn()
        {
            var p1 = base.GetGrain<IPlayer5GrainTransactional>(1);
            await p1.SetLocation(ITC.Seattle);
            await p1.SetScore(42);

            var p2 = base.GetGrain<IPlayer5GrainTransactional>(2);
            var p3 = base.GetGrain<IPlayer5GrainTransactional>(3);

            await p2.SetLocation(ITC.Seattle);
            await p2.SetScore(34);
            await p3.SetLocation(ITC.SanFrancisco);
            await p3.SetScore(34);

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer5GrainTransactional>(ITC.LocationProperty);
            var scoreIdx = await base.GetAndWaitForIndex<int, IPlayer5GrainTransactional>(ITC.ScoreProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCountTxn<IPlayer5GrainTransactional, Player5PropertiesTransactional>(location);
            Task<int> getScoreCount(int score) => this.GetPlayerScoreCountTxn<IPlayer5GrainTransactional, Player5PropertiesTransactional>(score);

            Assert.Equal(2, await getLocationCount(ITC.Seattle));
            Assert.Equal(2, await getScoreCount(34));

            await p2.Deactivate();
            Thread.Sleep(1000);
            Assert.Equal(2, await getLocationCount(ITC.Seattle));   // Transactional indexes are always Total, so the count remains 2
            Assert.Equal(2, await getScoreCount(34));               // Transactional indexes are always Total, so the count remains 2 

            p2 = base.GetGrain<IPlayer5GrainTransactional>(2);
            Assert.Equal(ITC.Seattle, await p2.GetLocation());
            Assert.Equal(2, await getLocationCount(ITC.Seattle));
            Assert.Equal(2, await getScoreCount(34));

            // Test updates
            await p2.SetLocation(ITC.SanFrancisco);
            await p2.SetScore(42);

            Assert.Equal(1, await getLocationCount(ITC.Seattle));
            Assert.Equal(1, await getScoreCount(34));

            Assert.Equal(2, await getLocationCount(ITC.SanFrancisco));
            Assert.Equal(2, await getScoreCount(42));
        }
    }
}
