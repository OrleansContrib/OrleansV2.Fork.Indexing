using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using System.Threading;

namespace Orleans.Indexing.Tests
{
    using ITC = IndexingTestConstants;

    public abstract class SimpleIndexingTwoSiloRunner : IndexingTestRunnerBase
    {
        protected SimpleIndexingTwoSiloRunner(BaseIndexingFixture fixture, ITestOutputHelper output)
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

            IPlayer_NFT_AI_EG_PS p1 = base.GetGrain<IPlayer_NFT_AI_EG_PS>(1);
            await p1.SetLocation(ITC.Seattle);

            IPlayer_NFT_AI_EG_PS p2 = base.GetGrain<IPlayer_NFT_AI_EG_PS>(2);
            IPlayer_NFT_AI_EG_PS p3 = base.GetGrain<IPlayer_NFT_AI_EG_PS>(3);

            await p2.SetLocation(ITC.Seattle);
            await p3.SetLocation(ITC.SanFrancisco);

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer_NFT_AI_EG_PS>(ITC.LocationProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCount<IPlayer_NFT_AI_EG_PS, PlayerProperties_NFT_AI_EG_PS>(location);

            Assert.Equal(2, await getLocationCount(ITC.Seattle));

            await p2.Deactivate();
            Thread.Sleep(1000);
            Assert.Equal(1, await getLocationCount(ITC.Seattle));

            p2 = base.GetGrain<IPlayer_NFT_AI_EG_PS>(2);
            Assert.Equal(ITC.Seattle, await p2.GetLocation());
            Assert.Equal(2, await getLocationCount(ITC.Seattle));
        }
    }
}
