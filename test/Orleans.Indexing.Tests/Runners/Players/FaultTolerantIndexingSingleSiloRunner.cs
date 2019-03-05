using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    using ITC = IndexingTestConstants;

    public abstract class FaultTolerantIndexingSingleSiloRunner : IndexingTestRunnerBase
    {
        protected FaultTolerantIndexingSingleSiloRunner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

#if ALLOW_FT_ACTIVE
        /// <summary>
        /// Tests basic functionality of ActiveHashIndexPartitionedPerSiloImpl with 2 Silos
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup3()
        {
            await base.StartAndWaitForSecondSilo();

            IPlayer2Grain p1 = base.GetGrain<IPlayer2Grain>(1);
            await p1.SetLocation(ITC.Seattle);

            IPlayer2Grain p2 = base.GetGrain<IPlayer2Grain>(2);
            IPlayer2Grain p3 = base.GetGrain<IPlayer2Grain>(3);

            await p2.SetLocation(ITC.Seattle);
            await p3.SetLocation(ITC.SanFrancisco);

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer2Grain>(ITC.LocationProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCount<IPlayer2Grain, Player2Properties>(location, ITC.DelayUntilIndexesAreUpdatedLazily);

            base.Output.WriteLine("Before check 1");
            Assert.Equal(2, await getLocationCount(ITC.Seattle));

            await p2.Deactivate();
            await Task.Delay(ITC.DelayUntilIndexesAreUpdatedLazily);

            base.Output.WriteLine("Before check 2");
            Assert.Equal(1, await getLocationCount(ITC.Seattle));

            p2 = base.GetGrain<IPlayer2Grain>(2);
            base.Output.WriteLine("Before check 3");
            Assert.Equal(ITC.Seattle, await p2.GetLocation());

            base.Output.WriteLine("Before check 4");
            Assert.Equal(2, await getLocationCount(ITC.Seattle));
            base.Output.WriteLine("Done.");
        }
#endif // ALLOW_FT_ACTIVE
    }
}
