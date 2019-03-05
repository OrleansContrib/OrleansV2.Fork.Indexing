using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    using ITC = IndexingTestConstants;

    public abstract class LazyIndexingTwoSiloRunner : IndexingTestRunnerBase
    {
        protected LazyIndexingTwoSiloRunner(BaseIndexingFixture fixture, ITestOutputHelper output)
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

            IPlayer2GrainNonFaultTolerantLazy p1 = base.GetGrain<IPlayer2GrainNonFaultTolerantLazy>(1);
            await p1.SetLocation(ITC.Seattle);

            IPlayer2GrainNonFaultTolerantLazy p2 = base.GetGrain<IPlayer2GrainNonFaultTolerantLazy>(2);
            IPlayer2GrainNonFaultTolerantLazy p3 = base.GetGrain<IPlayer2GrainNonFaultTolerantLazy>(3);

            await p2.SetLocation(ITC.Seattle);
            await p3.SetLocation(ITC.SanFrancisco);

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer2GrainNonFaultTolerantLazy>(ITC.LocationProperty);

            Task<int> getLocationCount(string location) => this.GetPlayerLocationCount<IPlayer2GrainNonFaultTolerantLazy, Player2PropertiesNonFaultTolerantLazy>(location, ITC.DelayUntilIndexesAreUpdatedLazily);

            Assert.Equal(2, await getLocationCount(ITC.Seattle));

            await p2.Deactivate();
            await Task.Delay(ITC.DelayUntilIndexesAreUpdatedLazily);

            Assert.Equal(1, await getLocationCount(ITC.Seattle));

            p2 = base.GetGrain<IPlayer2GrainNonFaultTolerantLazy>(2);
            Assert.Equal(ITC.Seattle, await p2.GetLocation());

            Assert.Equal(2, await getLocationCount(ITC.Seattle));
        }
    }
}
