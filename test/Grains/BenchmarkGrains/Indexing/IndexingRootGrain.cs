using System.Threading.Tasks;
using BenchmarkGrainInterfaces.Indexing;
using Orleans;
using Orleans.Concurrency;

namespace BenchmarkGrains.Indexing
{
    [Reentrant]
    [StatelessWorker]
    public class IndexingRootGrain<TGrainInterface> : Grain, IIndexingRootGrain<TGrainInterface> where TGrainInterface : IIndexingGrain, IGrainWithIntegerKey
    {
        public async Task Start(int grainId, int numPropertiesPerGrain)
        {
            var grain = base.GrainFactory.GetGrain<TGrainInterface>(grainId);
            await grain.SetId1(grainId * 100 + 1);
            if (numPropertiesPerGrain >= 2) await grain.SetId2(grainId * 100 + 2);
            if (numPropertiesPerGrain >= 3) await grain.SetId3(grainId * 100 + 3);
            if (numPropertiesPerGrain >= 4) await grain.SetId4(grainId * 100 + 4);

            switch (grain)
            {
                case IIndexingGrainTransactional_PK txnPK:
                    await txnPK.WriteStateTxnAsync();
                    break;
                case IIndexingGrainTransactional_SB txnSB:
                    await txnSB.WriteStateTxnAsync();
                    break;
                default:
                    await grain.WriteStateAsync();
                    break;
            }
        }
    }
}
