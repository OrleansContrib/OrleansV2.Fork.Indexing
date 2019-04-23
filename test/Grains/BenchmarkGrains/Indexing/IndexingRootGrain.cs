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

            // Property values are unique by interface/property.
            var propertyValue = grainId;

            await grain.SetId1(propertyValue);
            if (numPropertiesPerGrain >= 2) await grain.SetId2(propertyValue);
            if (numPropertiesPerGrain >= 3) await grain.SetId3(propertyValue);
            if (numPropertiesPerGrain >= 4) await grain.SetId4(propertyValue);
                        
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
