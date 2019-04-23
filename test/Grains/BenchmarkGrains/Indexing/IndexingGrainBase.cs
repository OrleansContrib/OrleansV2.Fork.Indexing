using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BenchmarkGrainInterfaces.Indexing;
using Orleans;
using Orleans.Concurrency;
using Orleans.Indexing.Facet;

namespace BenchmarkGrains.Indexing
{
    public class IndexingGrainBase : Grain
    {
        private protected readonly IIndexedState<IndexingState> IndexedState;

        private int id1 = IndexingBenchmarkConstants.NullId;
        private int id2 = IndexingBenchmarkConstants.NullId;
        private int id3 = IndexingBenchmarkConstants.NullId;
        private int id4 = IndexingBenchmarkConstants.NullId;

        public IndexingGrainBase(IIndexedState<IndexingState> indexedState)
            => this.IndexedState = indexedState;

        private Task SetProperty(ref int property, int value)
        {
            property = value;
            return Task.CompletedTask;
        }

        public Task SetId1(int id) => this.SetProperty(ref this.id1, id);
        public Task SetId2(int id) => this.SetProperty(ref this.id2, id);
        public Task SetId3(int id) => this.SetProperty(ref this.id3, id);
        public Task SetId4(int id) => this.SetProperty(ref this.id4, id);

        public Task WriteStateAsync()
            => this.IndexedState.PerformUpdate(state =>
            {
                if (this.id1 != IndexingBenchmarkConstants.NullId) state.Id1 = this.id1;
                if (this.id2 != IndexingBenchmarkConstants.NullId) state.Id2 = this.id2;
                if (this.id3 != IndexingBenchmarkConstants.NullId) state.Id3 = this.id3;
                if (this.id4 != IndexingBenchmarkConstants.NullId) state.Id4 = this.id4;
                return true;
            });

        public Task WriteStateTxnAsync() => this.WriteStateAsync();

        public virtual Task<Immutable<HashSet<Guid>>> GetActiveWorkflowIdsSet()
            => this.IndexedState is IFaultTolerantWorkflowIndexedState<IndexingState> fts ? fts.GetActiveWorkflowIdsSet() : throw new NotImplementedException();

        public virtual Task RemoveFromActiveWorkflowIds(HashSet<Guid> removedWorkflowId)
            => this.IndexedState is IFaultTolerantWorkflowIndexedState<IndexingState> fts ? fts.RemoveFromActiveWorkflowIds(removedWorkflowId) : throw new NotImplementedException();
    }
}
