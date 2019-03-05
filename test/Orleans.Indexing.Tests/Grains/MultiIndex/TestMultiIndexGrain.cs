using Orleans.Concurrency;
using Orleans.Indexing.Facet;
using Orleans.Providers;
using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Orleans.Indexing.Tests
{
    public abstract class TestMultiIndexGrainNonFaultTolerant<TGrainState> : TestMultiIndexGrain<TGrainState>
        where TGrainState : class, ITestMultiIndexState, new()
    {
        public TestMultiIndexGrainNonFaultTolerant(IIndexedState<TGrainState> indexedState) : base(indexedState)
            => Debug.Assert(this.GetType().GetConsistencyScheme() == ConsistencyScheme.NonFaultTolerantWorkflow);
    }

    public abstract class TestMultiIndexGrainFaultTolerant<TGrainState> : TestMultiIndexGrain<TGrainState>
        where TGrainState : class, ITestMultiIndexState, new()
    {
        public TestMultiIndexGrainFaultTolerant(IIndexedState<TGrainState> indexedState) : base(indexedState)
            => Debug.Assert(this.GetType().GetConsistencyScheme() == ConsistencyScheme.FaultTolerantWorkflow);
    }

    public abstract class TestMultiIndexGrain<TGrainState> : Grain, ITestMultiIndexGrain
        where TGrainState : class, ITestMultiIndexState, new()
    {
        private readonly TestMultiIndexGrainBase<TGrainState> testBase;
        private TGrainState State => this.testBase.IndexedState.State;

        public Task<string> GetUnIndexedString() => Task.FromResult(this.State.UnIndexedString);
        public Task SetUnIndexedString(string value) => this.testBase.SetProperty(() => this.State.UnIndexedString = value, retry:false);

        public Task<int> GetUniqueInt() => Task.FromResult(this.State.UniqueInt);
        public Task SetUniqueInt(int value) => this.testBase.SetProperty(() => this.State.UniqueInt = value, retry: this.testBase.IsUniqueIntIndexed);

        public Task<string> GetUniqueString() => Task.FromResult(this.State.UniqueString);
        public Task SetUniqueString(string value) => this.testBase.SetProperty(() => this.State.UniqueString = value, retry: this.testBase.IsUniqueStringIndexed);

        public Task<int> GetNonUniqueInt() => Task.FromResult(this.State.NonUniqueInt);
        public Task SetNonUniqueInt(int value) => this.testBase.SetProperty(() => this.State.NonUniqueInt = value, retry: this.testBase.IsNonUniqueIntIndexed);

        public Task<string> GetNonUniqueString() => Task.FromResult(this.State.NonUniqueString);
        public Task SetNonUniqueString(string value) => this.testBase.SetProperty(() => this.State.NonUniqueString = value, retry: this.testBase.IsNonUniqueStringIndexed);

        public Task Deactivate()
        {
            base.DeactivateOnIdle();
            return Task.CompletedTask;
        }

        public TestMultiIndexGrain(IIndexedState<TGrainState> indexedState) =>
            this.testBase = new TestMultiIndexGrainBase<TGrainState>(this.GetType(), indexedState);

        #region Facet methods - required overrides of Grain<TGrainState>
        public override Task OnActivateAsync() => this.testBase.IndexedState.OnActivateAsync(this, base.OnActivateAsync);
        public override Task OnDeactivateAsync() => this.testBase.IndexedState.OnDeactivateAsync(() => Task.CompletedTask);
        #endregion Facet methods - required overrides of Grain<TGrainState>

        #region Required shims for IIndexableGrain methods for fault tolerance
        public Task<Immutable<System.Collections.Generic.HashSet<Guid>>> GetActiveWorkflowIdsSet() => this.testBase.IndexedState.GetActiveWorkflowIdsSet();
        public Task RemoveFromActiveWorkflowIds(System.Collections.Generic.HashSet<Guid> removedWorkflowId) => this.testBase.IndexedState.RemoveFromActiveWorkflowIds(removedWorkflowId);
        #endregion Required shims for IIndexableGrain methods for fault tolerance
    }
}
