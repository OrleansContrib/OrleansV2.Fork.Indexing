using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.Indexing.Facet;
using System;

namespace Orleans.Indexing.Tests.MultiInterface
{
    public abstract class TestEmployeeGrain<TGrainState> : Grain
        where TGrainState : class, IEmployeeGrainState, new()
    {
        // This is populated by Orleans.Indexing with the indexes from the implemented interfaces on this class.
        private readonly IIndexedState<TGrainState> indexedState;

        private IEmployeeGrainState State => indexedState.State;

        // This illustrates implementing the Grain interfaces to get and set the properties.
        #region IPersonInterface
        public Task<string> GetName() => Task.FromResult(this.State.Name);
        public Task SetName(string value) { this.State.Name = value; return Task.CompletedTask; }

        public Task<int> GetAge() => Task.FromResult(this.State.Age);
        public Task SetAge(int value) { this.State.Age = value; return Task.CompletedTask; }
        #endregion IPersonInterface

        #region IJobInterface
        public Task<string> GetTitle() => Task.FromResult(this.State.Title);
        public Task SetTitle(string value) { this.State.Title = value; return Task.CompletedTask; }

        public Task<string> GetDepartment() => Task.FromResult(this.State.Department);
        public Task SetDepartment(string value) { this.State.Department = value; return Task.CompletedTask; }
        #endregion IJobInterface

        #region IEmployeeProperties
        public Task<int> GetEmployeeId() => Task.FromResult(this.State.EmployeeId);
        public Task SetEmployeeId(int value) { this.State.EmployeeId = value; return Task.CompletedTask; }
        #endregion IEmployeeProperties

        #region IEmployeeGrainState - not indexed
        public Task<int> GetSalary() => Task.FromResult(this.State.Salary);
        public Task SetSalary(int value) { this.State.Salary = value; return Task.CompletedTask; }
        #endregion IEmployeeGrainState - not indexed

        public Task WriteState() => this.indexedState.WriteAsync();

        public Task Deactivate() { base.DeactivateOnIdle(); return Task.CompletedTask; }

        protected TestEmployeeGrain(IIndexedState<TGrainState> indexedState) => this.indexedState = indexedState;

        #region Facet methods - required overrides of Grain<TGrainState>
        public override Task OnActivateAsync() => this.indexedState.OnActivateAsync(this, base.OnActivateAsync);
        public override Task OnDeactivateAsync() => this.indexedState.OnDeactivateAsync(() => Task.CompletedTask);
        #endregion Facet methods - required overrides of Grain<TGrainState>

        #region Required shims for IIndexableGrain methods for fault tolerance
        public Task<Immutable<System.Collections.Generic.HashSet<Guid>>> GetActiveWorkflowIdsSet() => this.indexedState.GetActiveWorkflowIdsSet();
        public Task RemoveFromActiveWorkflowIds(System.Collections.Generic.HashSet<Guid> removedWorkflowId) => this.indexedState.RemoveFromActiveWorkflowIds(removedWorkflowId);
        #endregion Required shims for IIndexableGrain methods for fault tolerance
    }
}
