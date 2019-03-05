using System;
using System.Threading.Tasks;

using Orleans;
using Orleans.Concurrency;
using Orleans.Providers;
using Orleans.Indexing.Facet;
using Orleans.Indexing;

using SportsTeamIndexing.Interfaces;

namespace SportsTeamIndexing.Grains
{
    [StorageProvider(ProviderName = SportsTeamGrain.GrainStoreName)]
    public class SportsTeamGrain : Grain, ISportsTeamGrain, IIndexableGrain<SportsTeamIndexedProperties>
    {
        // This must be configured when setting up the Silo; see SiloHost.cs StartSilo().
        public const string GrainStoreName = "SportsTeamGrainMemoryStore";

        IIndexedState<SportsTeamState> indexedState;

        SportsTeamState TeamState => indexedState.State;

        public SportsTeamGrain(
            [NonFaultTolerantWorkflowIndexedState(GrainStoreName)]
            IIndexedState<SportsTeamState> indexedState) => this.indexedState = indexedState;

        public Task<string> GetName() => Task.FromResult(this.TeamState.Name);
        public Task SetName(string name) => this.SetProperty(() => this.TeamState.Name = name);

        public Task<string> GetQualifiedName() => Task.FromResult(this.TeamState.QualifiedName);
        public Task SetQualifiedName(string name) => this.SetProperty(() => this.TeamState.QualifiedName = name);

        public Task<string> GetLocation() => Task.FromResult(this.TeamState.Location);
        public Task SetLocation(string location) => this.SetProperty(() => this.TeamState.Location = location);

        public Task<string> GetLeague() => Task.FromResult(this.TeamState.League);
        public Task SetLeague(string role) => this.SetProperty(() => this.TeamState.League = role);

        public Task<string> GetVenue() => Task.FromResult(this.TeamState.Venue);
        public Task SetVenue(string venue) => this.SetProperty(() => this.TeamState.Venue = venue);

        public async Task SaveAsync()
        {
            Console.WriteLine($"Grain {await this.GetQualifiedName()} SaveAsync");
            await this.indexedState.WriteAsync();
        }

        private Task SetProperty(Action action)
        {
            action();
            return Task.CompletedTask;
        }

        #region Facet methods - required overrides of Grain<TGrainState>
        public override Task OnActivateAsync() => this.indexedState.OnActivateAsync(this, () => Task.CompletedTask);
        public override Task OnDeactivateAsync() => this.indexedState.OnDeactivateAsync(() => Task.CompletedTask);
        #endregion Facet methods - required overrides of Grain<TGrainState>

        // TODO remove when facetization is complete; for now it just makes the compiler happy
        public Task<object> ExtractIndexImage(IIndexUpdateGenerator iUpdateGen) => throw new NotImplementedException();

        #region required implementations of IIndexableGrain methods; they are only called for FaultTolerant index writing
        public Task<Immutable<System.Collections.Generic.HashSet<Guid>>> GetActiveWorkflowIdsSet() => this.indexedState.GetActiveWorkflowIdsSet();
        public Task RemoveFromActiveWorkflowIds(System.Collections.Generic.HashSet<Guid> removedWorkflowId) => this.indexedState.RemoveFromActiveWorkflowIds(removedWorkflowId);
        #endregion required implementations of IIndexableGrain methods
    }
}
