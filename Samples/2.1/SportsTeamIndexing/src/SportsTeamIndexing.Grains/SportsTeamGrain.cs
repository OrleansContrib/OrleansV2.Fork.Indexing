using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;
using Orleans.Indexing.Facet;
using Orleans.Indexing;

using SportsTeamIndexing.Interfaces;

namespace SportsTeamIndexing.Grains
{
    public class SportsTeamGrain : Grain, ISportsTeamGrain, IIndexableGrain<SportsTeamIndexedProperties>
    {
        // This must be configured when setting up the Silo; see SiloHost.cs StartSilo().
        public const string GrainStoreName = "SportsTeamGrainMemoryStore";

        private readonly IIndexedState<SportsTeamState> indexedState;

        public SportsTeamGrain(
#if USE_TRANSACTIONS
            [TransactionalIndexedState("stateName", GrainStoreName)]
#else
            [NonFaultTolerantWorkflowIndexedState("stateName", GrainStoreName)]
#endif
            IIndexedState<SportsTeamState> indexedState) => this.indexedState = indexedState;

        #region indexed as a computed property
        public Task<string> GetQualifiedName() => this.indexedState.PerformRead(state => state.QualifiedName);
        #endregion indexed as a computed property

        #region indexed as single properties
        public Task<string> GetName() => this.indexedState.PerformRead(state => state.Name);
        public Task SetName(string value) => this.indexedState.PerformUpdate(state => state.Name = value);

        public Task<string> GetLocation() => this.indexedState.PerformRead(state => state.Location);
        public Task SetLocation(string value) => this.indexedState.PerformUpdate(state => state.Location = value);

        public Task<string> GetLeague() => this.indexedState.PerformRead(state => state.League);
        public Task SetLeague(string value) => this.indexedState.PerformUpdate(state => state.League = value);
        #endregion indexed as single properties

        #region not indexed
        public Task<string> GetVenue() => this.indexedState.PerformRead(state => state.Venue);
        public Task SetVenue(string value) => this.indexedState.PerformUpdate(state => state.Venue = value);
        #endregion not indexed

        public Task<SportsTeamState> ReadStateAsync() => this.indexedState.PerformRead(state => state);

        public Task WriteStateAsync(SportsTeamState value) => this.indexedState.PerformUpdate(state =>
                                                                {
                                                                    if (value.Name != null) state.Name = value.Name;
                                                                    if (value.Name != null) state.Location = value.Location;
                                                                    if (value.Name != null) state.League = value.League;
                                                                    if (value.Name != null) state.Venue = value.Venue;
                                                                });

        #region required implementations of IIndexableGrain methods; they are only called for FaultTolerant index writing
        public Task<Immutable<System.Collections.Generic.HashSet<Guid>>> GetActiveWorkflowIdsSet() => this.indexedState.GetActiveWorkflowIdsSet();
        public Task RemoveFromActiveWorkflowIds(System.Collections.Generic.HashSet<Guid> removedWorkflowId) => this.indexedState.RemoveFromActiveWorkflowIds(removedWorkflowId);
        #endregion required implementations of IIndexableGrain methods
    }
}
