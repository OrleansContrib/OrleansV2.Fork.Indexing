using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;

namespace Orleans.Indexing.Facet
{
    public class IndexedStateFactory : IIndexedStateFactory
    {
        private readonly IGrainActivationContext activationContext;

        public IndexedStateFactory(IGrainActivationContext activationContext, ITypeResolver typeResolver, IGrainFactory grainFactory)
            => this.activationContext = activationContext;

        public INonFaultTolerantWorkflowIndexedState<TState> CreateNonFaultTolerantWorkflowIndexedState<TState>(IIndexedStateConfiguration config)
            where TState : class, new()
            => this.CreateIndexedState<NonFaultTolerantWorkflowIndexedState<TState>>(config);

        public IFaultTolerantWorkflowIndexedState<TState> CreateFaultTolerantWorkflowIndexedState<TState>(IIndexedStateConfiguration config)
            where TState : class, new()
            => this.CreateIndexedState<FaultTolerantWorkflowIndexedState<TState>>(config);

        private TWrappedIndexedStateImplementation CreateIndexedState<TWrappedIndexedStateImplementation>(IIndexedStateConfiguration config)
            => ActivatorUtilities.CreateInstance<TWrappedIndexedStateImplementation>(this.activationContext.ActivationServices, config);
    }
}
