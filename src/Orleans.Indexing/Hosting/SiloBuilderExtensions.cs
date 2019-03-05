using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Indexing.Facet;

namespace Orleans.Indexing
{
    public static class SiloBuilderExtensions
    {
        /// <summary>
        /// Configure silo to use indexing using a configure action.
        /// </summary>
        public static ISiloHostBuilder UseIndexing(this ISiloHostBuilder builder, Action<IndexingOptions> configureOptions)
            => UseIndexing(builder, ob => ob.Configure(configureOptions));

        /// <summary>
        /// Configure silo to use indexing using a configuration builder.
        /// </summary>
        public static ISiloHostBuilder UseIndexing(this ISiloHostBuilder builder, Action<OptionsBuilder<IndexingOptions>> configureAction = null)
        {
            return builder.AddSimpleMessageStreamProvider(IndexingConstants.INDEXING_STREAM_PROVIDER_NAME)
                .AddMemoryGrainStorage(IndexingConstants.INDEXING_WORKFLOWQUEUE_STORAGE_PROVIDER_NAME)
                .AddMemoryGrainStorage(IndexingConstants.INDEXING_STORAGE_PROVIDER_NAME)
                .AddMemoryGrainStorage(IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)
                .ConfigureServices(services => services.UseIndexing(configureAction))
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(SiloBuilderExtensions).Assembly));
        }

        /// <summary>
        /// Configure silo services to use indexing using a configuration builder.
        /// </summary>
        private static IServiceCollection UseIndexing(this IServiceCollection services, Action<OptionsBuilder<IndexingOptions>> configureAction = null)
        {
            configureAction?.Invoke(services.AddOptions<IndexingOptions>(IndexingConstants.INDEXING_OPTIONS_NAME));
            services.AddSingleton<IndexFactory>()
                    .AddFromExisting<IIndexFactory, IndexFactory>();
            services.AddSingleton<SiloIndexManager>()
                    .AddFromExisting<ILifecycleParticipant<ISiloLifecycle>, SiloIndexManager>();
            services.AddFromExisting<IndexManager, SiloIndexManager>();

            // Facet Factory and Mappers
            services.AddTransient<IIndexedStateFactory, IndexedStateFactory>()
                    .AddSingleton(typeof(IAttributeToFactoryMapper<NonFaultTolerantWorkflowIndexedStateAttribute>),
                                  typeof(NonFaultTolerantWorkflowIndexedStateAttributeMapper))
                    .AddSingleton(typeof(IAttributeToFactoryMapper<FaultTolerantWorkflowIndexedStateAttribute>),
                                  typeof(FaultTolerantWorkflowIndexedStateAttributeMapper));
            return services;
        }
    }
}
