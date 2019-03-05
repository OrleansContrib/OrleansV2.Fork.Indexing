using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.TestingHost;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using TestExtensions;
using UnitTests.GrainInterfaces;
using UnitTests.Grains;

namespace Orleans.Indexing.Tests
{
    public abstract class BaseIndexingFixture : BaseTestClusterFixture
    {
        protected TestClusterBuilder ConfigureTestClusterForIndexing(TestClusterBuilder builder)
        {
            // Currently nothing
            //builder.Options.InitialSilosCount = 1;    // For debugging if needed
            return builder;
        }

        internal static ISiloHostBuilder Configure(ISiloHostBuilder hostBuilder, string databaseName = null)
        {
            if (!TestDefaultConfiguration.GetValue("CosmosDBEndpoint", out string cosmosDBEndpoint)
                || !TestDefaultConfiguration.GetValue("CosmosDBKey", out string cosmosDBKey))
            {
                throw new IndexConfigurationException("CosmosDB connection values are not specified");
            }

            hostBuilder.AddMemoryGrainStorage(IndexingTestConstants.GrainStore)
                       .AddMemoryGrainStorage("PubSubStore") // PubSubStore service is needed for the streams underlying OrleansQueryResults
                       .ConfigureLogging(loggingBuilder =>
                       {
                           loggingBuilder.SetMinimumLevel(LogLevel.Information);
                           loggingBuilder.AddDebug();
                       })
                       .ConfigureApplicationParts(parts =>
                       {
                           parts.AddApplicationPart(typeof(BaseIndexingFixture).Assembly);
                           parts.AddApplicationPart(typeof(ISimpleGrain).Assembly);
                           parts.AddApplicationPart(typeof(SimpleGrain).Assembly);
                       });

            return databaseName != null
                ? hostBuilder.AddCosmosDBGrainStorage(IndexingTestConstants.CosmosDBGrainStorage, opt =>
                    {
                        opt.AccountEndpoint = cosmosDBEndpoint;
                        opt.AccountKey = cosmosDBKey;
                        opt.ConnectionMode = Microsoft.Azure.Documents.Client.ConnectionMode.Gateway;
                        opt.DropDatabaseOnInit = true;
                        opt.AutoUpdateStoredProcedures = true;
                        opt.CanCreateResources = true;
                        opt.DB = databaseName;
                        opt.InitStage = ServiceLifecycleStage.RuntimeStorageServices;
                        opt.StateFieldsToIndex.AddRange(GetDSMIStateFieldsToIndex());
                    })
                : hostBuilder;
        }

        internal static IClientBuilder Configure(IClientBuilder clientBuilder)
        {
            return clientBuilder.ConfigureLogging(loggingBuilder =>
                                {
                                    loggingBuilder.SetMinimumLevel(LogLevel.Information);
                                    loggingBuilder.AddDebug();
                                })
                                .ConfigureApplicationParts(parts =>
                                {
                                    parts.AddApplicationPart(typeof(BaseIndexingFixture).Assembly);
                                    parts.AddApplicationPart(typeof(ISimpleGrain).Assembly);
                                    parts.AddApplicationPart(typeof(SimpleGrain).Assembly);
                                });
        }

        // Code below adapted from ApplicationPartsIndexableGrainLoader to identify the necessary fields for the DSMI storage
        // provider to index.

        private static IEnumerable<string> GetDSMIStateFieldsToIndex()
        {
            var grainClassTypes = typeof(BaseIndexingFixture).Assembly.GetConcreteGrainClasses(logger: null).ToArray();

            // Orleans.CosmosDB appends the field names to "State."; thus we do not prepend the interface names.
            var interfacesToIndexedPropertyNames = new Dictionary<Type, string[]>();
            foreach (var grainClassType in grainClassTypes)
            {
                GetDSMIFieldsForASingleGrainType(grainClassType, interfacesToIndexedPropertyNames);
            }
            return new HashSet<string>(interfacesToIndexedPropertyNames.Where(kvp => kvp.Value.Length > 0).SelectMany(kvp => kvp.Value));
        }

        internal static void GetDSMIFieldsForASingleGrainType(Type grainClassType, Dictionary<Type, string[]> interfacesToIndexedPropertyNames)
        {
            foreach (var (grainInterfaceType, propertiesClassType) in ApplicationPartsIndexableGrainLoader.EnumerateIndexedInterfacesForAGrainClassType(grainClassType)
                                                                        .Where(tup => !interfacesToIndexedPropertyNames.ContainsKey(tup.interfaceType)))
            {
                interfacesToIndexedPropertyNames[grainInterfaceType] = propertiesClassType.GetProperties()
                                                                        .Where(propInfo => propInfo.GetCustomAttributes<StorageManagedIndexAttribute>(inherit: false).Any())
                                                                        .Select(propInfo => IndexingConstants.UserStatePrefix + propInfo.Name)
                                                                        .ToArray();
            }
        }
    }
}
