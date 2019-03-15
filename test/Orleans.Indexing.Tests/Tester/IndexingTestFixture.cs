using Orleans.TestingHost;
using Orleans.Hosting;
using Microsoft.Extensions.Configuration;
using System;

namespace Orleans.Indexing.Tests
{
    public class IndexingTestFixture : BaseIndexingFixture
    {
        internal virtual void AddSiloBuilderConfigurator(TestClusterBuilder builder) => builder.AddSiloBuilderConfigurator<SiloBuilderConfigurator>();

        protected override void ConfigureTestCluster(TestClusterBuilder builder)
        {
            base.ConfigureTestClusterForIndexing(builder);
            AddSiloBuilderConfigurator(builder);
            builder.AddClientBuilderConfigurator<ClientBuilderConfigurator>();
        }

        private class SiloBuilderConfigurator : ISiloBuilderConfigurator
        {
            public void Configure(ISiloHostBuilder hostBuilder) =>
                BaseIndexingFixture.Configure(hostBuilder)
                                   .UseIndexing(indexingOptions => ConfigureBasicOptions(indexingOptions));
        }

        private class ClientBuilderConfigurator : IClientBuilderConfigurator
        {
            public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
                BaseIndexingFixture.Configure(clientBuilder)
                                   .UseIndexing(indexingOptions => ConfigureBasicOptions(indexingOptions));
        }

        protected static IndexingOptions ConfigureBasicOptions(IndexingOptions indexingOptions)
        {
            indexingOptions.MaxHashBuckets = 42;
            indexingOptions.NumWorkflowQueuesPerInterface = Math.Min(4, Environment.ProcessorCount); // Debugging startup is slow due to multiple GrainServices if this is high
            return indexingOptions; // allow chaining
        }
    }

    public class WorkflowDSMIndexingFixture : IndexingTestFixture
    {
        internal class SiloBuilderConfiguratorDSMI : ISiloBuilderConfigurator
        {
            // Each class is an Xunit collection receiving the class fixture; we drop the database, so must
            // use a different DB name for each class.
            protected const string DatabaseNamePrefix = "IndexStorageTest_";
            internal virtual string GetDatabaseName() => throw new NotImplementedException();

            public void Configure(ISiloHostBuilder hostBuilder) =>
                BaseIndexingFixture.Configure(hostBuilder, GetDatabaseName())
                                   .UseIndexing(indexingOptions => ConfigureBasicOptions(indexingOptions));
        }
    }

    public class WorkflowDSMI_EG_IndexingFixture : WorkflowDSMIndexingFixture
    {
        internal override void AddSiloBuilderConfigurator(TestClusterBuilder builder) => builder.AddSiloBuilderConfigurator<SiloBuilderConfiguratorDSMI_EG>();

        internal class SiloBuilderConfiguratorDSMI_EG : SiloBuilderConfiguratorDSMI
        {
            internal override string GetDatabaseName() => DatabaseNamePrefix + "DSMI_EG";
        }
    }

    public class WorkflowDSMI_LZ_IndexingFixture : WorkflowDSMIndexingFixture
    {
        internal override void AddSiloBuilderConfigurator(TestClusterBuilder builder) => builder.AddSiloBuilderConfigurator<SiloBuilderConfiguratorDSMI_LZ>();

        internal class SiloBuilderConfiguratorDSMI_LZ : SiloBuilderConfiguratorDSMI
        {
            internal override string GetDatabaseName() => DatabaseNamePrefix + "DSMI_LZ";
        }
    }
}
