using Orleans.TestingHost;
using Orleans.Hosting;
using Microsoft.Extensions.Configuration;
using System;

namespace Orleans.Indexing.Tests
{
    public class WorkflowIndexingFixture : BaseIndexingFixture
    {
        internal virtual void AddSiloBuilderConfigurator(TestClusterBuilder builder) => builder.AddSiloBuilderConfigurator<SiloBuilderConfiguratorWf>();

        protected override void ConfigureTestCluster(TestClusterBuilder builder)
        {
            base.ConfigureTestClusterForIndexing(builder);
            AddSiloBuilderConfigurator(builder);
            builder.AddClientBuilderConfigurator<ClientBuilderConfiguratorWf>();
        }

        private class SiloBuilderConfiguratorWf : ISiloBuilderConfigurator
        {
            public void Configure(ISiloHostBuilder hostBuilder) =>
                BaseIndexingFixture.Configure(hostBuilder)
                                   .UseIndexing(indexingOptions => ConfigureBasicOptions(indexingOptions));
        }

        private class ClientBuilderConfiguratorWf : IClientBuilderConfigurator
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

    public class WorkflowDSMIndexingFixture : WorkflowIndexingFixture
    {
        internal class SiloBuilderConfiguratorWfDSMI : ISiloBuilderConfigurator
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

    public class WorkflowDSMIEGIndexingFixture : WorkflowDSMIndexingFixture
    {
        internal override void AddSiloBuilderConfigurator(TestClusterBuilder builder) => builder.AddSiloBuilderConfigurator<SiloBuilderConfiguratorWfDSMIEG>();

        internal class SiloBuilderConfiguratorWfDSMIEG : WorkflowDSMIEGIndexingFixture.SiloBuilderConfiguratorWfDSMI
        {
            internal override string GetDatabaseName() => DatabaseNamePrefix + "DSMI_EG";
        }
    }

    public class WorkflowDSMILZIndexingFixture : WorkflowDSMIndexingFixture
    {
        internal override void AddSiloBuilderConfigurator(TestClusterBuilder builder) => builder.AddSiloBuilderConfigurator<SiloBuilderConfiguratorWfDSMILZ>();

        internal class SiloBuilderConfiguratorWfDSMILZ : WorkflowDSMIEGIndexingFixture.SiloBuilderConfiguratorWfDSMI
        {
            internal override string GetDatabaseName() => DatabaseNamePrefix + "DSMI_LZ";
        }
    }
}
