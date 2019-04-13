using System;
using System.Threading.Tasks;
using System.Linq;
using Orleans.Hosting;
using Orleans.TestingHost;
using BenchmarkGrainInterfaces.Indexing;
using TestExtensions;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Indexing;
using Orleans.Configuration;
using Orleans.Runtime;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using System.Text;
using BenchmarkGrains.Indexing;
using Orleans;

namespace Benchmarks.Indexing
{
    public class IndexingBenchmark<TGrainInterface> where TGrainInterface : IIndexingGrain, IGrainWithIntegerKey
    {
        // PerKey has higher throughput. TODO: Consider adding commandline arguments for experimentation.
        private static IndexingParameters indexingParamsPK = new IndexingParameters(runs: 2, grains: 20000, props: 4, concurrentGrains: 5000);
        private static IndexingParameters indexingParamsSB = new IndexingParameters(runs: 2, grains: 400, props: 1, concurrentGrains: 20);
        private const int MaxHashBuckets = -1;

        private TestCluster host;
        private IndexingParameters indexingParams;

        public IndexingBenchmark(bool isPerKey, Action<IndexingBenchmark<TGrainInterface>> setupAction)
        {
            this.indexingParams = isPerKey ? indexingParamsPK : indexingParamsSB;
            Console.WriteLine($"    ({this.indexingParams}, MaxHashBuckets {(MaxHashBuckets < 0 ? "<unlimited>" : MaxHashBuckets.ToString())}");
            setupAction(this);
        }

        public void MemorySetup()
        {
            var builder = new TestClusterBuilder(4);
            builder.AddSiloBuilderConfigurator<SiloMemoryStorageConfigurator>();
            builder.AddSiloBuilderConfigurator<SiloIndexingConfigurator>();
            this.host = builder.Build();
            this.host.Deploy();
        }

        public void AzureSetup()
        {
            var builder = new TestClusterBuilder(4);
            builder.AddSiloBuilderConfigurator<SiloAzureStorageConfigurator>();
            builder.AddSiloBuilderConfigurator<SiloIndexingConfigurator>();
            this.host = builder.Build();
            this.host.Deploy();
        }

        public class SiloMemoryStorageConfigurator : ISiloBuilderConfigurator
        {
            public void Configure(ISiloHostBuilder hostBuilder) => hostBuilder.AddMemoryGrainStorageAsDefault();
        }

        public class SiloAzureStorageConfigurator : ISiloBuilderConfigurator
        {
            public void Configure(ISiloHostBuilder hostBuilder)
                => hostBuilder.AddAzureTableTransactionalStateStorageAsDefault(options
                                => options.ConnectionString = TestDefaultConfiguration.DataConnectionString);
        }

        public async Task RunAsync()
        {
            Console.WriteLine($"Cold Run.");
            await FullRunAsync();
            for (int i = 0; i < this.indexingParams.NumRuns; i++)
            {
                Console.WriteLine($"Warm Run {i + 1}.");
                await FullRunAsync();
            }
        }

        private async Task FullRunAsync()
        {
            int runners = Math.Max(1, (int)Math.Sqrt(this.indexingParams.NumConcurrentGrains));
            int grainsPerRunner = Math.Max(1, this.indexingParams.NumGrains / runners);
            IndexingReport[] reports = await Task.WhenAll(Enumerable.Range(0, runners).Select(i => RunAsync(i, grainsPerRunner, runners)));
            var finalReport = new IndexingReport();
            foreach (var report in reports)
            {
                finalReport.Succeeded += report.Succeeded;
                finalReport.Failed += report.Failed;
                finalReport.Elapsed = TimeSpan.FromMilliseconds(Math.Max(finalReport.Elapsed.TotalMilliseconds, report.Elapsed.TotalMilliseconds));
            }
            Console.WriteLine($"{finalReport.Succeeded} grains" +
                              $" ({this.indexingParams.NumProperties} indexed propert{(this.indexingParams.NumProperties == 1 ? "y" : "ies")} per grain)" +
                              $" in {finalReport.Elapsed.TotalMilliseconds}ms.");
            Console.WriteLine($"{(int)(finalReport.Succeeded * 1000 / finalReport.Elapsed.TotalMilliseconds)} grains per second.");
            Console.WriteLine(finalReport.Failed > 0 ? $"{finalReport.Failed} grains failed!!" : "All grains succeeded.");
        }

        public async Task<IndexingReport> RunAsync(int run, int grainsPerRunner, int concurrentPerRun)
        {
            var load = this.host.Client.GetGrain<IIndexingLoadGrain<TGrainInterface>>(Guid.NewGuid());
            await load.Generate(run, grainsPerRunner, this.indexingParams.NumProperties, concurrentPerRun);
            IndexingReport report = null;
            while (report == null)
            {
                await Task.Delay(TimeSpan.FromSeconds(10));
                report = await load.TryGetReport();
            }
            return report;
        }

        public void Teardown() => host.StopAllSilos();

        public sealed class SiloIndexingConfigurator : ISiloBuilderConfigurator
        {
            public void Configure(ISiloHostBuilder hostBuilder)
            {
                hostBuilder
                    .UseIndexing(indexingOptions => indexingOptions.MaxHashBuckets = MaxHashBuckets)
                    .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(IndexingState).Assembly).WithReferences())
                    .ConfigureServices(services => services.AddSingleton<TelemetryConsumer>())
                    .Configure<TelemetryOptions>(options => options.AddConsumer<TelemetryConsumer>())
                    .Configure<StatisticsOptions>(options => options.PerfCountersWriteInterval = TimeSpan.FromSeconds(3));
            }
        }
    }

    public class TelemetryConsumer : IMetricTelemetryConsumer
    {
        private readonly ILogger logger;

        public TelemetryConsumer(ILogger<TelemetryConsumer> logger)
            => this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        // TrackTrace/TrackMetric are unused (the first two are only referenced by the others)
        public void TrackTrace(string message) => this.logger.LogInformation(message);

        public void TrackTrace(string message, IDictionary<string, string> properties) => TrackTrace(PrintProperties(message, properties));

        public void TrackTrace(string message, Severity severity) => TrackTrace(message);

        public void TrackTrace(string message, Severity severity, IDictionary<string, string> properties) => TrackTrace(message, properties);

        public void TrackMetric(string name, double value, IDictionary<string, string> properties = null) => TrackTrace(PrintProperties(name, value, properties));

        public void TrackMetric(string name, TimeSpan value, IDictionary<string, string> properties = null) => TrackTrace(PrintProperties(name, value, properties));

        public void IncrementMetric(string name) => TrackTrace(name + $" - Increment");

        public void IncrementMetric(string name, double value) => TrackTrace(PrintProperties(name, value, null));

        public void DecrementMetric(string name) => TrackTrace(name + $" - Decrement");

        public void DecrementMetric(string name, double value) => TrackTrace(PrintProperties(name, value, null));

        // Flush/Close are unused
        public void Flush() { }

        public void Close() { }

        private static string PrintProperties<TValue>(string message, TValue value, IDictionary<string, string> properties)
            => AppendProperties(new StringBuilder(message + $" - Value: {value}"), properties).ToString();

        private static string PrintProperties(string message, IDictionary<string, string> properties)
            => AppendProperties(new StringBuilder(message), properties).ToString();

        private static StringBuilder AppendProperties(StringBuilder sb, IDictionary<string, string> properties)
        {
            if (properties == null || properties.Keys.Count == 0)
            {
                return sb;
            }

            sb.Append(" - Properties: {");
            foreach (var key in properties.Keys)
            {
                sb.Append(" ")
                  .Append(key)
                  .Append(" : ")
                  .Append(properties[key])
                  .Append(",");
            }
            return sb.Remove(sb.Length - 1, 1)
                     .Append(" }");
        }
    }
}
