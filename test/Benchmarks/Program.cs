using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using BenchmarkDotNet.Running;
using Benchmarks.MapReduce;
using Benchmarks.Serialization;
using Benchmarks.Ping;
using Benchmarks.Transactions;
using Benchmarks.GrainStorage;
using Benchmarks.Indexing;
using BenchmarkGrainInterfaces.Indexing;

namespace Benchmarks
{
    class Program
    {
        // PerKey has higher throughput.
        private static IndexingParameters indexingParamsPK = new IndexingParameters(runs: 2, grains: 20000, props: 2, concurrentGrains: 5000);
        private static IndexingParameters indexingParamsSB = new IndexingParameters(runs: 2, grains: 400, props: 1, concurrentGrains: 20);
        private static bool suppressPause = false;

        private static readonly Dictionary<string, Action> _benchmarks = new Dictionary<string, Action>
        {
            ["MapReduce"] = () =>
            {
                RunBenchmark(
                "Running MapReduce benchmark", 
                () =>
                {
                    var mapReduceBenchmark = new MapReduceBenchmark();
                    mapReduceBenchmark.BenchmarkSetup();
                    return mapReduceBenchmark;
                },
                benchmark => benchmark.Bench().GetAwaiter().GetResult(),
                benchmark => benchmark.Teardown());
            },
            ["Serialization"] = () =>
            {
                BenchmarkRunner.Run<SerializationBenchmarks>();
            },
            ["Transactions.Memory"] = () =>
            {
                RunBenchmark(
                "Running Transactions benchmark",
                () =>
                {
                    var benchmark = new TransactionBenchmark(2, 20000, 5000);
                    benchmark.MemorySetup();
                    return benchmark;
                },
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(),
                benchmark => benchmark.Teardown());
            },
            ["Transactions.Memory.Throttled"] = () =>
            {
                RunBenchmark(
                "Running Transactions benchmark",
                () =>
                {
                    var benchmark = new TransactionBenchmark(2, 200000, 15000);
                    benchmark.MemoryThrottledSetup();
                    return benchmark;
                },
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(),
                benchmark => benchmark.Teardown());
            },
            ["Transactions.Azure"] = () =>
            {
                RunBenchmark(
                "Running Transactions benchmark",
                () =>
                {
                    var benchmark = new TransactionBenchmark(2, 20000, 5000);
                    benchmark.AzureSetup();
                    return benchmark;
                },
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(),
                benchmark => benchmark.Teardown());
            },
            ["Transactions.Azure.Throttled"] = () =>
            {
                RunBenchmark(
                "Running Transactions benchmark",
                () =>
                {
                    var benchmark = new TransactionBenchmark(2, 200000, 15000);
                    benchmark.AzureThrottledSetup();
                    return benchmark;
                },
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(),
                benchmark => benchmark.Teardown());
            },
            ["Transactions.Azure.Overloaded"] = () =>
            {
                RunBenchmark(
                "Running Transactions benchmark",
                () =>
                {
                    var benchmark = new TransactionBenchmark(2, 200000, 15000);
                    benchmark.AzureSetup();
                    return benchmark;
                },
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(),
                benchmark => benchmark.Teardown());
            },
            ["Indexing.Memory.FT.LZ.PK"] = () =>
            {
                RunBenchmark($"Running FT Workflow PerKeyHash partitioning Indexing benchmark using Memory storage ({indexingParamsPK})",
                () => new IndexingBenchmark<IIndexingGrainWorkflowFT_PK>(indexingParamsPK, bm => bm.MemorySetup()),
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(), benchmark => benchmark.Teardown());
            },
            ["Indexing.Memory.FT.LZ.SB"] = () =>
            {
                RunBenchmark($"Running FT Workflow SingleBucket partitioning Indexing benchmark using Memory storage ({indexingParamsSB})",
                () => new IndexingBenchmark<IIndexingGrainWorkflowFT_SB>(indexingParamsSB, bm => bm.MemorySetup()),
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(), benchmark => benchmark.Teardown());
            },
            ["Indexing.Azure.FT.LZ.PK"] = () =>
            {
                RunBenchmark($"Running FT Workflow PerKeyHash partitioning Indexing benchmark using Azure storage ({indexingParamsPK})",
                () => new IndexingBenchmark<IIndexingGrainWorkflowFT_PK>(indexingParamsPK, bm => bm.AzureSetup()),
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(), benchmark => benchmark.Teardown());
            },
            ["Indexing.Azure.FT.LZ.SB"] = () =>
            {
                RunBenchmark($"Running FT Workflow SingleBucket partitioning Indexing benchmark using Azure storage ({indexingParamsSB})",
                () => new IndexingBenchmark<IIndexingGrainWorkflowFT_SB>(indexingParamsSB, bm => bm.AzureSetup()),
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(), benchmark => benchmark.Teardown());
            },
            ["Indexing.Memory.NFT.EG.PK"] = () =>
            {
                RunBenchmark($"Running NFT Eager Workflow PerKeyHash partitioning Indexing benchmark using Memory storage ({indexingParamsPK})",
                () => new IndexingBenchmark<IIndexingGrainWorkflowNFT_EG_PK>(indexingParamsPK, bm => bm.MemorySetup()),
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(), benchmark => benchmark.Teardown());
            },
            ["Indexing.Memory.NFT.EG.SB"] = () =>
            {
                RunBenchmark($"Running NFT Eager Workflow SingleBucket partitioning Indexing benchmark using Memory storage ({indexingParamsSB})",
                () => new IndexingBenchmark<IIndexingGrainWorkflowNFT_EG_SB>(indexingParamsSB, bm => bm.MemorySetup()),
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(), benchmark => benchmark.Teardown());
            },
            ["Indexing.Azure.NFT.EG.PK"] = () =>
            {
                RunBenchmark($"Running NFT Eager Workflow PerKeyHash partitioning Indexing benchmark using Azure storage ({indexingParamsPK})",
                () => new IndexingBenchmark<IIndexingGrainWorkflowNFT_EG_PK>(indexingParamsPK, bm => bm.AzureSetup()),
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(), benchmark => benchmark.Teardown());
            },
            ["Indexing.Azure.NFT.EG.SB"] = () =>
            {
                RunBenchmark($"Running NFT Eager Workflow SingleBucket partitioning Indexing benchmark using Azure storage ({indexingParamsSB})",
                () => new IndexingBenchmark<IIndexingGrainWorkflowNFT_EG_SB>(indexingParamsSB, bm => bm.AzureSetup()),
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(), benchmark => benchmark.Teardown());
            },
            ["Indexing.Memory.NFT.LZ.PK"] = () =>
            {
                RunBenchmark($"Running NFT Lazy Workflow PerKeyHash partitioning Indexing benchmark using Memory storage ({indexingParamsPK})",
                () => new IndexingBenchmark<IIndexingGrainWorkflowNFT_LZ_PK>(indexingParamsPK, bm => bm.MemorySetup()),
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(), benchmark => benchmark.Teardown());
            },
            ["Indexing.Memory.NFT.LZ.SB"] = () =>
            {
                RunBenchmark($"Running NFT Lazy Workflow SingleBucket partitioning Indexing benchmark using Memory storage ({indexingParamsSB})",
                () => new IndexingBenchmark<IIndexingGrainWorkflowNFT_LZ_SB>(indexingParamsSB, bm => bm.MemorySetup()),
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(), benchmark => benchmark.Teardown());
            },
            ["Indexing.Azure.NFT.LZ.PK"] = () =>
            {
                RunBenchmark($"Running NFT Lazy Workflow PerKeyHash partitioning Indexing benchmark using Azure storage ({indexingParamsPK})",
                () => new IndexingBenchmark<IIndexingGrainWorkflowNFT_LZ_PK>(indexingParamsPK, bm => bm.AzureSetup()),
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(), benchmark => benchmark.Teardown());
            },
            ["Indexing.Azure.NFT.LZ.SB"] = () =>
            {
                RunBenchmark($"Running NFT Lazy Workflow SingleBucket partitioning Indexing benchmark using Azure storage ({indexingParamsSB})",
                () => new IndexingBenchmark<IIndexingGrainWorkflowNFT_LZ_SB>(indexingParamsSB, bm => bm.AzureSetup()),
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(), benchmark => benchmark.Teardown());
            },
            ["Indexing.Memory.TXN.EG.PK"] = () =>
            {
                RunBenchmark($"Running Transactional PerKeyHash partitioning Indexing benchmark using Memory storage ({indexingParamsPK})",
                () => new IndexingBenchmark<IIndexingGrainTransactional_PK>(indexingParamsPK, bm => bm.MemorySetup()),
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(), benchmark => benchmark.Teardown());
            },
            ["Indexing.Memory.TXN.EG.SB"] = () =>
            {
                RunBenchmark($"Running Transactional SingleBucket partitioning Indexing benchmark using Memory storage ({indexingParamsSB})",
                () => new IndexingBenchmark<IIndexingGrainTransactional_SB>(indexingParamsSB, bm => bm.MemorySetup()),
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(), benchmark => benchmark.Teardown());
            },
            ["Indexing.Azure.TXN.EG.PK"] = () =>
            {
                RunBenchmark($"Running Transactional PerKeyHash partitioning Indexing benchmark using Azure storage ({indexingParamsPK})",
                () => new IndexingBenchmark<IIndexingGrainTransactional_PK>(indexingParamsPK, bm => bm.AzureSetup()),
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(), benchmark => benchmark.Teardown());
            },
            ["Indexing.Azure.TXN.EG.SB"] = () =>
            {
                RunBenchmark($"Running Transactional SingleBucket partitioning Indexing benchmark using Azure storage ({indexingParamsSB})",
                () => new IndexingBenchmark<IIndexingGrainTransactional_SB>(indexingParamsSB, bm => bm.AzureSetup()),
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(), benchmark => benchmark.Teardown());
            },
            ["Ping"] = () =>
            {
                RunBenchmark(
                    "Running Ping benchmark",
                    () =>
                    {
                        var benchmark = new PingBenchmark();
                        benchmark.Setup();
                        return benchmark;
                    },
                    benchmark => benchmark.RunAsync().GetAwaiter().GetResult(),
                    benchmark => benchmark.Teardown());
            },
            ["SequentialPing"] = () =>
            {
                BenchmarkRunner.Run<SequentialPingBenchmark>();
            },
            ["PingForever"] = () =>
            {
                new SequentialPingBenchmark().PingForever().GetAwaiter().GetResult();
            },
            ["PingPongForever"] = () =>
            {
                new SequentialPingBenchmark().PingPongForever().GetAwaiter().GetResult();
            },
            ["PingPongForeverSaturate"] = () =>
            {
                new SequentialPingBenchmark().PingPongForever().GetAwaiter().GetResult();
            },
            ["GrainStorage.Memory"] = () =>
            {
                RunBenchmark(
                "Running grain storage benchmark against memory",
                () =>
                {
                    var benchmark = new GrainStorageBenchmark();
                    benchmark.MemorySetup();
                    return benchmark;
                },
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(),
                benchmark => benchmark.Teardown());
            },
            ["GrainStorage.AzureTable"] = () =>
            {
                RunBenchmark(
                "Running grain storage benchmark against Azure Table",
                () =>
                {
                    var benchmark = new GrainStorageBenchmark();
                    benchmark.AzureTableSetup();
                    return benchmark;
                },
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(),
                benchmark => benchmark.Teardown());
            },
            ["GrainStorage.AzureBlob"] = () =>
            {
                RunBenchmark(
                "Running grain storage benchmark against Azure Blob",
                () =>
                {
                    var benchmark = new GrainStorageBenchmark();
                    benchmark.AzureBlobSetup();
                    return benchmark;
                },
                benchmark => benchmark.RunAsync().GetAwaiter().GetResult(),
                benchmark => benchmark.Teardown());
            },
        };

        // requires benchmark name or 'All' word as first parameter. If it ends with ".*"
        // then it is a "BeginsWith" match.
        static void Main(string[] args)
        {
            if (args.Length > 0 && args[0].Equals("all", StringComparison.InvariantCultureIgnoreCase))
            {
                Console.WriteLine("Running full benchmarks suite");
                _benchmarks.Select(pair => pair.Value).ToList().ForEach(action => action());
                return;
            }

            string[] getMatchingBenchmarks()
            {
                if (args.Length > 0 && args[0].Contains('*'))
                {
                    var argParts = args[0].Split('.');
                    bool isPartMatch(string argPart, string namePart) => argPart == "*" || argPart == namePart;
                    bool isPartsMatch(string[] nameParts)
                        => (argParts.Length == nameParts.Length || (argParts.Length < nameParts.Length && argParts.Last() == "*"))
                            && Enumerable.Range(0, argParts.Length).All(ii => isPartMatch(argParts[ii], nameParts[ii]));
                    return _benchmarks.Keys.Where(key => isPartsMatch(key.Split('.'))).ToArray();
                }
                return args.Length > 0 ? new[] { args[0] } : new string[0];
            }
            var matchingBenchmarks = getMatchingBenchmarks();

            if (matchingBenchmarks.Length == 0)
            {
                Console.WriteLine("Please select one or more benchmarks from the following list. '*' is supported as a wildcard between periods.");
                _benchmarks
                    .Select(pair => pair.Key)
                    .ToList()
                    .ForEach(Console.WriteLine);
                Console.WriteLine("All");
                return;
            }

            void runBenchmark(string benchmark)
            {
                Console.WriteLine(benchmark);
                _benchmarks[benchmark]();
            }

            if (matchingBenchmarks.Length > 1)
            {
                Console.WriteLine("Matching benchmarks:");
                Array.ForEach(matchingBenchmarks, Console.WriteLine);
                Console.WriteLine();

                suppressPause = true;
                foreach (var benchmark in matchingBenchmarks.Take(matchingBenchmarks.Length - 1))
                {
                    runBenchmark(benchmark);
                    Console.WriteLine();
                    Console.WriteLine(" ------------------------- ");
                    Console.WriteLine();
                }
            }

            suppressPause = false;
            runBenchmark(matchingBenchmarks.Last());
        }

        private static void RunBenchmark<T>(string name, Func<T> init, Action<T> benchmarkAction, Action<T> tearDown)
        {
            Console.WriteLine(name);
            var bench = init();
            var stopWatch = Stopwatch.StartNew();
            benchmarkAction(bench);
            Console.WriteLine($"Elapsed milliseconds: {stopWatch.ElapsedMilliseconds}");
            if (!suppressPause)
            {
                Console.WriteLine();
                Console.WriteLine("Press Enter to continue ...");
            }
            tearDown(bench);
            if (!suppressPause)
            {
                Console.ReadLine();
            }
        }
    }
}
