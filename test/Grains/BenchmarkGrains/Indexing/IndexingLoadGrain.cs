using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Diagnostics;
using Orleans;
using BenchmarkGrainInterfaces.Indexing;

namespace BenchmarkGrains.Indexing
{
    public class IndexingLoadGrain<TGrainInterface> : Grain, IIndexingLoadGrain<TGrainInterface> where TGrainInterface : IIndexingGrain, IGrainWithIntegerKey
    {
        private Task<IndexingReport> runTask;

        public Task Generate(int run, int numGrainsPerRunner, int numPropertiesPerGrain, int concurrentGrainsPerRun)
        {
            this.runTask = RunGeneration(run, numGrainsPerRunner, numPropertiesPerGrain, concurrentGrainsPerRun);
            this.runTask.Ignore();
            return Task.CompletedTask;
        }

        public async Task<IndexingReport> TryGetReport() => this.runTask.IsCompleted ? await this.runTask : default;

        private async Task<IndexingReport> RunGeneration(int run, int numGrainsPerRunner, int numPropertiesPerGrain, int concurrentGrainsPerRun)
        {
            var pending = new List<Task>();
            var report = new IndexingReport();
            var sw = Stopwatch.StartNew();
            var generated = run * numGrainsPerRunner;   // For producing grain IDs as well as loop variables
            var max = generated + numGrainsPerRunner;
            while (generated < max)
            {
                while (generated < max && pending.Count < concurrentGrainsPerRun)
                {
                    pending.Add(this.StartIndexingTasks(generated++, numPropertiesPerGrain));
                }
                pending = await this.ResolvePending(pending, report);
            }
            await this.ResolvePending(pending, report, true);
            sw.Stop();
            report.Elapsed = sw.Elapsed;
            return report;
        }

        private async Task<List<Task>> ResolvePending(List<Task> pending, IndexingReport report, bool all = false)
        {
            try
            {
                await (all ? Task.WhenAll(pending) : Task.WhenAny(pending));
            }
            catch (Exception) { }

            List<Task> remaining = new List<Task>();
            foreach (Task t in pending)
            {
                if (t.IsFaulted || t.IsCanceled)
                {
                    report.Failed++;
                }
                else if (t.IsCompleted)
                {
                    report.Succeeded++;
                }
                else
                {
                    remaining.Add(t);
                }
            }
            return remaining;
        }

        private Task StartIndexingTasks(int grainId, int numPropertiesPerGrain)
            => GrainFactory.GetGrain<IIndexingRootGrain<TGrainInterface>>(Guid.Empty).Start(grainId, numPropertiesPerGrain);
    }
}
