using System;

namespace BenchmarkGrainInterfaces.Indexing
{
    public class IndexingReport
    {
        public int Succeeded { get; set; }
        public int Failed { get; set; }
        public TimeSpan Elapsed { get; set; }
    }
}
