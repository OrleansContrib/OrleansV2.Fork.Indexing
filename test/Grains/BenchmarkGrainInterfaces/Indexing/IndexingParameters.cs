using System;

namespace BenchmarkGrainInterfaces.Indexing
{
    [Serializable]
    public class IndexingParameters
    {
        public const int MaxPropertiesPerGrain = 4;

        /// <summary>
        /// Number of runs (not including the first cold run)
        /// </summary>
        public int NumRuns { get; }

        /// <summary>
        /// Number of grains to instantiate.
        /// </summary>
        public int NumGrains { get; }

        /// <summary>
        /// Number of properties per grain to index.
        //// Will always be sequential; i.e. if this is two it will only set Id1 and Id2, ignoring Id3 and Id4.
        /// </summary>
        public int NumProperties  { get; }

        /// <summary>
        /// Number of grains to run concurrently. Each grain will have NumProperties index operations;
        /// it is an internal detail whether these run concurrently or not, but the grains will.
        /// </summary>
        public int NumConcurrentGrains { get; }

        public IndexingParameters(int runs, int grains, int props, int concurrentGrains)
        {
            if (concurrentGrains > grains)
            {
                throw new ApplicationException("Not enough grains to support the requested number of concurrent grains");
            }
            if (props > MaxPropertiesPerGrain)
            {
                throw new ApplicationException("Too many properties per grain requested");
            }
            this.NumRuns = runs;
            this.NumGrains = grains;
            this.NumProperties = props;
            this.NumConcurrentGrains = concurrentGrains;
        }

        public override string ToString()
            => $"runs {this.NumRuns}, grains {this.NumGrains}, properties {this.NumProperties}, concurrent grains {this.NumConcurrentGrains}";
    }
}
