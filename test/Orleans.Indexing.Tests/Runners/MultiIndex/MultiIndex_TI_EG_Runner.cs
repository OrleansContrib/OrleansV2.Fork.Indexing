﻿using Orleans.Providers;
using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    // None; Total Index cannot be Eager.

    public abstract class MultiIndex_TI_EG_Runner : IndexingTestRunnerBase
    {
        protected MultiIndex_TI_EG_Runner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

        // None; Total Index cannot be Eager.

        internal static Func<IndexingTestRunnerBase, int, Task>[] GetAllTestTasks()
        {
            return new Func<IndexingTestRunnerBase, int, Task>[]
            {
            };
        }
    }
}
