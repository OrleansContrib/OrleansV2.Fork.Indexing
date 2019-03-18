using System.Threading.Tasks;

namespace BenchmarkGrainInterfaces.Indexing
{
    public interface IIndexingGrain
    {
        Task SetId1(int id);

        Task SetId2(int id);

        Task SetId3(int id);

        Task SetId4(int id);

        Task WriteStateAsync();
    }
}
