using System.Threading.Tasks;

namespace Orleans.Indexing.Tests
{
    public interface IPlayerGrain : IGrainWithIntegerKey
    {
        Task<string> GetEmail();
        Task<string> GetLocation();
        Task<int> GetScore();

        Task SetEmail(string email);
        Task SetLocation(string location);
        Task SetScore(int score);

        Task Deactivate();
    }
}
