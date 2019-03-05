using System.Threading.Tasks;
using Orleans;
using Orleans.Indexing;

namespace SportsTeamIndexing.Interfaces
{
    /// <summary>
    /// Orleans grain communication interface
    /// </summary>
    public interface ISportsTeamGrain : IGrainWithIntegerKey, IIndexableGrain<SportsTeamIndexedProperties>
    {
        #region indexed as a computed property
        Task<string> GetQualifiedName();
        Task SetQualifiedName(string name);
        #endregion indexed as a computed property

        #region indexed as single properties
        Task<string> GetName();
        Task SetName(string name);

        Task<string> GetLocation();
        Task SetLocation(string location);

        Task<string> GetLeague();
        Task SetLeague(string league);
        #endregion indexed as single properties

        #region not indexed
        Task<string> GetVenue();
        Task SetVenue(string venue);
        #endregion not indexed

        Task SaveAsync();
    }
}
