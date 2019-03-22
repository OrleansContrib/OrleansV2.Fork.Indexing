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
#if USE_TRANSACTIONS
        [Transaction(TransactionOption.CreateOrJoin, ReadOnly = true)]
#endif
        Task<string> GetQualifiedName();
        #endregion indexed as a computed property

        #region indexed as single properties
#if USE_TRANSACTIONS
        [Transaction(TransactionOption.CreateOrJoin, ReadOnly = true)]
#endif
        Task<string> GetName();
#if USE_TRANSACTIONS
        [Transaction(TransactionOption.CreateOrJoin)]
#endif
        Task SetName(string name);

#if USE_TRANSACTIONS
        [Transaction(TransactionOption.CreateOrJoin, ReadOnly = true)]
#endif
        Task<string> GetLocation();
#if USE_TRANSACTIONS
        [Transaction(TransactionOption.CreateOrJoin)]
#endif
        Task SetLocation(string location);

#if USE_TRANSACTIONS
        [Transaction(TransactionOption.CreateOrJoin, ReadOnly = true)]
#endif
        Task<string> GetLeague();
#if USE_TRANSACTIONS
        [Transaction(TransactionOption.CreateOrJoin)]
#endif
        Task SetLeague(string league);
        #endregion indexed as single properties

        #region not indexed
#if USE_TRANSACTIONS
        [Transaction(TransactionOption.CreateOrJoin, ReadOnly = true)]
#endif
        Task<string> GetVenue();
#if USE_TRANSACTIONS
        [Transaction(TransactionOption.CreateOrJoin)]
#endif
        Task SetVenue(string venue);
        #endregion not indexed

#if USE_TRANSACTIONS
        [Transaction(TransactionOption.CreateOrJoin, ReadOnly = true)]
#endif
        Task<SportsTeamState> ReadStateAsync();

#if USE_TRANSACTIONS
        [Transaction(TransactionOption.CreateOrJoin)]
#endif
        Task WriteStateAsync(SportsTeamState state);
    }
}
