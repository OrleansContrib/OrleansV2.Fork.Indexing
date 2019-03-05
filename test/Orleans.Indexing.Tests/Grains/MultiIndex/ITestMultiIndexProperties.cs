// Naming convention used when creating sub-interfaces and classes:
//  Prefix is:
//      I - for interface
//      FT or NFT - Fault Tolerant or Non FT for class impl
//      IFT or INFT - Fault Tolerant or Non FT for class interface
//  UI, US, NI, NS  - property abbrev (specific to Multi-Index tests)
//  AI, TI, XI, DSMI - Active, Total, Mixed AI and TI, or DirectStorageManaged
//  EG, LZ - eager or lazy
//  PK, PS, SB - partition per key/silo or single bucket
//
// The full name is thus
//  (I|FT_|NFT_)Grain|Props|State_<properties>_<index_type>_<eg or lz>_<partition>[__<same>[...]]

namespace Orleans.Indexing.Tests
{
    public interface ITestMultiIndexProperties
    {
        int UniqueInt { get; set; }

        string UniqueString { get; set; }

        int NonUniqueInt { get; set; }

        string NonUniqueString { get; set; }
    }
}
