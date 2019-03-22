using System;

namespace SportsTeamIndexing.Interfaces
{
    [Serializable]
    public class SportsTeamState : SportsTeamIndexedProperties
    {
        // This property is not indexed.
        public string Venue { get; set; }
    }
}
