using System;
using SportsTeamIndexing.Interfaces;

namespace SportsTeamIndexing.Grains
{
    [Serializable]
    public class SportsTeamState : SportsTeamIndexedProperties
    {
        // This property is not indexed.
        public string Venue { get; set; }
    }
}
