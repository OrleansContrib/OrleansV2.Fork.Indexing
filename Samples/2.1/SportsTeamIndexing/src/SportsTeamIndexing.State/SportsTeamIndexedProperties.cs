using System;
using SportsTeamIndexing.Interfaces;

namespace SportsTeamIndexing.State
{
    [Serializable]
    public class SportsTeamIndexedProperties : ISportsTeamIndexedProperties
    {
        public string Name { get; set; }

        public string QualifiedName { get => JoinName(this.League, this.Name); set => SplitName(value); }
        public string Location { get; set; }
        public string League { get; set; }

        public static string JoinName(string league, string teamName) => $"{league}_{teamName}";
        public static (string league, string teamName) SplitName(string name)
        {
            var parts = name.Split('_');
            return (parts[0], parts[1]);
        }
    }
}
