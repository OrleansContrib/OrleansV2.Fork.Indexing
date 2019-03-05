using System;
using Orleans.Indexing;

namespace SportsTeamIndexing.Interfaces
{
    [Serializable]
    public class SportsTeamIndexedProperties : ISportsTeamIndexedProperties
    {
        /// <summary>
        /// Using the base IndexAttribute, with the index key type and interface type fully specified
        /// </summary>
        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, ISportsTeamGrain>), IsEager = true, IsUnique = false)]
        public string Name { get; set; }

        /// <summary>
        /// QualifiedName is an indexed computed property
        /// </summary>
        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, ISportsTeamGrain>), IsEager = true, IsUnique = true)]
        public string QualifiedName { get => JoinName(this.League, this.Name); set => SplitName(value); }

        /// <summary>
        /// Using the ActiveIndexAttribute, with the key type and interface type left unspecified (they will be
        /// resolved by Indexing during assembly load).
        /// </summary>
        [ActiveIndex(ActiveIndexType.HashIndexPartitionedByKeyHash, IsEager = true, IsUnique = false)]
        public string Location { get; set; }

        /// <summary>
        /// The League index specification uses the default of non-unique.
        /// </summary>
        [ActiveIndex(ActiveIndexType.HashIndexPartitionedByKeyHash, IsEager = true)]
        public string League { get; set; }

        #region utilities for computed property QualifiedName
        /// <summary>
        /// The name should be null if the properties are not set so Orleans.Indexing will not try to place it in the index until it has been set.
        /// </summary>
        public static string JoinName(string league, string teamName) => (league == null || teamName == null) ? null : $"{league}_{teamName}";
        public static (string league, string teamName) SplitName(string name)
        {
            var parts = name.Split('_');
            return (parts[0], parts[1]);
        }
        #endregion utilities for computed property QualifiedName
    }
}
