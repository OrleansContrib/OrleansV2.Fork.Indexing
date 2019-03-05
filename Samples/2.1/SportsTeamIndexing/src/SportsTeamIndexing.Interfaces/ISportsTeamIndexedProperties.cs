namespace SportsTeamIndexing.Interfaces
{
    public interface ISportsTeamIndexedProperties
    {
        string Name { get; set; }

        string QualifiedName { get; set; }   // Computed property

        string Location { get; set; }

        string League { get; set; }
    }
}
