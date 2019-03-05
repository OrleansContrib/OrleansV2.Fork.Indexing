using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

using Orleans;
using Orleans.Runtime;
using Orleans.Configuration;
using Orleans.Indexing;

using SportsTeamIndexing.Interfaces;

namespace OrleansClient
{
    using SportsTeamQueryable = IOrleansQueryable<ISportsTeamGrain, SportsTeamIndexedProperties>;

    /// <summary>
    /// Orleans test silo client
    /// </summary>
    public class ClientProgram
    {
        const int initializeAttemptsBeforeFailing = 5;
        private static int attempt = 0;

        public static async Task<int> Main(string[] args)
        {
            if (System.Diagnostics.Debugger.IsAttached)
            {
                // In case we have both Silo and Client as startup projects, give the Silo time to finish starting.
                await Task.Delay(5000);
            }

            try
            {
                using (var client = await StartClientWithRetries())
                {
                    await DoClientWork(client);
                    Console.WriteLine("Press any key to exit");
                    Console.ReadKey();
                }
                return 0;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                Console.WriteLine("Press any key to exit");
                Console.ReadKey();
                return 1;
            }
        }

        private static async Task<IClusterClient> StartClientWithRetries()
        {
            var client = new ClientBuilder()
                .UseLocalhostClustering(gatewayPort:30002)
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = "dev";
                    options.ServiceId = "SportsTeamIndexingApp";
                })
                .ConfigureLogging(logging => logging.AddConsole())
                // parts.ConfigureDefaults() must be before .UseIndexing which adds a non-Framework assembly (Orleans.Indexing)
                // and thus parts.ConfigureDefaults() would do nothing.
                .ConfigureApplicationParts(parts => parts.ConfigureDefaults())
                .UseIndexing()
                .Build();

            var pid = System.Diagnostics.Process.GetCurrentProcess().Id;

            attempt = 0;
            await client.Connect(RetryFilter);
            Console.WriteLine("Client successfully connected to silo host");
            return client;
        }

        private static async Task<bool> RetryFilter(Exception exception)
        {
            // Sometimes this is just an Orleans.Runtime.OrleansException, e.g.: Not connected to a gateway
            if (exception.GetType() != typeof(SiloUnavailableException))
            {
                Console.WriteLine($"Cluster client failed to connect to cluster with unexpected error.  Exception: {exception}");
                return false;
            }
            attempt++;
            Console.WriteLine($"Cluster client attempt {attempt} of {initializeAttemptsBeforeFailing} failed to connect to cluster.  Exception: {exception}");
            if (attempt > initializeAttemptsBeforeFailing)
            {
                Console.WriteLine("Client connection failures exceeded retry count; abandoning.");
                return false;
            }
            await Task.Delay(TimeSpan.FromSeconds(4));
            return true;
        }

        private static async Task DoClientWork(IClusterClient client)
        {
            // example of calling grains from the initialized client
            int id = 1000;
            async Task createGrain(string name, string league, string location, string venue)
            {
                var team = client.GetGrain<ISportsTeamGrain>(id++);
                await team.SetName(name);
                await team.SetLeague(league);
                await team.SetLocation(location);
                await team.SetVenue(venue);

                var qualifiedName = await team.GetQualifiedName();
                System.Diagnostics.Debug.Assert(qualifiedName == SportsTeamIndexedProperties.JoinName(league, name));

                await team.SaveAsync();
            }

            // Define the grains. 4 teams from 4 leagues (as of this writing).
            await createGrain("Seahawks", "NFL", "Seattle, WA", "CenturyLink Field");
            await createGrain("Giants", "NFL", "New York, NY", "MetLife Stadium");
            await createGrain("Jets", "NFL", "New York, NY", "MetLife Stadium");
            await createGrain("Cardinals", "NFL", "Glendale, AZ", "State Farm Stadium");

            await createGrain("Mariners", "MLB", "Seattle, WA", "T-Mobile Park"); // TODO: fill in when resolved
            await createGrain("Giants", "MLB", "San Francisco, CA", "AT&T Park");
            await createGrain("Yankees", "MLB", "New York, NY", "Yankee Stadium");
            await createGrain("Cardinals", "MLB", "Saint Louis, MO", "Busch Stadium");

            await createGrain("Storm", "WNBA", "Seattle, WA", "KeyArena");
            await createGrain("Mercury", "WNBA", "Phoenix, AZ", "Talking Stick Resort Arena");
            await createGrain("Sparks", "WNBA", "Los Angeles, CA", "Staples Center");
            await createGrain("Liberty", "WNBA", "White Plains, NY", "Westchester County Center");

            await createGrain("Reign", "NWSL", "Seattle, WA", "Memorial Stadium");
            await createGrain("Thorns", "NWSL", "Portland, OR", "Providence Park");
            await createGrain("Royals", "NWSL", "Sandy, UT", "Rio Tinto Stadium");
            await createGrain("Spirit", "NWSL", "Germantown, MD", "Maryland SoccerPlex");

            // Define the queries.
            var indexFactory = (IIndexFactory)client.ServiceProvider.GetService(typeof(IIndexFactory));

            SportsTeamQueryable getActiveGrains() => indexFactory.GetActiveGrains<ISportsTeamGrain, SportsTeamIndexedProperties>();

            SportsTeamQueryable queryByLeague(string league) => from team in getActiveGrains() where team.League == league select team;
            SportsTeamQueryable queryByLocation(string location) => from team in getActiveGrains() where team.Location == location select team;
            // These illustrate reversing the comparison direction.
            SportsTeamQueryable queryByName(string name) => from team in getActiveGrains() where name == team.Name select team;
            SportsTeamQueryable queryByQualifiedName(string name) => from team in getActiveGrains() where name == team.QualifiedName select team;
            // Note that we cannot query on Venue as it is not indexed; attempting to do so will throw an exception.

            async Task<HashSet<ISportsTeamGrain>> getGrains(SportsTeamQueryable queryable) => new HashSet<ISportsTeamGrain>(await queryable.GetResults());

            const string nltab = "\n\t";
            async Task<string> stringifyTeams(IEnumerable<ISportsTeamGrain> grainSet)
                => nltab + string.Join(nltab, await Task.WhenAll(grainSet.Select(async g => $"{await g.GetName()} ({await g.GetLeague()}; {await g.GetLocation()}; {await g.GetVenue()})")));

            // Execute the queries.

            // Demo query for the documentation.
            var query = from team in indexFactory.GetActiveGrains<ISportsTeamGrain, SportsTeamIndexedProperties>()
                        where team.Name == "Seahawks"
                        select team;

            await query.ObserveResults(new QueryResultStreamObserver<ISportsTeamGrain>(async team =>
            {
                Console.WriteLine($"\n\n{await team.GetName()} location = {await team.GetLocation()}; pk = {team.GetPrimaryKeyLong()}");
            }));

            // Simple query.
            Console.WriteLine("\nHow many NFL teams are there?");
            var grainsInNFL = await getGrains(queryByLeague("NFL"));
            Console.WriteLine($"  {grainsInNFL.Count}{await stringifyTeams(grainsInNFL)}");

            // Intersecting indexes.
            Console.WriteLine("\nHow many of those are in New York?");
            var grainsByLocation = await getGrains(queryByLocation("New York, NY"));
            var intersectLeagueLocation = grainsInNFL.Intersect(grainsByLocation).ToArray();
            Console.WriteLine($"  {intersectLeagueLocation.Length}{await stringifyTeams(intersectLeagueLocation)}");

            // Simple query, using ObserveResults (the more real-world case).
            Console.WriteLine("\nHow many teams are named Giants?");
            var grainsByName = new List<ISportsTeamGrain>();
            await queryByName("Giants").ObserveResults(new QueryResultStreamObserver<ISportsTeamGrain>(team =>
            {
                grainsByName.Add(team);
                return Task.CompletedTask;
            }));
            Console.WriteLine($"  {grainsByName.Count}{await stringifyTeams(grainsByName)}");

            // Compound index simulated using computed property.
            Console.WriteLine("\nHow many teams named Giants are in New York?");
            var grainsByQualifiedName = await getGrains(queryByQualifiedName(SportsTeamIndexedProperties.JoinName("NFL", "Giants")));
            Console.WriteLine($"  {grainsByQualifiedName.Count}{await stringifyTeams(grainsByQualifiedName)}");

            // Unioning indexes.
            Console.WriteLine("\nHow many women's teams are there?");
            var grainsInWNBA = await getGrains(queryByLeague("WNBA"));
            var grainsInNWSL = await getGrains(queryByLeague("NWSL"));
            var womensGrains = grainsInWNBA.Union(grainsInNWSL).ToArray();
            Console.WriteLine($"  {womensGrains.Length}{await stringifyTeams(womensGrains)}");
        }
    }
}
