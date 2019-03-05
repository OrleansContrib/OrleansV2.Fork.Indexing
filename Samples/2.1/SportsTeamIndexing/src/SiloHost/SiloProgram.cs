using System;
using System.Net;
using System.Threading.Tasks;
using SportsTeamIndexing.Grains;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Indexing;

namespace OrleansSiloHost
{
    public class SiloProgram
    {
        public static int Main(string[] args)
        {
            return RunMainAsync().Result;
        }

        private static async Task<int> RunMainAsync()
        {
            try
            {
                var host = await StartSilo();
                Console.WriteLine("Press Enter to terminate...");
                Console.ReadLine();

                await host.StopAsync();
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return 1;
            }
        }

        private static async Task<ISiloHost> StartSilo()
        {
            // define the cluster configuration
            var builder = new SiloHostBuilder()
                .AddMemoryGrainStorage(SportsTeamGrain.GrainStoreName)
                .AddMemoryGrainStorage("PubSubStore") // PubSubStore service is needed for the streams underlying OrleansQueryResults
                .UseLocalhostClustering(gatewayPort: 30002)
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = "dev";
                    options.ServiceId = "SportsTeamIndexingApp";
                })
                .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Loopback)
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(SportsTeamGrain).Assembly).WithReferences())
                .ConfigureLogging(logging => logging.AddConsole())
                .UseIndexing();

            var pid = System.Diagnostics.Process.GetCurrentProcess().Id;

            var host = builder.Build();
            await host.StartAsync();
            return host;
        }
    }
}
