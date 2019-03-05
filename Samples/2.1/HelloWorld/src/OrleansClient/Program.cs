using HelloWorld.Interfaces;
using Orleans;
using Orleans.Runtime;
using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;

namespace OrleansClient
{
    /// <summary>
    /// Orleans test silo client
    /// </summary>
    public class Program
    {
        const int initializeAttemptsBeforeFailing = 5;
        private static int attempt = 0;

        static int Main(string[] args)
        {
            return RunMainAsync().Result;
        }

        private static async Task<int> RunMainAsync()
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
            attempt = 0;
            IClusterClient client;
            client = new ClientBuilder()
                .UseLocalhostClustering(gatewayPort: 30001)
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = "dev";
                    options.ServiceId = "HelloWorldApp";
                })
                .ConfigureLogging(logging => logging.AddConsole())
                .Build();

            await client.Connect(RetryFilter);
            Console.WriteLine("Client successfully connect to silo host");
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
                return false;
            }
            await Task.Delay(TimeSpan.FromSeconds(4));
            return true;
        }

        private static async Task DoClientWork(IClusterClient client)
        {
            // example of calling grains from the initialized client
            var friend = client.GetGrain<IHello>(0);
            var response = await friend.SayHello("Good morning, my friend!");
            Console.WriteLine($"\n{response}\n\n");

            var archive = client.GetGrain<IHelloArchive>(1);
            response = await archive.SayHello("Good morning, my friend!");
            Console.WriteLine($"{response}");
            response = await archive.SayHello("Have a nice day!");
            Console.WriteLine($"{response}\n");

            var fetchedArchive = client.GetGrain<IHelloArchive>(1);
            var greetings = (await fetchedArchive.GetGreetings()).Select(greeting => $"\"{greeting}\"").ToArray();
            Console.WriteLine($"Your greetings were: {string.Join(", ", greetings)}");
            if (greetings.Length > 2)
            {
                Console.WriteLine("  (The duplicate greetings illustrate state preservation on the silo across multiple runs of the client.)");
            }
            Console.WriteLine("\n");
        }
    }
}
