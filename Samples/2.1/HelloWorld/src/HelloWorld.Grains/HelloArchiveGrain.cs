using HelloWorld.Interfaces;
using Orleans;
using Orleans.Providers;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace HelloWorld.Grains
{
    [StorageProvider(ProviderName = "MemoryStorage")]
    public class HelloArchiveGrain : Grain<GreetingArchive>, IHelloArchive
    {
        public async Task<string> SayHello(string greeting)
        {
            State.Greetings.Add(greeting);
            await WriteStateAsync();

            var response = greeting.Contains("morning") ? "Hello" : "Thank you";
            return $"You said: '{greeting}', I say: {response}!";
        }

        public Task<IEnumerable<string>> GetGreetings() => Task.FromResult<IEnumerable<string>>(State.Greetings);
    }

    [Serializable]
    public class GreetingArchive
    {
        public List<string> Greetings { get; } = new List<string>();
    }
}
