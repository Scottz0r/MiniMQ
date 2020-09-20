using Serilog;
using Serilog.Core;
using Serilog.Events;
using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace MiniMQ
{
    class ThreadIdEnricher : ILogEventEnricher
    {
        public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
        {
            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty(
                    "ThreadId", Thread.CurrentThread.ManagedThreadId));
        }
    }

    class Program
    {
        private static CancellationTokenSource tokenSource = new CancellationTokenSource();

        static void Main(string[] args)
        {
            // Register Ctrl + C to stop servers via Cancellation Token.
            Console.CancelKeyPress += (object s, ConsoleCancelEventArgs e) =>
            {
                e.Cancel = true;
                tokenSource.Cancel();
            };

            Log.Logger = new LoggerConfiguration()
                .Enrich.With(new ThreadIdEnricher())
                .WriteTo.Console(
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] ({ThreadId}) {Message:lj}{NewLine}{Exception}"
                )
                .MinimumLevel.Debug()
                .CreateLogger();

            Log.Information("Mini MQ - a lightweight Message Queue.");

            // Listener
            IPHostEntry ipHostInfo = Dns.GetHostEntry(Dns.GetHostName());
            IPAddress ipAddress = ipHostInfo.AddressList[0];

            var producerEndPoint = new IPEndPoint(ipAddress, 11000);
            var producerServer = new MQProducerServer();
            producerServer.Init();
            var producerTask = producerServer.Start(producerEndPoint, tokenSource.Token);

            var consumerEndPoint = new IPEndPoint(ipAddress, 11001);
            var consumerServer = new MQConsumerServer();
            consumerServer.Init();
            var consumerTask = consumerServer.Start(consumerEndPoint, tokenSource.Token);

            Task.WaitAll(producerTask, consumerTask);

            Environment.Exit(0);
        }
    }
}
