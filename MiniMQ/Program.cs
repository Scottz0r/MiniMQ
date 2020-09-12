using Serilog;
using Serilog.Core;
using Serilog.Events;
using System;
using System.Net;
using System.Threading;

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
        //static void Main(string[] args)
        //{
        //    Log.Logger = new LoggerConfiguration()
        //        .Enrich.With(new ThreadIdEnricher())
        //        .WriteTo.Console(
        //            outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] ({ThreadId}) {Message:lj}{NewLine}{Exception}"
        //        )
        //        .MinimumLevel.Debug()
        //        .CreateLogger();

        //    Log.Information("Mini MQ - a lightweight Message Queue.");

        //    IPHostEntry ipHostInfo = Dns.GetHostEntry(Dns.GetHostName());
        //    IPAddress ipAddress = ipHostInfo.AddressList[0];
        //    IPEndPoint localEndPoint = new IPEndPoint(ipAddress, 11000);

        //    var s = new MQServer();
        //    s.Init();
        //    s.Start(localEndPoint);

        //    Environment.Exit(0);
        //}

        static void Main(string[] args)
        {
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
            IPEndPoint localEndPoint = new IPEndPoint(ipAddress, 11000);

            var s = new MQProducerServer();
            s.Init();
            var producerTask = s.Start(localEndPoint, new CancellationToken());

            producerTask.Wait();

            Environment.Exit(0);
        }
    }
}
