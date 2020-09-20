using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Serilog;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace MiniMQ
{
    public enum ConsumerActions
    {
        Send = 0,
        Heartbeat = 1
    }

    public class MQConsumerServer
    {
        private readonly ConsumerManager _consumers = new ConsumerManager();

        private const int LISTEN_ACCEPT_BACKLOG = 100;

        private Socket _listenSocket;

        private SemaphoreSlim _acceptSemaphore = new SemaphoreSlim(1);

        public MQConsumerServer()
        {

        }

        public void Init()
        {
            Log.Information("Initializing Consumer server");
        }

        public Task Start(IPEndPoint localEndPoint, CancellationToken cancellationToken)
        {
            Log.Information("Initializing Consumer listener socket");

            // Create the socket to listen for incoming connections.
            _listenSocket = new Socket(localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _listenSocket.Bind(localEndPoint);

            // Start the server with a listen backlog.
            _listenSocket.Listen(LISTEN_ACCEPT_BACKLOG);

            var queueLoop = Task.Run(() => QueueLoop(cancellationToken));
            var acceptLoop = AcceptLoop(cancellationToken);

            return Task.WhenAll(queueLoop, acceptLoop);
        }

        public void QueueLoop(CancellationToken cancellationToken)
        {
            Message message = null;

            while (!cancellationToken.IsCancellationRequested)
            {
                // Do not try to dequeue another message if the last processing did not find a consumer.
                if (message == null)
                {
                    message = MessageQueue.Take(cancellationToken);
                }

                // Block until a consumer is ready.
                var consumer = _consumers.NextAvailableConsumer();
                try
                {
                    consumer.SendMessage(message);
                    message = null;
                }
                catch(Exception ex)
                {
                    // Something when wrong when trying to send (maybe client got closed in the few milliseconds).
                    Log.Error("Exception occurred when trying to send a message: {Error}", ex);
                }
            }
        }

        public async Task AcceptLoop(CancellationToken cancellationToken)
        {
            var acceptEventArg = new SocketAsyncEventArgs();
            acceptEventArg.Completed += new EventHandler<SocketAsyncEventArgs>(ProcessAccept);

            // TODO: Need to wrap this in a try/catch for CancellationToken.
            while (!cancellationToken.IsCancellationRequested)
            {
                await _acceptSemaphore.WaitAsync(cancellationToken);

                acceptEventArg.AcceptSocket = null;

                if (!_listenSocket.AcceptAsync(acceptEventArg))
                {
                    ProcessAccept(this, acceptEventArg);
                }
            }

            Log.Information("Consumer server shutting down connections.");
            _consumers.Shutdown();
        }

        public void ProcessAccept(object sender, SocketAsyncEventArgs e)
        {
            if (e.SocketError != SocketError.Success)
            {
                Log.Error("An error occurred when attempting to Accept a new connection");
                _acceptSemaphore.Release();
                return;
            }

            try
            {
                var buffer = new byte[32]; // TODO: Buffer manager to reduce memory allocations.
                var consumer = new Consumer(e.AcceptSocket, buffer);
                Log.Information("Consumer client connected. Client Id: {0}", consumer.Id);

                _consumers.Add(consumer);
            }
            catch (Exception ex)
            {
                Log.Information("An exception occurred when trying to establish a new Consumer client: {0}", ex.Message);
            }
            finally
            {
                _acceptSemaphore.Release();
            }
        }
    }
}
