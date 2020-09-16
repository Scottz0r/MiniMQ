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
    public enum ProducerActions
    {
        Put = 0,
        Heartbeat = 1
    }

    public class MQProducerServer
    {
        private readonly ConcurrentDictionary<Guid, ProducerToken> _producers = new ConcurrentDictionary<Guid, ProducerToken>();

        private const int LISTEN_ACCEPT_BACKLOG = 100;

        private Socket _listenSocket;

        private SemaphoreSlim _acceptSemaphore = new SemaphoreSlim(1);

        public MQProducerServer()
        {

        }

        public void Init()
        {
            Log.Information("Initializing Producer server");
        }

        public Task Start(IPEndPoint localEndPoint, CancellationToken cancellationToken)
        {
            Log.Information("Initializing Producer listener socket");

            // Create the socket to listen for incoming connections.
            _listenSocket = new Socket(localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _listenSocket.Bind(localEndPoint);

            // Start the server with a listen backlog.
            _listenSocket.Listen(LISTEN_ACCEPT_BACKLOG);

            return AcceptLoop(cancellationToken);
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

                if(!_listenSocket.AcceptAsync(acceptEventArg))
                {
                    ProcessAccept(this, acceptEventArg);
                }
            }

            Log.Information("Producer shutting down connections.");

            Log.Information("Shutting down producer listener socket.");
            try
            {
                _listenSocket.Shutdown(SocketShutdown.Both);
                _listenSocket.Close();
            }
            catch(Exception ex)
            {
                Log.Warning("An exception occurred when shutting down the producer listener: {Error}", ex);
            }

            Log.Information("Shutting down producer sockets.");
            foreach (var sk in _producers)
            {
                try
                {
                    sk.Value.Socket.Shutdown(SocketShutdown.Both);
                    sk.Value.Socket.Close();
                }
                catch(Exception ex)
                {
                    Log.Warning("An exception occurred when shutting down socket {Id}: {Error}", sk.Key, ex);
                }
            }
            _producers.Clear();
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
                var token = new ProducerToken(e.AcceptSocket);
                token.Buffer = new byte[4096]; // TODO: Buffer manager to reduce memory allocations.
                Log.Information("Producer client connected. Client Id: {0}", token.Id);

                _producers[token.Id] = token;

                // Set up new Async Event Args for the new client.
                SocketAsyncEventArgs readEventArgs = new SocketAsyncEventArgs();
                readEventArgs.UserToken = token;
                readEventArgs.Completed += new EventHandler<SocketAsyncEventArgs>(IOCompleted);
                readEventArgs.SetBuffer(token.Buffer, 0, token.Buffer.Length);

                if(!token.Socket.ReceiveAsync(readEventArgs))
                {
                    IOCompleted(this, readEventArgs);
                }
            }
            catch(Exception ex)
            {
                Log.Information("An exception occurred when trying to establish a new Producer client: {0}", ex.Message);
            }
            finally
            {
                _acceptSemaphore.Release();
            }
        }

        void IOCompleted(object sender, SocketAsyncEventArgs e)
        {
            switch (e.LastOperation)
            {
                case SocketAsyncOperation.Receive:
                    ProcessReceive(e);
                    break;
                case SocketAsyncOperation.Send:
                    ProcessSend(e);
                    break;
                default:
                    // Log. This should never happen, but don't blow up.
                    break;
            }
        }

        private void ProcessReceive(SocketAsyncEventArgs e)
        {
            // Check if the remote host closed the connection
            ProducerToken token = (ProducerToken)e.UserToken;
            if (e.BytesTransferred > 0 && e.SocketError == SocketError.Success)
            {
                Log.Debug("Received {Bytes} from client {ClientId}", e.BytesTransferred, token.Id);

                CollectMessageBytes(e);
            }
            else
            {
                // Zero bytes, but a receive indicates a graceful shutdown by client.
                CloseClientSocket(e);
            }
        }

        private void CollectMessageBytes(SocketAsyncEventArgs e)
        {
            ProducerToken token = (ProducerToken)e.UserToken;

            // Mark the client as being active.
            token.UpdateActivity();

            // TODO: This should probably be split up into multiple methods maybe.

            bool done = false;
            while(!done)
            {
                // If no bytes collected, then need to process the action to know what to do with request.
                if(token.CollectedBytes == 0)
                {
                    if(e.BytesTransferred > 0)
                    {
                        if(e.Buffer[0] == (byte)ProducerActions.Heartbeat)
                        {
                            // Heartbeats as a single byte message that get responded to with an ACK.
                            if (e.BytesTransferred > 1)
                            {
                                SendError(e, 2); // TODO - Error codes. Malformed message.
                                return;
                            }

                            SendAck(e);
                            return;
                        }
                        else if(e.Buffer[0] == (byte)ProducerActions.Put)
                        {
                            // Must be enough bytes in message to parse a length.
                            if(e.BytesTransferred < 3) // TODO: Probably need some constants.
                            {
                                SendError(e, 2); // TODO - Error codes. Malformed message.
                                return;
                            }

                            var networkSize = BitConverter.ToInt16(e.Buffer, 1);
                            var localSize = (ushort)IPAddress.NetworkToHostOrder(networkSize);

                            token.MessageContents = new byte[localSize];
                            int thisChunkSize = e.BytesTransferred - 3;
                            Array.Copy(e.Buffer, 3, token.MessageContents, 0, thisChunkSize);
                            token.CollectedBytes = thisChunkSize;
                        }
                        else
                        {
                            SendError(e, 1); // TODO - Error Codes - Unknown action type.
                            return;
                        }
                    }
                }
                else
                {
                    // Need to range check to make bounds of contents not exceeded.
                    if(token.CollectedBytes + e.BytesTransferred > token.MessageContents.Length)
                    {
                        // Reset collection state.
                        token.CollectedBytes = 0;
                        token.MessageContents = null; // TODO: Return buffer to manager.

                        SendError(e, 2); // TODO - Error codes. Malformed message.
                        return;
                    }

                    // Need to complete collecting the entire message.
                    Array.Copy(e.Buffer, 0, token.MessageContents, token.CollectedBytes, e.BytesTransferred);
                    token.CollectedBytes += e.BytesTransferred;
                }

                // TODO: Would want another variable to know actual length because buffer may be larger if they are
                // getting reused.
                if(token.CollectedBytes < token.MessageContents.Length)
                {
                    // Need to listen for more bytes in the message. If the operation is async, then break out of
                    // this method. Otherwise, do another loop. This will prevent stack overflowing on multiple chunks.
                    if (token.Socket.ReceiveAsync(e))
                    {
                        return;
                    }
                }
                else
                {
                    Log.Debug("Message completely received from {Client}.", token.Id);

                    // Add content to queue.
                    var newMessage = new Message(token.MessageContents);
                    MessageQueue.Add(newMessage);

                    // Reset collection state.
                    token.CollectedBytes = 0;
                    token.MessageContents = null; // TODO: Queue would take ownership of MessageContents from the token.

                    // Tell producer client message was received.
                    SendAck(e);
                    return;
                }
            }
        }

        private void SendAck(SocketAsyncEventArgs e)
        {
            ProducerToken token = (ProducerToken)e.UserToken;
            Log.Debug("Sending ACK to {Client}", token.Id);

            token.Buffer[0] = 0;
            e.SetBuffer(token.Buffer, 0, 1);
            if(!token.Socket.SendAsync(e))
            {
                ProcessSend(e);
            }
        }

        private void SendError(SocketAsyncEventArgs e, byte errorCode)
        {
            ProducerToken token = (ProducerToken)e.UserToken;
            Log.Debug("Sending Error to {Client}", token.Id);

            token.Buffer[0] = 1;
            token.Buffer[1] = errorCode;

            e.SetBuffer(token.Buffer, 0, 2);
            if (!token.Socket.SendAsync(e))
            {
                ProcessSend(e);
            }
        }

        private void ProcessSend(SocketAsyncEventArgs e)
        {
            if (e.SocketError == SocketError.Success)
            {
                // Done sending message to producer client. Go back to waiting for more data.
                ProducerToken token = (ProducerToken)e.UserToken;
                Log.Debug("Send to {ClientId} successful", token.Id);

                // Reset buffer size before going back into send mode.
                e.SetBuffer(token.Buffer, 0, token.Buffer.Length);

                // Go back into receive mode.
                if (!token.Socket.ReceiveAsync(e))
                {
                    ProcessReceive(e);
                }
            }
            else
            {
                CloseClientSocket(e);
            }
        }

        private void CloseClientSocket(SocketAsyncEventArgs e)
        {
            ProducerToken token = (ProducerToken)e.UserToken;

            var clientId = token.Id;

            // Close the socket associated with the client
            try
            {
                token.Socket.Shutdown(SocketShutdown.Send);
                token.Socket.Close();

                // Remove the token from the collection of clients.
                ProducerToken removeToken;
                _producers.TryRemove(token.Id, out removeToken);
            }
            catch (Exception ex)
            {
                Log.Error("An error occurred when closing the producer client: {Error}", ex);
            }

            // decrement the counter keeping track of the total number of clients connected to the server
            // Interlocked.Decrement(ref m_numConnectedSockets);

            // Free the SocketAsyncEventArg so they can be reused by another client
            // m_readWritePool.Push(e);

            // m_maxNumberAcceptedClients.Release();
            // Console.WriteLine("A client has been disconnected from the server. There are {0} clients connected to the server", m_numConnectedSockets);
            Log.Information("Client {ClientId} has been disconnected", clientId);
        }
    }
}
