using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Serilog;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Buffers;

namespace MiniMQ
{
    public enum ProducerActions
    {
        Put = 0,
        Heartbeat = 1
    }

    public class MQProducerServer
    {
        private const int LISTEN_ACCEPT_BACKLOG = 100;
        private const int RECEIVE_BUFFER_SIZE = 1024 * 16;

        private readonly ConcurrentDictionary<Guid, ProducerToken> _producers = new ConcurrentDictionary<Guid, ProducerToken>();

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
                    sk.Value.Dispose();
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
                var token = new ProducerToken(e.AcceptSocket, RECEIVE_BUFFER_SIZE);
                Log.Information("Producer client connected. Client Id: {0}", token.Id);

                _producers[token.Id] = token;

                // Set up new Async Event Args for the new client.
                SocketAsyncEventArgs readEventArgs = new SocketAsyncEventArgs();
                readEventArgs.UserToken = token;
                readEventArgs.Completed += new EventHandler<SocketAsyncEventArgs>(IOCompleted);
                readEventArgs.SetBuffer(token.Buffer);

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
            // Infinite loop because very high volumes of traffic will never need to be asynchronous. This will prevent a stack overflow by
            // going back into a "message receive" mode once the incoming message and ACK is sent.
            for (; ; ) // TODO: Token cancellation?
            {
                bool isPendingAsync;

                switch (e.LastOperation)
                {
                    case SocketAsyncOperation.Receive:
                        // Go back into receive depending if message collection and ACK work flow is waiting an asynchronous operation.
                        isPendingAsync = ProcessReceive(e);
                        break;
                    case SocketAsyncOperation.Send:
                        ProcessSend(e);
                        // Always go back into receive mode upon a send.
                        isPendingAsync = false;
                        break;
                    default:
                        // Log. This should never happen, but don't blow up.
                        Log.Error("Unexpected LastOperation from socket: {LastOperation}", e.LastOperation);
                        return;
                }

                if(!isPendingAsync)
                {
                    // Reset buffer size before going back into receive mode.
                    ProducerToken token = (ProducerToken)e.UserToken;
                    e.SetBuffer(token.Buffer);

                    if (token.Socket.ReceiveAsync(e))
                    {
                        // Going to wait for an asynchronous operation, so break out of this function. Otherwise, process synchronously.
                        return;
                    }
                }
                else
                {
                    // If not going back into receive mode, break out of this infinite loop.
                    return;
                }
            }
        }

        // Returns true if this is waiting an asynchronous operation.
        private bool ProcessReceive(SocketAsyncEventArgs e)
        {
            // Check if the remote host closed the connection
            ProducerToken token = (ProducerToken)e.UserToken;
            if (e.BytesTransferred > 0 && e.SocketError == SocketError.Success)
            {
                Log.Debug("Received {Bytes} from client {ClientId}", e.BytesTransferred, token.Id);

                return CollectMessageBytes(e);
            }
            else
            {
                // Zero bytes, but a receive indicates a graceful shutdown by client.
                CloseClientSocket(e);
                return false;
            }
        }

        // Returns true if this is waiting an asynchronous operation.
        private bool CollectMessageBytes(SocketAsyncEventArgs e)
        {
            ProducerToken token = (ProducerToken)e.UserToken;
            ReadOnlySpan<byte> buffSpan = token.Buffer.Span;

            // Mark the client as being active.
            token.UpdateActivity();

            // TODO: This should probably be split up into multiple methods maybe.

            for(; ; )
            {
                // If no bytes collected, then need to process the action to know what to do with request.
                //if(token.CollectedBytes == 0)
                if(token.MessageCollector == null)
                {
                    if(e.BytesTransferred > 0)
                    {
                        if(buffSpan[0] == (byte)ProducerActions.Heartbeat)
                        {
                            // Heartbeats as a single byte message that get responded to with an ACK.
                            if (e.BytesTransferred > 1)
                            {
                                return SendError(e, 2); // TODO - Error codes. Malformed message.
                            }

                            return SendAck(e);
                        }
                        else if(buffSpan[0] == (byte)ProducerActions.Put)
                        {
                            // Must be enough bytes in message to parse a length.
                            if(e.BytesTransferred < 3) // TODO: Probably need some constants.
                            {
                                return SendError(e, 2); // TODO - Error codes. Malformed message.
                            }

                            var networkSize = BitConverter.ToInt16(buffSpan.Slice(1, 2));
                            var localSize = (ushort)IPAddress.NetworkToHostOrder(networkSize);

                            int thisChunkSize = e.BytesTransferred - 3;
                            token.StartCollecting(localSize);
                            var thisChunk = buffSpan.Slice(3, thisChunkSize);
                            token.MessageCollector.Append(thisChunk);
                        }
                        else
                        {
                            return SendError(e, 1); // TODO - Error Codes - Unknown action type.
                        }
                    }
                }
                else
                {
                    // Need to complete collecting the entire message.
                    var thisChunk = buffSpan.Slice(0, e.BytesTransferred);
                    token.MessageCollector.Append(thisChunk);
                }

                if(!token.MessageCollector.CollectionCompleted)
                {
                    // Need to listen for more bytes in the message. If the operation is async, then break out of
                    // this method. Otherwise, do another loop. This will prevent stack overflowing on multiple chunks.
                    if (token.Socket.ReceiveAsync(e))
                    {
                        return true; // Waiting on an asynchronous operation.
                    }
                }
                else
                {
                    Log.Debug("Message completely received from {Client}.", token.Id);

                    // Add content to queue. Take memory from collector and move to message.
                    int messageSize = token.MessageCollector.MessageSize;
                    IMemoryOwner<byte> messageMemory = token.MessageCollector.TakeMemory();
                    var newMessage = new Message(messageMemory, messageSize);
                    MessageQueue.Add(newMessage);

                    // Remove all collection state from this producer's message collector.
                    token.ClearCollector();

                    // Tell producer client message was received.
                    return SendAck(e);
                }
            }
        }

        private bool SendAck(SocketAsyncEventArgs e)
        {
            ProducerToken token = (ProducerToken)e.UserToken;
            Log.Debug("Sending ACK to {Client}", token.Id);

            var buffSpan = token.Buffer.Span;
            buffSpan[0] = 0;

            var sendMemory = token.Buffer.Slice(0, 1);
            e.SetBuffer(sendMemory);

            if(!token.Socket.SendAsync(e))
            {
                ProcessSend(e);
                return false;
            }

            return true;
        }

        private bool SendError(SocketAsyncEventArgs e, byte errorCode)
        {
            ProducerToken token = (ProducerToken)e.UserToken;
            Log.Debug("Sending Error to {Client}", token.Id);

            var buffSpan = token.Buffer.Span;
            buffSpan[0] = 1;
            buffSpan[1] = errorCode;

            var sendMemory = token.Buffer.Slice(0, 2);
            e.SetBuffer(sendMemory);

            if (!token.Socket.SendAsync(e))
            {
                ProcessSend(e);
                return false;
            }

            return true;
        }

        private void ProcessSend(SocketAsyncEventArgs e)
        {
            if (e.SocketError == SocketError.Success)
            {
                ProducerToken token = (ProducerToken)e.UserToken;
                Log.Debug("Send to {ClientId} successful", token.Id);

                // Do nothing else. If this went to receive here, then a stack overflow would happen with high traffic.
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

            Log.Information("Client {ClientId} has been disconnected", clientId);
        }
    }
}
