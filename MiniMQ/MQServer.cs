using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Serilog;

namespace MiniMQ
{
    public class MQServer
    {
        private const int LISTEN_ACCEPT_BACKLOG = 100;

        private Socket _listenSocket;

        // TODO: Maybe need another structure for concurrency.
        private List<MQAsyncUserToken> _clients = new List<MQAsyncUserToken>();

        private Semaphore _acceptSemaphore = new Semaphore(1, 1);

        public MQServer()
        {

        }

        public void Init()
        {
            Log.Information("Initializing server");
        }

        public void Start(IPEndPoint localEndPoint)
        {
            Log.Information("Initializing listener socket");

            // create the socket which listens for incoming connections
            _listenSocket = new Socket(localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _listenSocket.Bind(localEndPoint);
            // start the server with a listen backlog of 100 connections
            _listenSocket.Listen(LISTEN_ACCEPT_BACKLOG);

            // post accepts on the listening socket
            AcceptLoop();
        }

        private void AcceptLoop()
        {
            Log.Information("Accept loop start");

            var acceptEventArg = new SocketAsyncEventArgs();
            acceptEventArg.Completed += new EventHandler<SocketAsyncEventArgs>(ProcessAccept);

            for (;;)
            {
                // Only process one accept at a time. Use a semaphore to handle instant process and async callback.
                _acceptSemaphore.WaitOne();

                // Reset accept socket before each listen.
                acceptEventArg.AcceptSocket = null;

                bool willRaiseEvent = _listenSocket.AcceptAsync(acceptEventArg);
                if (!willRaiseEvent)
                {
                    ProcessAccept(this, acceptEventArg);
                }

                // TODO: Server shutdown handler?
            }
        }

        /// <summary>
        /// Process an accept event. The e.AcceptSocket will contain the newly accepted client connection.
        /// </summary>
        private void ProcessAccept(object sender, SocketAsyncEventArgs e)
        {
            if (e.SocketError != SocketError.Success)
            {
                // TODO: something?
                Log.Error("An error occurred when attempting to Accept a new connection");
                _acceptSemaphore.Release();
                return;
            }

            try
            {
                // Interlocked.Increment(ref m_numConnectedSockets);

                // Console.WriteLine("Client connection accepted. There are {0} clients connected to the server", m_numConnectedSockets);

                var token = new MQAsyncUserToken(e.AcceptSocket);
                token.Buffer = new byte[4096];
                Log.Information("Client connected. Client Id: {0}", token.Id);

                // TODO: This is why a pool is used. This is expensive to create when accepting.
                SocketAsyncEventArgs readEventArgs = new SocketAsyncEventArgs();
                readEventArgs.UserToken = token;
                readEventArgs.Completed += new EventHandler<SocketAsyncEventArgs>(IOCompleted);
                readEventArgs.SetBuffer(token.Buffer, 0, token.Buffer.Length);

                // Add to list of connected clients.
                lock (_clients)
                {
                    _clients.Add(token);
                }

                // As soon as the client is connected, post a receive to the connection
                bool willRaiseEvent = e.AcceptSocket.ReceiveAsync(readEventArgs);
                if (!willRaiseEvent)
                {
                    ProcessReceive(readEventArgs);
                }

                // Go "back" to StartAccept to complete the Accept loop.
                // TODO: This will eventually case a stack overflow.
                // StartAccept(e);
            }
            catch (Exception ex)
            {
                Log.Information("An exception occurred when trying to establish a new client: {0}", ex.Message);
                // TODO: other considerations?
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
            // check if the remote host closed the connection
            MQAsyncUserToken token = (MQAsyncUserToken)e.UserToken;
            if (e.BytesTransferred > 0 && e.SocketError == SocketError.Success)
            {
                Log.Debug("Received {Bytes} from client {ClientId}", e.BytesTransferred, token.Id);

                // TODO: Stat tracking.
                // Increment the count of the total bytes receive by the server
                // Interlocked.Add(ref m_totalBytesRead, e.BytesTransferred);
                // Console.WriteLine("The server has read a total of {0} bytes", m_totalBytesRead);

                // TODO: Need to know when to go back into a read state. Is there a way to do this without recursion?
                CollectMessageBytes(e);

                // TODO: Only do this part when done. May need to wait some more to get rest of bytes.
                // echo the data received back to the client
                // e.SetBuffer(e.Offset, e.BytesTransferred);

                // Setting this to a manual ACK.
                token.Buffer[0] = 0;
                token.Buffer[1] = 0;
                token.Buffer[3] = 0;
                e.SetBuffer(token.Buffer, 0, 3);
                bool willRaiseEvent = token.Socket.SendAsync(e);
                if (!willRaiseEvent)
                {
                    ProcessSend(e);
                }
            }
            else
            {
                // Zero bytes, but a receive indicates a graceful shutdown by client.
                CloseClientSocket(e);
            }
        }

        private void ProcessSend(SocketAsyncEventArgs e)
        {
            if (e.SocketError == SocketError.Success)
            {
                // done echoing data back to the client
                MQAsyncUserToken token = (MQAsyncUserToken)e.UserToken;

                Log.Debug("Send to {ClientId} successful", token.Id);
                // TODO: Process after send, if there is anything to do.

                // Go back into "Receive mode."
                bool willRaiseEvent = token.Socket.ReceiveAsync(e);
                if (!willRaiseEvent)
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
            MQAsyncUserToken token = (MQAsyncUserToken)e.UserToken;

            var clientId = token.Id;

            // close the socket associated with the client
            try
            {
                token.Socket.Shutdown(SocketShutdown.Send);
                token.Socket.Close();

                // Remove from list of clients.
                lock (_clients)
                {
                    _clients.Remove(token);
                }
            }
            // throws if client process has already closed
            catch (Exception ex)
            {
                // TODO: Log
            }

            // decrement the counter keeping track of the total number of clients connected to the server
            // Interlocked.Decrement(ref m_numConnectedSockets);

            // Free the SocketAsyncEventArg so they can be reused by another client
            // m_readWritePool.Push(e);

            // m_maxNumberAcceptedClients.Release();
            // Console.WriteLine("A client has been disconnected from the server. There are {0} clients connected to the server", m_numConnectedSockets);
            Log.Information("Client {ClientId} has been disconnected", clientId);
        }

        private void CollectMessageBytes(SocketAsyncEventArgs e)
        {
            const int HEADER_SIZE = 3;

            MQAsyncUserToken token = (MQAsyncUserToken)e.UserToken;

            // If this is the start of collection, figure out message type and number of bytes.
            if(token.CollectionState.BodySize == 0)
            {
                // Must have at least 3 bytes in start to know message type and body size.
                if(e.BytesTransferred >= HEADER_SIZE)
                {
                    var messageTypeRaw = (int)e.Buffer[0];
                    token.CollectionState.MessageType = ToMessageType(messageTypeRaw);

                    var networkSize = BitConverter.ToInt16(e.Buffer, 1);
                    token.CollectionState.BodySize = (ushort)IPAddress.NetworkToHostOrder(networkSize);

                    // TODO: Lazy allocation.
                    int thisChunkSize = e.BytesTransferred - HEADER_SIZE;
                    token.MessageBody = new byte[token.CollectionState.BodySize];
                    Array.Copy(e.Buffer, 3, token.MessageBody, 0, thisChunkSize);

                    token.CollectionState.CollectedBodyBytes = thisChunkSize;
                }
                else
                {
                    Log.Error("Invalid message header from client {ClientId}", token.Id);
                    // ? Bad message, don't know what to do. Bail on it? Send an error.
                }
            }
            else
            {
                // Continue collecting multiple chunks of data.
                // TODO: Needs testing.
                Array.Copy(e.Buffer, 0, token.MessageBody, token.CollectionState.CollectedBodyBytes, e.BytesTransferred);
                token.CollectionState.CollectedBodyBytes = e.BytesTransferred;
            }

            // TODO: Probably want to send this back up and let that method decide what to do.
            if(token.CollectionState.CollectedBodyBytes < token.CollectionState.BodySize)
            {
                // Need to go back into receive mode to collect another message chunk.
            }
            else
            {
                // Send off to controller.
            }
        }

        private MessageType ToMessageType(int value)
        {
            if(Enum.IsDefined(typeof(MessageType), value))
            {
                return (MessageType)value;
            }

            return MessageType.Unknown;
        }
    }
}
