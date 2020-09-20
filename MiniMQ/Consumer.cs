using Serilog;
using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;

namespace MiniMQ
{
    public class Consumer
    {
        private const byte CONSUMER_ACK_BYTE = 0;

        private readonly SocketAsyncEventArgs _eventArgs;

        public Guid Id { get; }

        public DateTime LastActivity { get; private set; }

        public Socket Socket { get; }

        public byte[] Buffer { get; }

        public ConsumerState State { get; private set; }

        public Guid? CurrentMessageId { get; private set; }

        public event EventHandler<EventArgs> OnStateChanged;

        public event EventHandler<Guid> OnMessageAck;

        public Consumer(Socket socket, byte[] buffer)
        {
            Buffer = buffer;
            Socket = socket;
            Id = Guid.NewGuid();
            LastActivity = DateTime.Now;
            State = ConsumerState.Ready;

            _eventArgs = new SocketAsyncEventArgs();
            _eventArgs.UserToken = this;
            _eventArgs.Completed += new EventHandler<SocketAsyncEventArgs>(IOCompleted);
            _eventArgs.SetBuffer(buffer, 0, Buffer.Length);
        }

        // TODO: What about dead messages? Messages that are sent but not ACKed in a specific number of time?

        public void SendMessage(Message message)
        {
            // Confirm this is in a valid state to send a message.
            if(State != ConsumerState.Ready)
            {
                throw new InvalidOperationException("Consumer is not in a valid state to send a message.");
            }

            Log.Debug("Sending Message {MessageId} to {ClientId}", message.Id, Id);

            // TODO: Message size property. Also need to put a header in the message to indicate the message type and body size.
            CurrentMessageId = message.Id;
            _eventArgs.SetBuffer(message.Buffer, 0, message.Buffer.Length);
            if (!Socket.SendAsync(_eventArgs))
            {
                ProcessSend();
            }
        }

        public void Close(bool noEvent = false)
        {
            Log.Debug("Closing Consumer {Id}", Id);

            try
            {
                Socket.Shutdown(SocketShutdown.Both);
                Socket.Close();
            }
            catch(Exception ex)
            {
                Log.Warning("Exception thrown when attempting to close Consumer socket: {Exception}", ex);
            }

            Log.Debug("Consumer {Id} has been closed", Id);

            // Notify that this consumer is no longer active.
            if(!noEvent)
            {
                State = ConsumerState.Closed;
                OnStateChanged?.Invoke(this, new EventArgs());
            }
        }

        private void IOCompleted(object sender, SocketAsyncEventArgs e)
        {
            switch (e.LastOperation)
            {
                case SocketAsyncOperation.Receive:
                    ProcessReceive();
                    break;
                case SocketAsyncOperation.Send:
                    ProcessSend();
                    break;
                default:
                    // Log. This should never happen, but don't blow up.
                    break;
            }
        }

        private void ProcessReceive()
        {
            UpdateActivity();

            // Check if the remote host closed the connection
            if (_eventArgs.BytesTransferred > 0 && _eventArgs.SocketError == SocketError.Success)
            {
                Log.Debug("Received {Bytes} from client {ClientId}", _eventArgs.BytesTransferred, Id);

                if (_eventArgs.BytesTransferred > 0 && _eventArgs.Buffer[0] == CONSUMER_ACK_BYTE)
                {
                    // Assumes there is a message, but check to be safe.
                    if(CurrentMessageId != null)
                    {
                        OnMessageAck?.Invoke(this, CurrentMessageId.Value);
                    }

                    // Clear state from last message send.
                    CurrentMessageId = null;

                    // Notify that this Client is ready.
                    State = ConsumerState.Ready;
                    OnStateChanged?.Invoke(this, new EventArgs());
                }
            }
            else
            {
                // Zero bytes, but a receive indicates a graceful shutdown by client.
                Close();
            }
        }

        private void ProcessSend()
        {
            UpdateActivity();

            if (_eventArgs.SocketError == SocketError.Success)
            {
                // Done sending message to producer client. Go back to waiting for more data.
                Log.Debug("Send to {ClientId} successful", Id);

                // Reset buffer size before going back into send mode.
                _eventArgs.SetBuffer(Buffer, 0, Buffer.Length);

                // Go back into receive mode to listen for an ACK.
                if (!Socket.ReceiveAsync(_eventArgs))
                {
                    ProcessReceive();
                }
                else
                {
                    // If ran asynchronously, trigger a state change.
                    State = ConsumerState.WaitingResponse;
                    OnStateChanged?.Invoke(this, new EventArgs());
                }
            }
            else
            {
                Close();
            }
        }

        private void UpdateActivity()
        {
            LastActivity = DateTime.Now;
        }
    }
}
