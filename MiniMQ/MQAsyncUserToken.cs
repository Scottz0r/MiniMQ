using System;
using System.Collections.Generic;
using System.Text;
using System.Net.Sockets;

namespace MiniMQ
{
    public enum MessageType
    {
        Unknown = -1,
        Ack = 0,
        PutMessage = 1,
    }

    public class CollectionState
    {
        public MessageType MessageType { get; set; } = MessageType.Unknown;

        public int BodySize { get; set; } = 0;

        public int CollectedBodyBytes { get; set; } = 0;
    }

    public class MQAsyncUserToken
    {
        public Guid Id { get; }

        public Socket Socket { get; }

        public DateTime LastActivity { get; set; }

        public CollectionState CollectionState { get; set; }

        // Buffer for network IO.
        public byte[] Buffer { get; set; }

        // TODO: Being lazy, for now, and allocating a new buffer of the exact size of the full message. This
        // object will own this memory.
        public byte [] MessageBody { get; set; }

        public MQAsyncUserToken(Socket socket)
        {
            Id = Guid.NewGuid();
            LastActivity = DateTime.Now;
            CollectionState = new CollectionState();

            Socket = socket;
        }
    }
}
