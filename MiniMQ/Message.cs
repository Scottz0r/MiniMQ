using System;
using System.Collections.Generic;
using System.Text;

namespace MiniMQ
{
    public class Message
    {
        public Guid Id { get; set; }

        public byte[] Buffer { get; set; }

        public Message(byte[] buffer)
        {
            Id = Guid.NewGuid();
            Buffer = buffer;
        }
    }
}
