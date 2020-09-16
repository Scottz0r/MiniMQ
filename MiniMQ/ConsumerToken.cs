using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;

namespace MiniMQ
{
    public class ConsumerToken
    {
        public Guid Id { get; }

        public Socket Socket { get; set; }

        public DateTime LastActivity { get; set; }

        public byte[] Buffer { get; set; }

        public Guid? AckMessageId { get; set; }

        public ConsumerToken(Socket socket)
        {
            Id = Guid.NewGuid();
            Socket = socket;

            UpdateActivity();
        }

        public void UpdateActivity()
        {
            LastActivity = DateTime.Now;
        }
    }
}
