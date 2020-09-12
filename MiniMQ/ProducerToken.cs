using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;

namespace MiniMQ
{
    public class ProducerToken
    {
        public Guid Id { get; set; }

        public Socket Socket { get; }

        public byte[] Buffer { get; set; }

        public int CollectedBytes { get; set; }

        public DateTime LastActivity { get; set; }

        // TODO: Better way to manage this buffer because it'll be passed around.
        public byte[] MessageContents { get; set; }

        public ProducerToken(Socket socket)
        {
            Id = Guid.NewGuid();
            Socket = socket;
            CollectedBytes = 0;

            UpdateActivity();
        }

        public void UpdateActivity()
        {
            LastActivity = DateTime.Now;
        }
    }
}
