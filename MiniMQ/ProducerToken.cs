using System;
using System.Buffers;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;

namespace MiniMQ
{
    public sealed class ProducerToken : IDisposable
    {
        private readonly IMemoryOwner<byte> _ownedMemory;

        public MessageCollector MessageCollector { get; private set; }

        public Guid Id { get; }

        public Socket Socket { get; }

        public Memory<byte> Buffer
        {
            get
            {
                return _ownedMemory.Memory;
            }
        }

        public DateTime LastActivity { get; private set; }

        public ProducerToken(Socket socket, int bufferSize)
        {
            Id = Guid.NewGuid();
            Socket = socket;

            _ownedMemory = MemoryPool<byte>.Shared.Rent(bufferSize);

            UpdateActivity();
        }

        public void UpdateActivity()
        {
            LastActivity = DateTime.Now;
        }

        public void StartCollecting(int messageSize)
        {
            MessageCollector?.Dispose();
            MessageCollector = new MessageCollector(messageSize);
        }

        public void ClearCollector()
        {
            MessageCollector?.Dispose();
            MessageCollector = null;
        }

        public void Dispose()
        {
            _ownedMemory?.Dispose();

            MessageCollector?.Dispose();
            Socket?.Dispose();
        }
    }
}
