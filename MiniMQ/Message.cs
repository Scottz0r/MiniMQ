using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace MiniMQ
{
    public sealed class Message : IDisposable
    {
        private IMemoryOwner<byte> _ownedMemory;

        public Guid Id { get;  }

        public Memory<byte> Buffer
        {
            get
            {
                return _ownedMemory.Memory.Slice(0, MessageLength);
            }
        }

        public int MessageLength { get; }

        public Message(IMemoryOwner<byte> buffer, int messageLength)
        {
            Id = Guid.NewGuid();
            _ownedMemory = buffer;
            MessageLength = messageLength;
        }

        public void Dispose()
        {
            _ownedMemory?.Dispose();
        }
    }
}
