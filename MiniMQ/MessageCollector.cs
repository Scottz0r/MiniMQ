using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace MiniMQ
{
    /// <summary>
    /// This class manages collecting message bytes into a single message. This has ownership of a memory buffer
    /// until the buffer is taken.
    /// </summary>
    public class MessageCollector : IDisposable
    {
        private IMemoryOwner<byte> _memory;

        public int CurrentSize { get; private set; }

        public int MessageSize { get; }

        public Memory<byte> Buffer
        {
            get
            {
                return _memory.Memory;
            }
        }

        public bool CollectionCompleted
        {
            get
            {
                return CurrentSize == MessageSize;
            }
        }

        public MessageCollector(int messageSize)
        {
            _memory = MemoryPool<byte>.Shared.Rent(messageSize);
            MessageSize = messageSize;
            CurrentSize = 0;
        }

        public void Append(ReadOnlySpan<byte> data)
        {
            var remainingBytes = MessageSize - CurrentSize;

            if (remainingBytes <= 0)
            {
                throw new InvalidOperationException("Message collection completed.");
            }

            var copyBytes = Math.Min(data.Length, remainingBytes);

            data.CopyTo(Buffer.Span.Slice(CurrentSize, copyBytes));
            CurrentSize += copyBytes;
        }

        public void Dispose()
        {
            _memory?.Dispose();
        }

        public IMemoryOwner<byte> TakeMemory()
        {
            var temp = _memory;
            _memory = null;
            return temp;
        }
    }
}
