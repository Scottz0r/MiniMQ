using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MiniMQ
{
    // TODO: Maybe don't make this static?
    public static class MessageQueue
    {
        // Queue of messages, ready to be sent.
        private static readonly BlockingCollection<Message> _messageQueue
            = new BlockingCollection<Message>(new ConcurrentQueue<Message>());

        // Holding = Waiting for an ACK to ensure message was received.
        private static readonly ConcurrentDictionary<Guid, Message> _holding = new ConcurrentDictionary<Guid, Message>();

        public static void Add(Message message)
        {
            _messageQueue.Add(message);
        }

        /// <summary>
        /// Take an item from the Queue. The item will be placed in the holding area until removed.
        /// </summary>
        public static Message Take(CancellationToken cancellationToken)
        {
            var message = _messageQueue.Take(cancellationToken);
            PutInHolding(message);
            return message;
        }

        private static void PutInHolding(Message message)
        {
            _holding[message.Id] = message;
        }

        public static void RemoveFromHolding(Guid id)
        {
            // Need to return message's buffer to the manager. This is the end of the message's lifetime.
            if(_holding.TryRemove(id, out Message message))
            {
                message.Dispose(); // Dispose to return memory to pool.
            }
        }

        public static void RequeueFromHolding(Guid id)
        {
            if(_holding.TryRemove(id, out Message message))
            {
                _messageQueue.Add(message);
            }
        }
    }
}
