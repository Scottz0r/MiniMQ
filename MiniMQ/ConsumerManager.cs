using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using Serilog;

namespace MiniMQ
{
    public class ConsumerManager
    {
        private ConcurrentDictionary<Guid, Consumer> _consumers = new ConcurrentDictionary<Guid, Consumer>();

        // TODO: Benchmark Queue performance. There could be many thousands of queue/dequeue going on.
        private ConcurrentQueue<Guid> _readyConsumers = new ConcurrentQueue<Guid>();

        private AutoResetEvent _clientReadyEvent = new AutoResetEvent(false);

        public ConsumerManager()
        {
            // TODO: Probably need to inject a Message Manager to be able to ACK sent messages and tie those loose ends up.
        }

        public void Add(Consumer consumer)
        {
            // Caution: These are going to be coming from different threads.
            consumer.OnStateChanged += ConsumerStateChange;
            consumer.OnMessageAck += ConsumerMessageAck;

            if(_consumers.TryAdd(consumer.Id, consumer))
            {
                _readyConsumers.Enqueue(consumer.Id);
                _clientReadyEvent.Set();
            }
            else
            {
                // TODO: This should happen, but handle anyway.
                Log.Error("Consumer {Id} failed to be added. Disconnecting.", consumer.Id);
                consumer.Close(true);
            }
        }

        public Consumer NextAvailableConsumer()
        {
            // Infinite loop to block until a consumer is ready. Because of concurrent state, a ready consumer may have bee closed before
            // this manager can return it.
            for(; ; )
            {
                if (_readyConsumers.IsEmpty)
                {
                    _clientReadyEvent.WaitOne();
                }

                if (_readyConsumers.TryDequeue(out Guid consumerId))
                {
                    if(_consumers.TryGetValue(consumerId, out Consumer consumer))
                    {
                        return consumer;
                    }
                }
            }
        }

        public void Shutdown()
        {
            foreach(var kvp in _consumers)
            {
                try
                {
                    kvp.Value.Close(true);
                }
                catch(Exception ex)
                {
                    Log.Error("And exception occurred when attempting to shutdown all clients: {Error}", ex);
                }
            }

            _consumers.Clear();
            _readyConsumers.Clear();
        }

        private void ConsumerStateChange(object sender, EventArgs e)
        {
            Consumer consumer = (Consumer)sender;

            if(consumer.State == ConsumerState.Closed)
            {
                // If the consumer is holding on to a message, it needs to be put back into the queue.
                if(consumer.CurrentMessageId.HasValue)
                {
                    MessageQueue.RequeueFromHolding(consumer.CurrentMessageId.Value);
                }

                _consumers.TryRemove(consumer.Id, out Consumer _);
                // No need to touch queue. If the ID isn't in the consumer's dictionary, then it will be ignored.
            }
            else if(consumer.State == ConsumerState.Ready)
            {
                // Consumer is ready to be used. Put back into the queue and trigger cross thread signal (which may not be needed).
                _readyConsumers.Enqueue(consumer.Id);
                _clientReadyEvent.Set();
            }
        }

        private void ConsumerMessageAck(object sender, Guid messageId)
        {
            MessageQueue.RemoveFromHolding(messageId);
        }
    }
}
