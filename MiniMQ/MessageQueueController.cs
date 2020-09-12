using Serilog;
using System;
using System.Collections.Generic;
using System.Text;

namespace MiniMQ
{
    public class MessageQueueController
    {
        public MessageQueueController()
        {

        }

        // TODO: Probably don't want the full token, but a "MQRequest" object with the buffer and stuff.
        public MQActionResult ProcessMessage(MQAsyncUserToken token)
        {
            Log.Information("Got a message.");
            return new MQActionResult();
        }
    }
}
