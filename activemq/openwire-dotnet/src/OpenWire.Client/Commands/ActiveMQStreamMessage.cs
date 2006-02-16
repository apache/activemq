using System;
using System.Collections;

using OpenWire.Client;
using OpenWire.Client.Core;

namespace OpenWire.Client.Commands
{
    public class ActiveMQStreamMessage : ActiveMQMessage
    {
    			public const byte ID_ActiveMQStreamMessage = 27;
    			



        // TODO generate Equals method
        // TODO generate GetHashCode method
        // TODO generate ToString method


        public override byte GetCommandType() {
            return ID_ActiveMQStreamMessage;
        }


        // Properties

    }
}
