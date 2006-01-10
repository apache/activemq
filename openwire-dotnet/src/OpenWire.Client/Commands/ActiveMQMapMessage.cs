using System;
using System.Collections;

using OpenWire.Client;
using OpenWire.Client.Core;

namespace OpenWire.Client.Commands
{
    public class ActiveMQMapMessage : ActiveMQMessage
    {
    			public const byte ID_ActiveMQMapMessage = 25;
    			



        // TODO generate Equals method
        // TODO generate GetHashCode method
        // TODO generate ToString method


        public override byte GetCommandType() {
            return ID_ActiveMQMapMessage;
        }


        // Properties

    }
}
