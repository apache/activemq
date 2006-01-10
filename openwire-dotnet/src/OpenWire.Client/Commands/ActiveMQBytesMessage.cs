using System;
using System.Collections;

using OpenWire.Client;
using OpenWire.Client.Core;

namespace OpenWire.Client.Commands
{
    public class ActiveMQBytesMessage : ActiveMQMessage
    {
    			public const byte ID_ActiveMQBytesMessage = 24;
    			



        // TODO generate Equals method
        // TODO generate GetHashCode method
        // TODO generate ToString method


        public override byte GetCommandType() {
            return ID_ActiveMQBytesMessage;
        }


        // Properties

    }
}
