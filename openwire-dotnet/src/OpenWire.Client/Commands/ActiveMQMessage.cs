using System;
using System.Collections;

using OpenWire.Client;
using OpenWire.Client.Core;

namespace OpenWire.Client.Commands {
        public class ActiveMQMessage : Message, IMessage {
                public const byte ID_ActiveMQMessage = 23;

                public static ActiveMQMessage Transform(IMessage message) {
                        return (ActiveMQMessage) message;
                }

                // TODO generate Equals method
                // TODO generate GetHashCode method
                // TODO generate ToString method

                public override byte GetCommandType() {
                        return ID_ActiveMQMessage; 
                }


                // Properties
                public IDestination FromDestination {
                        get {
                                return Destination;
                        }
                        set {
                                this.Destination = ActiveMQDestination.Transform(value);
                        }
                }

        } 
}
