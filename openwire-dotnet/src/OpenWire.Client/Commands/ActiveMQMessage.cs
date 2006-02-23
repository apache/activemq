using System;
using System.Collections;

using OpenWire.Client;
using OpenWire.Client.Core;

namespace OpenWire.Client.Commands {
        public class ActiveMQMessage : Message, IMessage, MarshallAware {
                public const byte ID_ActiveMQMessage = 23;

                public static ActiveMQMessage Transform(IMessage message) {
                        return (ActiveMQMessage) message;
                }

                // TODO generate Equals method
                // TODO generate GetHashCode method
                // TODO generate ToString method

                public override byte GetDataStructureType() {
                        return ID_ActiveMQMessage;
                }

                
                public override bool IsMarshallAware() {
                        return true;
                }
                
                // Properties
                public IDestination FromDestination {
                        get { return Destination; }
                        set { this.Destination = ActiveMQDestination.Transform(value); }
                }

                public void BeforeMarshall(OpenWireFormat wireFormat) {
                }

                public void AfterMarshall(OpenWireFormat wireFormat) {
                }

                public void BeforeUnmarshall(OpenWireFormat wireFormat) {
                }
                public void AfterUnmarshall(OpenWireFormat wireFormat) {
                }

                public void SetMarshalledForm(OpenWireFormat wireFormat, byte[] data) {
                }

                public byte[] GetMarshalledForm(OpenWireFormat wireFormat) {
                        return null;
                }
        }
}
