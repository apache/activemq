using System;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client {
        /// <summary>
        /// Default provider of ISession
        /// </summary>
        public class Session : ISession, IDisposable {
                private Connection connection;
                private AcknowledgementMode acknowledgementMode;

                public Session(Connection connection, AcknowledgementMode acknowledgementMode) {
                        this.connection = connection;
                        this.acknowledgementMode = acknowledgementMode; 
                }

                public void Dispose() {
                } 
                
                public void Acknowledge(Message message) {
                        if (acknowledgementMode == AcknowledgementMode.ClientAcknowledge) {
                                MessageAck ack = new MessageAck();
                                connection.Transport.Request(ack);
                        }
                }
        } 
}
