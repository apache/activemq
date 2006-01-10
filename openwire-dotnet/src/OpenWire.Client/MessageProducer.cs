using System;
using OpenWire.Client.Commands;

namespace OpenWire.Client {
        /// <summary>
        /// An object capable of sending messages to some destination
        /// </summary>
        public class MessageProducer : IMessageProducer {

                private Session session;
                private ProducerInfo info;

                public MessageProducer(Session session, ProducerInfo info) {
                        this.session = session;
                        this.info = info; 
                }

                public void Send(IMessage message) {
                        Send(info.Destination, message); 
                }

                public void Send(IDestination destination, IMessage message) {
                        session.DoSend(destination, message); 
                } 
                
                public void Dispose() {
                        session.DisposeOf(info.ProducerId);
                }
        } 
}
