using System;
using OpenWire.Client.Commands;

namespace OpenWire.Client {
        /// <summary>
        /// An object capable of receiving messages from some destination
        /// </summary>
        public class MessageConsumer : IMessageConsumer {

                private Session session;
                private ConsumerInfo info;
                private bool closed;

                public MessageConsumer(Session session, ConsumerInfo info) {
                        this.session = session;
                        this.info = info; 
                }

                public IMessage Receive() {
                        CheckClosed();
                        // TODO
                        return null; 
                }

                public IMessage ReceiveNoWait() {
                        CheckClosed();
                        // TODO
                        return null; 
                } 
                
                public void Dispose() {
                        session.DisposeOf(info.ConsumerId);
                        closed = true;
                }
                
                protected void CheckClosed() {
                        if (closed) {
                                throw new ConnectionClosedException();
                        }
                }
        } 
}
