using System;
using System.Collections;
using System.Threading;
using OpenWire.Client.Commands;

namespace OpenWire.Client
{
    /// <summary>
    /// An object capable of receiving messages from some destination
    /// </summary>
    public class MessageConsumer : IMessageConsumer
    {
        
        private Session session;
        private ConsumerInfo info;
        private bool closed;
        private Dispatcher dispatcher = new Dispatcher();
        
        public event MessageHandler Listener;
        
        public MessageConsumer(Session session, ConsumerInfo info)
        {
            this.session = session;
            this.info = info;
        }
        
        /// <summary>
        /// Method Dispatch
        /// </summary>
        /// <param name="message">An ActiveMQMessage</param>
        public void Dispatch(ActiveMQMessage message)
        {
            dispatcher.Enqueue(message);
        }
        
        public IMessage Receive()
        {
            CheckClosed();
            return dispatcher.Dequeue();
        }
        
        public IMessage Receive(long timeout)
        {
            CheckClosed();
            return dispatcher.Dequeue(timeout);
        }
        
        public IMessage ReceiveNoWait()
        {
            CheckClosed();
            return dispatcher.DequeueNoWait();
        }
        
        public void Dispose()
        {
            session.DisposeOf(info.ConsumerId);
            closed = true;
        }
        
        protected void CheckClosed()
        {
            if (closed)
            {
                throw new ConnectionClosedException();
            }
        }
    }
}
