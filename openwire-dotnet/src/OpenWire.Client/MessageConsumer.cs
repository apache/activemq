using System;
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
            Console.WriteLine("Dispatching message to consumer: " + message);
        }
        
        public IMessage Receive()
        {
            CheckClosed();
            Thread.Sleep(60000);
            // TODO
            return null;
        }
        
        public IMessage ReceiveNoWait()
        {
            CheckClosed();
            // TODO
            return null;
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
