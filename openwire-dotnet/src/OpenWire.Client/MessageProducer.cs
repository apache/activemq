using System;
using OpenWire.Client.Commands;

namespace OpenWire.Client
{
    /// <summary>
    /// An object capable of sending messages to some destination
    /// </summary>
    public class MessageProducer : IMessageProducer
    {
        
        private Session session;
        private ProducerInfo info;
        private long messageCounter;
        
        
        public MessageProducer(Session session, ProducerInfo info)
        {
            this.session = session;
            this.info = info;
        }
        
        public void Send(IMessage message)
        {
            Send(info.Destination, message);
        }
        
        public void Send(IDestination destination, IMessage message)
        {
            MessageId id = new MessageId();
            id.ProducerId = info.ProducerId;
            lock (this)
            {
                id.ProducerSequenceId = ++messageCounter;
            }
            ActiveMQMessage activeMessage = (ActiveMQMessage) message;
            activeMessage.MessageId = id;
            activeMessage.ProducerId = info.ProducerId;
            activeMessage.Destination = (ActiveMQDestination) destination;
            
            Console.WriteLine("About to send message with MessageId: " + activeMessage.MessageId);
            Console.WriteLine("About to send message with ProducerId: " + activeMessage.ProducerId);
            Console.WriteLine("About to send message with Destination: " + activeMessage.Destination);
            session.DoSend(destination, message);
        }
        
        public void Dispose()
        {
            session.DisposeOf(info.ProducerId);
        }
    }
}
