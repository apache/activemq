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

		bool persistent;
		long timeToLive;
		int priority;
		bool disableMessageID;
		bool disableMessageTimestamp;

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
            
            session.DoSend(destination, message);
        }
        
        public void Dispose()
        {
            session.DisposeOf(info.ProducerId);
        }

		public bool Persistent
		{
			get { return persistent; }
			set { this.persistent = value; }
		}

		public long TimeToLive
		{
			get { return timeToLive; }
			set { this.timeToLive = value; }
		}

		public int Priority
		{
			get { return priority; }
			set { this.priority = value; }
		}

		public bool DisableMessageID
		{
			get { return disableMessageID; }
			set { this.disableMessageID = value; }
		}
		
		public bool DisableMessageTimestamp
		{
			get { return disableMessageTimestamp; }
			set { this.disableMessageTimestamp = value; }
		}

    }
}
