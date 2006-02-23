using System;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client
{
    /// <summary>
    /// Default provider of ISession
    /// </summary>
    public class Session : ISession
    {
        private Connection connection;
        private AcknowledgementMode acknowledgementMode;
        private SessionInfo info;
        private long consumerCounter;
        private long producerCounter;
        private int prefetchSize = 1000;
        
        public Session(Connection connection, SessionInfo info)
        {
            this.connection = connection;
            this.info = info;
        }
        
        public void Dispose()
        {
            DisposeOf(info.SessionId);
        }
        
        public IMessageProducer CreateProducer()
        {
            return CreateProducer(null);
        }
        
        public IMessageProducer CreateProducer(IDestination destination)
        {
            ProducerInfo command = CreateProducerInfo(destination);
            connection.SyncRequest(command);
            return new MessageProducer(this, command);
        }
        
        public void Acknowledge(Message message)
        {
            if (acknowledgementMode == AcknowledgementMode.ClientAcknowledge)
            {
                MessageAck ack = new MessageAck();
                // TODO complete packet
                connection.SyncRequest(ack);
            }
        }
        
        public IMessageConsumer CreateConsumer(IDestination destination)
        {
            return CreateConsumer(destination, null);
        }
        
        public IMessageConsumer CreateConsumer(IDestination destination, string selector)
        {
            ConsumerInfo command = CreateConsumerInfo(destination, selector);
            connection.SyncRequest(command);
            MessageConsumer consumer = new MessageConsumer(this, command);
            connection.AddConsumer(command.ConsumerId, consumer);
            return consumer;
        }
        
        public IQueue GetQueue(string name)
        {
            return new ActiveMQQueue(name);
        }
        
        public ITopic GetTopic(string name)
        {
            return new ActiveMQTopic(name);
        }
        
        public IMessage CreateMessage()
        {
            ActiveMQMessage answer = new ActiveMQMessage();
            Configure(answer);
            return answer;
        }
        
        
        public ITextMessage CreateTextMessage()
        {
            ActiveMQTextMessage answer = new ActiveMQTextMessage();
            Configure(answer);
            return answer;
        }
        
        public ITextMessage CreateTextMessage(string text)
        {
            ActiveMQTextMessage answer = new ActiveMQTextMessage(text);
            Configure(answer);
            return answer;
        }
        
        // Implementation methods
        public void DoSend(IDestination destination, IMessage message)
        {
            ActiveMQMessage command = ActiveMQMessage.Transform(message);
            // TODO complete packet
            connection.SyncRequest(command);
        }
        
        public void DisposeOf(DataStructure objectId)
        {
            Console.WriteLine("Disposing of session: " + objectId + " with datatype: " + objectId.GetDataStructureType());
            /*
             RemoveInfo command = new RemoveInfo();
             command.ObjectId = objectId;
             connection.SyncRequest(command);
             */
        }
        
        public void DisposeOf(ConsumerId objectId)
        {
            Console.WriteLine("Disposing of consumer: " + objectId);
            connection.RemoveConsumer(objectId);
            /*
             RemoveInfo command = new RemoveInfo();
             command.ObjectId = objectId;
             connection.SyncRequest(command);
             */
        }
        
        protected ConsumerInfo CreateConsumerInfo(IDestination destination, string selector)
        {
            ConsumerInfo answer = new ConsumerInfo();
            ConsumerId id = new ConsumerId();
            id.ConnectionId = info.SessionId.ConnectionId;
            id.SessionId = info.SessionId.Value;
            lock (this)
            {
                id.Value = ++consumerCounter;
            }
            answer.ConsumerId = id;
            answer.Destination = (ActiveMQDestination) destination;
            answer.Selector = selector;
            answer.PrefetchSize = prefetchSize;
            
            // TODO configure other features on the consumer
            return answer;
        }
        
        protected ProducerInfo CreateProducerInfo(IDestination destination)
        {
            ProducerInfo answer = new ProducerInfo();
            ProducerId id = new ProducerId();
            id.ConnectionId = info.SessionId.ConnectionId;
            id.SessionId = info.SessionId.Value;
            lock (this)
            {
                id.Value = ++producerCounter;
            }
            answer.ProducerId = id;
            answer.Destination = (ActiveMQDestination) destination;
            return answer;
        }
        
        /// <summary>
        /// Configures the message command
        /// </summary>
        /// <param name="activeMQMessage">An ActiveMQMessage</param>
        /// <returns>An IMessage</retutns>
        protected void Configure(ActiveMQMessage message)
        {
        }
        
    }
}
