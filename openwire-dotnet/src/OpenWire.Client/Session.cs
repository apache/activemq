using System;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client {
        /// <summary>
        /// Default provider of ISession
        /// </summary>
        public class Session : ISession {
                private Connection connection;
                private AcknowledgementMode acknowledgementMode;
                private SessionInfo info;
                private long consumerCounter;

                public Session(Connection connection, SessionInfo info) {
                        this.connection = connection;
                        this.info = info; 
                }

                public void Dispose() {
                        DisposeOf(info.SessionId); 
                }

                public IMessageProducer CreateProducer() {
                        return CreateProducer(null); 
                }

                public IMessageProducer CreateProducer(IDestination destination) {
                        ProducerInfo command = CreateProducerInfo(destination);
                        connection.SyncRequest(command);
                        return new MessageProducer(this, command); 
                }

                public void Acknowledge(Message message) {
                        if (acknowledgementMode == AcknowledgementMode.ClientAcknowledge) {
                                MessageAck ack = new MessageAck();
                                // TODO complete packet
                                connection.SyncRequest(ack); 
                        } 
                }

                public IMessageConsumer CreateConsumer(IDestination destination) {
                        return CreateConsumer(destination, null); 
                }

                public IMessageConsumer CreateConsumer(IDestination destination, string selector) {
                        ConsumerInfo command = CreateConsumerInfo(destination, selector);
                        connection.SyncRequest(command);
                        return new MessageConsumer(this, command); 
                }

                public IQueue GetQueue(string name) {
                        return new ActiveMQQueue(name);
                }

                public ITopic GetTopic(string name) {
                        return new ActiveMQTopic(name); 
                }

                // Implementation methods
                public void DoSend(IDestination destination, IMessage message) {
                        ActiveMQMessage command = ActiveMQMessage.Transform(message);
                        // TODO complete packet
                        connection.SyncRequest(command); 
                }

                public void DisposeOf(DataStructure objectId) {
                        RemoveInfo command = new RemoveInfo();
                        command.ObjectId = objectId;
                        connection.SyncRequest(command); 
                }

                protected ConsumerInfo CreateConsumerInfo(IDestination destination, string selector) {
                        ConsumerInfo answer = new ConsumerInfo();
                        ConsumerId consumerId = new ConsumerId();
                        consumerId.SessionId = info.SessionId.Value;
                        lock (this) {
                                consumerId.Value = ++consumerCounter; 
                        }
                        // TODO complete packet
                        answer.ConsumerId = consumerId;
                        return answer; 
                }

                protected ProducerInfo CreateProducerInfo(IDestination destination) {
                        ProducerInfo info = new ProducerInfo();
                        // TODO complete packet
                        return info; 
                } 
        } 
}
