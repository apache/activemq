/*
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System;
using System.Collections;
using System.Threading;

using ActiveMQ.OpenWire;
using ActiveMQ.OpenWire.Commands;

namespace ActiveMQ
{
    public enum AckType
    {
        DeliveredAck = 0, // Message delivered but not consumed
        PoisonAck = 1,    // Message could not be processed due to poison pill but discard anyway
        ConsumedAck = 2   // Message consumed, discard
    }
    
    
    /// <summary>
    /// An object capable of receiving messages from some destination
    /// </summary>
    public class MessageConsumer : IMessageConsumer
    {
        
        private Session session;
        private ConsumerInfo info;
        private AcknowledgementMode acknowledgementMode;
        private bool closed;
        private Dispatcher dispatcher = new Dispatcher();
        private int maximumRedeliveryCount = 10;
        private int redeliveryTimeout = 500;
        
        public event MessageListener Listener;
        
        
        public MessageConsumer(Session session, ConsumerInfo info, AcknowledgementMode acknowledgementMode)
        {
            this.session = session;
            this.info = info;
            this.acknowledgementMode = acknowledgementMode;
        }
        
        public ConsumerId ConsumerId
        {
            get {
                return info.ConsumerId;
            }
        }
        
        public int MaximumRedeliveryCount
        {
            get { return maximumRedeliveryCount; }
            set { maximumRedeliveryCount = value; }
        }
        
        public int RedeliveryTimeout
        {
            get { return redeliveryTimeout; }
            set { redeliveryTimeout = value; }
        }
        
        public void RedeliverRolledBackMessages()
        {
            dispatcher.RedeliverRolledBackMessages();
        }
        
        /// <summary>
        /// Method Dispatch
        /// </summary>
        /// <param name="message">An ActiveMQMessage</param>
        public void Dispatch(ActiveMQMessage message)
        {
            dispatcher.Enqueue(message);
            
            if (Listener != null)
            {
                // lets dispatch to the thread pool for this connection for messages to be processed
                ThreadPool.QueueUserWorkItem(new WaitCallback(session.DispatchAsyncMessages));
            }
        }
        
        public IMessage Receive()
        {
            CheckClosed();
            return AutoAcknowledge(dispatcher.Dequeue());
        }
        
        public IMessage Receive(int timeout)
        {
            CheckClosed();
            return AutoAcknowledge(dispatcher.Dequeue(timeout));
        }
        
        public IMessage ReceiveNoWait()
        {
            CheckClosed();
            return AutoAcknowledge(dispatcher.DequeueNoWait());
        }
        
        
        
        public void Dispose()
        {
            session.DisposeOf(info.ConsumerId);
            closed = true;
        }
        
        /// <summary>
        /// Dispatch any pending messages to the asynchronous listener
        /// </summary>
        public void DispatchAsyncMessages()
        {
            while (Listener != null)
            {
                IMessage message = dispatcher.DequeueNoWait();
                if (message != null)
                {
                    Listener(message);
                }
                else
                {
                    break;
                }
            }
        }
        
        protected void CheckClosed()
        {
            if (closed)
            {
                throw new ConnectionClosedException();
            }
        }
        
        protected IMessage AutoAcknowledge(IMessage message)
        {
            if (message is ActiveMQMessage)
            {
                ActiveMQMessage activeMessage = (ActiveMQMessage) message;
                
                // lets register the handler for client acknowledgment
                activeMessage.Acknowledger += new AcknowledgeHandler(DoClientAcknowledge);
                
                if (acknowledgementMode != AcknowledgementMode.ClientAcknowledge)
                {
                    DoAcknowledge(activeMessage);
                }
            }
            return message;
        }
        
        protected void DoClientAcknowledge(ActiveMQMessage message)
        {
            if (acknowledgementMode == AcknowledgementMode.ClientAcknowledge)
            {
                DoAcknowledge(message);
            }
        }
        
        protected void DoAcknowledge(Message message)
        {
            MessageAck ack = CreateMessageAck(message);
            //Console.WriteLine("Sending Ack: " + ack);
            session.Connection.SyncRequest(ack);
        }
        
        
        protected virtual MessageAck CreateMessageAck(Message message)
        {
            MessageAck ack = new MessageAck();
            ack.AckType = (int) AckType.ConsumedAck;
            ack.ConsumerId = info.ConsumerId;
            ack.Destination = message.Destination;
            ack.FirstMessageId = message.MessageId;
            ack.LastMessageId = message.MessageId;
            ack.MessageCount = 1;
            
            if (session.Transacted)
            {
                session.DoStartTransaction();
                ack.TransactionId = session.TransactionContext.TransactionId;
                session.TransactionContext.AddSynchronization(new MessageConsumerSynchronization(this, message));
            }
            return ack;
        }
        
        public void AfterRollback(ActiveMQMessage message)
        {
            // lets redeliver the message again
            message.RedeliveryCounter += 1;
            if (message.RedeliveryCounter > MaximumRedeliveryCount)
            {
                // lets send back a poisoned pill
                MessageAck ack = new MessageAck();
                ack.AckType = (int) AckType.PoisonAck;
                ack.ConsumerId = info.ConsumerId;
                ack.Destination = message.Destination;
                ack.FirstMessageId = message.MessageId;
                ack.LastMessageId = message.MessageId;
                ack.MessageCount = 1;
                session.Connection.OneWay(ack);
            }
            else
            {
                dispatcher.Redeliver(message);
                
                if (Listener != null)
                {
                    // lets re-dispatch the message at some point in the future
                    Thread.Sleep(RedeliveryTimeout);
                    ThreadPool.QueueUserWorkItem(new WaitCallback(session.DispatchAsyncMessages));
                }
            }
        }
    }
    
    // TODO maybe there's a cleaner way of creating stateful delegates to make this code neater
    class MessageConsumerSynchronization : ISynchronization
    {
        private MessageConsumer consumer;
        private Message message;
        
        public MessageConsumerSynchronization(MessageConsumer consumer, Message message)
        {
            this.message = message;
            this.consumer = consumer;
        }
        
        public void BeforeCommit()
        {
        }
        
        public void AfterCommit()
        {
        }
        
        public void AfterRollback()
        {
            consumer.AfterRollback((ActiveMQMessage) message);
        }
        
    }
}
