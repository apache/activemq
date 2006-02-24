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
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client
{
    public enum AckType {
        DeliveredAck = 0, // Message delivered but not consumed
        ConsumedAck = 1, // Message consumed, discard
        PoisonAck = 2 // Message could not be processed due to poison pill but discard anyway
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
        
        public event MessageHandler Listener;
        
        public MessageConsumer(Session session, ConsumerInfo info, AcknowledgementMode acknowledgementMode)
        {
            this.session = session;
            this.info = info;
            this.acknowledgementMode = acknowledgementMode;
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
            return AutoAcknowledge(dispatcher.Dequeue());
        }
        
        public IMessage Receive(long timeout)
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
        
        protected void DoClientAcknowledge(Message message)
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
            ack.TransactionId = message.TransactionId;
            return ack;
        }
        
        
    }
}
