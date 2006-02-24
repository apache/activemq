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
            activeMessage.Destination = ActiveMQDestination.Transform(destination);
            
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
