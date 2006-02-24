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
