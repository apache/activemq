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
    public delegate void MessageListener(IMessage message);
    
    /// <summary>
    /// A consumer of messages
    /// </summary>
    public interface IMessageConsumer : IDisposable
    {
        
        /// <summary>
        /// Waits until a message is available and returns it
        /// </summary>
        IMessage Receive();
        
        /// <summary>
        /// If a message is available within the timeout duration it is returned otherwise this method returns null
        /// </summary>
        IMessage Receive(int timeout);
        
        /// <summary>
        /// If a message is available immediately it is returned otherwise this method returns null
        /// </summary>
        IMessage ReceiveNoWait();
        
        /// <summary>
        /// An asynchronous listener which can be used to consume messages asynchronously
        /// </summary>
        event MessageListener Listener;
    }
}
