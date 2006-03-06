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
using JMS;
using System;

namespace JMS
{
	
	/// <summary>
	/// Represents a single unit of work on an IConnection.
	/// So the ISession can be used to perform transactional receive and sends
	/// </summary>
	public interface ISession : IDisposable
    {
        
        /// <summary>
        /// Creates a producer of messages
        /// </summary>
        IMessageProducer CreateProducer();
        
        /// <summary>
        /// Creates a producer of messages on a given destination
        /// </summary>
        IMessageProducer CreateProducer(IDestination destination);
        
        /// <summary>
        /// Creates a consumer of messages on a given destination
        /// </summary>
        IMessageConsumer CreateConsumer(IDestination destination);
        
        /// <summary>
        /// Creates a consumer of messages on a given destination with a selector
        /// </summary>
        IMessageConsumer CreateConsumer(IDestination destination, string selector);
        
        /// <summary>
        /// Creates a named durable consumer of messages on a given destination with a selector
        /// </summary>
        IMessageConsumer CreateDurableConsumer(ITopic destination, string name, string selector, bool noLocal);
		
        /// <summary>
        /// Returns the queue for the given name
        /// </summary>
        IQueue GetQueue(string name);
        
        /// <summary>
        /// Returns the topic for the given name
        /// </summary>
        ITopic GetTopic(string name);
        
        
        /// <summary>
        /// Creates a temporary queue
        /// </summary>
        ITemporaryQueue CreateTemporaryQueue();
		
        /// <summary>
        /// Creates a temporary topic
        /// </summary>
        ITemporaryTopic CreateTemporaryTopic();
		
        
        // Factory methods to create messages
        
        /// <summary>
        /// Creates a new message with an empty body
        /// </summary>
        IMessage CreateMessage();
        
        /// <summary>
        /// Creates a new text message with an empty body
        /// </summary>
        ITextMessage CreateTextMessage();
        
        /// <summary>
        /// Creates a new text message with the given body
        /// </summary>
        ITextMessage CreateTextMessage(string text);
        
        /// <summary>
        /// Creates a new Map message which contains primitive key and value pairs
        /// </summary>
        IMapMessage CreateMapMessage();
        
        /// <summary>
        /// Creates a new binary message
        /// </summary>
        IBytesMessage CreateBytesMessage();
        
        /// <summary>
        /// Creates a new binary message with the given body
        /// </summary>
        IBytesMessage CreateBytesMessage(byte[] body);
		
		
        // Transaction methods
        
        /// <summary>
        /// If this is a transactional session then commit all message
        /// send and acknowledgements for producers and consumers in this session
        /// </summary>
        void Commit();
        
        /// <summary>
        /// If this is a transactional session then rollback all message
        /// send and acknowledgements for producers and consumers in this session
        /// </summary>
        void Rollback();
        
    }
}

