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
    /// Represents a message either to be sent to a message broker or received from a message broker
    /// </summary>
    public interface IMessage
    {
        
        /// <summary>
        /// Provides access to the message properties (headers)
        /// </summary>
        IPrimitiveMap Properties {
            get;
        }
        
        /// <summary>
        /// The correlation ID used to correlate messages from conversations or long running business processes
        /// </summary>
        string JMSCorrelationID
        {
            get;
            set;
        }
        
        /// <summary>
        /// The destination of the message
        /// </summary>
        IDestination JMSDestination
        {
            get;
        }
        
        /// <summary>
        /// The time in milliseconds that this message should expire in
        /// </summary>
        long JMSExpiration
        {
            get;
            set;
        }
        
        /// <summary>
        /// The message ID which is set by the provider
        /// </summary>
        string JMSMessageId
        {
            get;
        }
        
        /// <summary>
        /// Whether or not this message is persistent
        /// </summary>
        bool JMSPersistent
        {
            get;
            set;
        }
        
        /// <summary>
        /// The Priority on this message
        /// </summary>
        byte JMSPriority
        {
            get;
            set;
        }
        
        /// <summary>
        /// Returns true if this message has been redelivered to this or another consumer before being acknowledged successfully.
        /// </summary>
        bool JMSRedelivered
        {
            get;
        }
        
        
        /// <summary>
        /// The destination that the consumer of this message should send replies to
        /// </summary>
        IDestination JMSReplyTo
        {
            get;
            set;
        }
        
        
        /// <summary>
        /// The timestamp the broker added to the message
        /// </summary>
        long JMSTimestamp
        {
            get;
        }
        
        /// <summary>
        /// The type name of this message
        /// </summary>
        string JMSType
        {
            get;
            set;
        }
        
        
        // JMS Extension headers
        
        /// <summary>
        /// Returns the number of times this message has been redelivered to other consumers without being acknowledged successfully.
        /// </summary>
        int JMSXDeliveryCount
        {
            get;
        }
        
        
        /// <summary>
        /// The Message Group ID used to group messages together to the same consumer for the same group ID value
        /// </summary>
        string JMSXGroupID
        {
            get;
            set;
        }
        /// <summary>
        /// The Message Group Sequence counter to indicate the position in a group
        /// </summary>
        int JMSXGroupSeq
        {
            get;
            set;
        }
        
        /// <summary>
        /// Returns the ID of the producers transaction
        /// </summary>
        string JMSXProducerTXID
        {
            get;
        }
        
    }
}
