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
using NMS;



/// <summary>
/// Represents a message either to be sent to a message broker or received from a message broker
/// </summary>
namespace NMS
{
	public interface IMessage
    {
        
        /// <summary>
        /// If using client acknowledgement mode on the session then this method will acknowledge that the
        /// message has been processed correctly.
        /// </summary>
        void Acknowledge();
        
        /// <summary>
        /// Provides access to the message properties (headers)
        /// </summary>
        IPrimitiveMap Properties
		{
            get;
        }
        
        /// <summary>
        /// The correlation ID used to correlate messages from conversations or long running business processes
        /// </summary>
        string NMSCorrelationID
        {
            get;
            set;
        }
        
        /// <summary>
        /// The destination of the message
        /// </summary>
        IDestination NMSDestination
        {
            get;
        }
        
        /// <summary>
        /// The time in milliseconds that this message should expire in
        /// </summary>
        long NMSExpiration
        {
            get;
            set;
        }
        
        /// <summary>
        /// The message ID which is set by the provider
        /// </summary>
        string NMSMessageId
        {
            get;
        }
        
        /// <summary>
        /// Whether or not this message is persistent
        /// </summary>
        bool NMSPersistent
        {
            get;
            set;
        }
        
        /// <summary>
        /// The Priority on this message
        /// </summary>
        byte NMSPriority
        {
            get;
            set;
        }
        
        /// <summary>
        /// Returns true if this message has been redelivered to this or another consumer before being acknowledged successfully.
        /// </summary>
        bool NMSRedelivered
        {
            get;
        }
        
        
        /// <summary>
        /// The destination that the consumer of this message should send replies to
        /// </summary>
        IDestination NMSReplyTo
        {
            get;
            set;
        }
        
        
        /// <summary>
        /// The timestamp the broker added to the message
        /// </summary>
        long NMSTimestamp
        {
            get;
        }
        
        /// <summary>
        /// The type name of this message
        /// </summary>
        string NMSType
        {
            get;
            set;
        }
                        
    }
}


