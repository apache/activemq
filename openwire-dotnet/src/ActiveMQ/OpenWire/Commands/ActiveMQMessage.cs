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

using OpenWire.Client;
using OpenWire.Client.Core;

namespace OpenWire.Client.Commands
{
    public delegate void AcknowledgeHandler(ActiveMQMessage message);
    
    public class ActiveMQMessage : Message, IMessage, MarshallAware
    {
        public const byte ID_ActiveMQMessage = 23;
        
        protected static MessagePropertyHelper propertyHelper = new MessagePropertyHelper();
        
        private PrimitiveMap properties;
        
        public event AcknowledgeHandler Acknowledger;
        
        public static ActiveMQMessage Transform(IMessage message)
        {
            return (ActiveMQMessage) message;
        }
        
        // TODO generate Equals method
        // TODO generate GetHashCode method
        
        
        public override byte GetDataStructureType()
        {
            return ID_ActiveMQMessage;
        }
        
        public void Acknowledge()
        {
            if (Acknowledger == null){
                throw new OpenWireException("No Acknowledger has been associated with this message: " + this);}
            else {
                Acknowledger(this);
            }
        }
        
        
        // Properties
        
        public IPrimitiveMap Properties
        {
            get {
                if (properties == null)
                {
                    properties = PrimitiveMap.Unmarshal(MarshalledProperties);
                }
                return properties;
            }
        }
        
        public IDestination FromDestination
        {
            get { return Destination; }
            set { this.Destination = ActiveMQDestination.Transform(value); }
        }
        
        
        // IMessage interface
        
        // JMS headers
        
        /// <summary>
        /// The correlation ID used to correlate messages with conversations or long running business processes
        /// </summary>
        public string JMSCorrelationID
        {
            get {
                return CorrelationId;
            }
            set {
                CorrelationId = value;
            }
        }
        
        /// <summary>
        /// The destination of the message
        /// </summary>
        public IDestination JMSDestination
        {
            get {
                return OriginalDestination;
            }
        }
        
        /// <summary>
        /// The time in milliseconds that this message should expire in
        /// </summary>
        public long JMSExpiration
        {
            get {
                return Expiration;
            }
            set {
                Expiration = value;
            }
        }
        
        /// <summary>
        /// The message ID which is set by the provider
        /// </summary>
        public string JMSMessageId
        {
            get {
                return BaseDataStreamMarshaller.ToString(MessageId);
            }
        }
        
        /// <summary>
        /// Whether or not this message is persistent
        /// </summary>
        public bool JMSPersistent
        {
            get {
                return Persistent;
            }
            set {
                Persistent = value;
            }
        }
        
        /// <summary>
        /// The Priority on this message
        /// </summary>
        public byte JMSPriority
        {
            get {
                return Priority;
            }
            set {
                Priority = value;
            }
        }
        
        /// <summary>
        /// Returns true if this message has been redelivered to this or another consumer before being acknowledged successfully.
        /// </summary>
        public bool JMSRedelivered
        {
            get {
                return RedeliveryCounter > 0;
            }
        }
        
        
        /// <summary>
        /// The destination that the consumer of this message should send replies to
        /// </summary>
        public IDestination JMSReplyTo
        {
            get {
                return ReplyTo;
            }
            set {
                ReplyTo = ActiveMQDestination.Transform(value);
            }
        }
        
        
        /// <summary>
        /// The timestamp the broker added to the message
        /// </summary>
        public long JMSTimestamp
        {
            get {
                return Timestamp;
            }
        }
        
        /// <summary>
        /// The type name of this message
        /// </summary>
        public string JMSType
        {
            get {
                return Type;
            }
            set {
                Type = value;
            }
        }
        
        
        // JMS Extension headers
        
        /// <summary>
        /// Returns the number of times this message has been redelivered to other consumers without being acknowledged successfully.
        /// </summary>
        public int JMSXDeliveryCount
        {
            get {
                return RedeliveryCounter + 1;
            }
        }
        
        
        /// <summary>
        /// The Message Group ID used to group messages together to the same consumer for the same group ID value
        /// </summary>
        public string JMSXGroupID
        {
            get {
                return GroupID;
            }
            set {
                GroupID = value;
            }
        }
        /// <summary>
        /// The Message Group Sequence counter to indicate the position in a group
        /// </summary>
        public int JMSXGroupSeq
        {
            get {
                return GroupSequence;
            }
            set {
                GroupSequence = value;
            }
        }
        
        /// <summary>
        /// Returns the ID of the producers transaction
        /// </summary>
        public string JMSXProducerTXID
        {
            get {
                TransactionId txnId = OriginalTransactionId;
                if (txnId == null)
                {
                    txnId = TransactionId;
                }
                if (txnId != null)
                {
                    return BaseDataStreamMarshaller.ToString(txnId);
                }
                return null;
            }
        }
        
        public object GetObjectProperty(string name)
        {
            return propertyHelper.GetObjectProperty(this, name);
        }
        
        public void SetObjectProperty(string name, object value)
        {
            propertyHelper.SetObjectProperty(this, name, value);
        }
        
        // MarshallAware interface
        
        public override bool IsMarshallAware()
        {
            return true;
        }
        
        public virtual void BeforeMarshall(OpenWireFormat wireFormat)
        {
            MarshalledProperties = null;
            if (properties != null)
            {
                MarshalledProperties = properties.Marshal();
            }
        }
        
        public virtual void AfterMarshall(OpenWireFormat wireFormat)
        {
        }
        
        public virtual void BeforeUnmarshall(OpenWireFormat wireFormat)
        {
        }
        
        public virtual void AfterUnmarshall(OpenWireFormat wireFormat)
        {
        }
        
        public virtual void SetMarshalledForm(OpenWireFormat wireFormat, byte[] data)
        {
        }
        
        public virtual byte[] GetMarshalledForm(OpenWireFormat wireFormat)
        {
            return null;
        }
        
    }
}
