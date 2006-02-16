using System;
using System.Collections;

namespace ActiveMQ
{
	/// <summary>
	/// Summary description for ActiveMQMessage.
	/// </summary>
	public class ActiveMQMessage : AbstractPacket 
	{

		const int DEFAULT_DELIVERY_MODE = PERSISTENT;

		/**
		 * The message producer's default priority is 4.
		 */
		const int DEFAULT_PRIORITY = 4;

		/**
		 * The message producer's default time to live is unlimited; the message
		 * never expires.
		 */
		const long DEFAULT_TIME_TO_LIVE = 0;

		/**
		 * message property types
		 */
		const byte EOF = 2;
		const byte BYTES = 3;
		const byte STRING = 4;
		const byte BOOLEAN = 5;
		const byte CHAR = 6;
		const byte BYTE = 7;
		const byte SHORT = 8;
		const byte INT = 9;
		const byte LONG = 10;
		const byte FLOAT = 11;
		const byte DOUBLE = 12;
		const byte NULL = 13;

		/**
		 * Message flag indexes (used for writing/reading to/from a Stream
		 */
    
		public const int CORRELATION_INDEX = 2;
		public const int TYPE_INDEX = 3;
		public const int BROKER_NAME_INDEX = 4;
		public const int CLUSTER_NAME_INDEX = 5;
		public const int TRANSACTION_ID_INDEX = 6;
		public const int REPLY_TO_INDEX = 7;
		public const int TIMESTAMP_INDEX = 8;
		public const int EXPIRATION_INDEX = 9;
		public const int REDELIVERED_INDEX = 10;
		public const int XA_TRANS_INDEX = 11;
		public const int CID_INDEX = 12;
		public const int PROPERTIES_INDEX = 13;
		public const int DISPATCHED_FROM_DLQ_INDEX = 14;
		public const int PAYLOAD_INDEX = 15;
		public const int EXTERNAL_MESSAGE_ID_INDEX = 16;
		public const int MESSAGE_PART_INDEX = 17;
		public const int CACHED_VALUES_INDEX = 18;
		public const int CACHED_DESTINATION_INDEX = 19;
		public const int LONG_SEQUENCE_INDEX = 20;
    


		const String DELIVERY_COUNT_NAME = "JMSXDeliveryCount";
		/**
		 * <code>readOnlyMessage</code> denotes if the message is read only
		 */
		protected bool readOnlyMessage;

		private String jmsMessageID;
		private String jmsClientID;
		private String jmsCorrelationID;
		private String producerKey;
		private ActiveMQDestination jmsDestination;
		private ActiveMQDestination jmsReplyTo;
		private int jmsDeliveryMode = DEFAULT_DELIVERY_MODE;
		private bool jmsRedelivered;
		private String jmsType;
		private long jmsExpiration;
		private int jmsPriority = DEFAULT_PRIORITY;
		private long jmsTimestamp;
		private Hashtable properties;
		private bool readOnlyProperties;
		private String entryBrokerName;
		private String entryClusterName;
		private int[] consumerNos; //these are set by the broker, and only relevant to consuming connections
		private Object transactionId;
		private bool xaTransacted;
		private String consumerIdentifier; //this is only used on the Client for acknowledging receipt of a message
		private bool messageConsumed;//only used on the client - to denote if its been delivered and read
		private bool transientConsumed;//only used on the client - to denote if its been delivered and read
		private long sequenceNumber;//the sequence for this message from the producerId
		private int deliveryCount = 1;//number of times the message has been delivered
		private bool dispatchedFromDLQ;
		private MessageAcknowledge messageAcknowledge;
		private byte[] bodyAsBytes;
		private Object jmsMessageIdentity;
		private short messsageHandle;//refers to the id of the MessageProducer that sent the message
		private bool externalMessageId;//is the messageId set from another JMS implementation ?
		private bool messagePart;//is the message split into multiple packets
		private short numberOfParts;
		private short partNumber;
		private String parentMessageID;//if split into multiple parts - the 'real' or first messageId


		/**
		 * Retrieve if a JMS Message type or not
		 *
		 * @return true if it is a JMS Message
		 */
		public override bool isJMSMessage() 
		{
			return true;
		}


		/**
		 * @return pretty print of this Message
		 */
		public override String ToString() 
		{
			return super.toString() + " ActiveMQMessage{ " +
				", jmsMessageID = " + jmsMessageID +
				", bodyAsBytes = " + bodyAsBytes +
				", readOnlyMessage = " + readOnlyMessage +
				", jmsClientID = '" + jmsClientID + "' " +
				", jmsCorrelationID = '" + jmsCorrelationID + "' " +
				", jmsDestination = " + jmsDestination +
				", jmsReplyTo = " + jmsReplyTo +
				", jmsDeliveryMode = " + jmsDeliveryMode +
				", jmsRedelivered = " + jmsRedelivered +
				", jmsType = '" + jmsType + "' " +
				", jmsExpiration = " + jmsExpiration +
				", jmsPriority = " + jmsPriority +
				", jmsTimestamp = " + jmsTimestamp +
				", properties = " + properties +
				", readOnlyProperties = " + readOnlyProperties +
				", entryBrokerName = '" + entryBrokerName + "' " +
				", entryClusterName = '" + entryClusterName + "' " +
				", consumerNos = " + consumerNos +
				", transactionId = '" + transactionId + "' " +
				", xaTransacted = " + xaTransacted +
				", consumerIdentifer = '" + consumerIdentifier + "' " +
				", messageConsumed = " + messageConsumed +
				", transientConsumed = " + transientConsumed +
				", sequenceNumber = " + sequenceNumber +
				", deliveryCount = " + deliveryCount +
				", dispatchedFromDLQ = " + dispatchedFromDLQ +
				", messageAcknowledge = " + messageAcknowledge +
				", jmsMessageIdentity = " + jmsMessageIdentity +
				", producerKey = " + producerKey + 
				" }";
		}


		/**
		 * @return Returns the messageAcknowledge.
		 */
		public MessageAcknowledge getMessageAcknowledge() 
		{
			return messageAcknowledge;
		}

		/**
		 * @param messageAcknowledge The messageAcknowledge to set.
		 */
		public void setMessageAcknowledge(MessageAcknowledge messageAcknowledge) 
		{
			this.messageAcknowledge = messageAcknowledge;
		}

		/**
		 * Return the type of Packet
		 *
		 * @return integer representation of the type of Packet
		 */

		public new int getPacketType() 
		{
			return ACTIVEMQ_MESSAGE;
		}


		/**
		 * set the message readOnly
		 *
		 * @param value
		 */
		public void setReadOnly(bool value) 
		{
			this.readOnlyProperties = value;
			this.readOnlyMessage = value;
		}

		/**
		 * test to see if a particular Consumer at a Connection
		 * is meant to receive this Message
		 *
		 * @param consumerNumber
		 * @return true if a target
		 */

		public bool isConsumerTarget(int consumerNumber) 
		{
			if (consumerNos != null) 
			{
				for (int i = 0; i < consumerNos.length; i++) 
				{
					if (consumerNos[i] == consumerNumber) 
					{
						return true;
					}
				}
			}
			return false;
		}

		/**
		 * @return consumer Nos as a String
		 */
		public String getConsumerNosAsString() 
		{
			String result = "";
			if (consumerNos != null) 
			{
				for (int i = 0; i < consumerNos.length; i++) 
				{
					result += consumerNos[i] + ",";
				}
			}
			return result;
		}

		/**
		 * @return true if the message is non-persistent or intended for a temporary destination
		 */
		public bool isTemporary() 
		{
			return jmsDeliveryMode == DeliveryMode.NON_PERSISTENT ||
				(jmsDestination != null && jmsDestination.isTemporary());
		}

		/**
		 * @return Returns hash code for this instance
		 */

		public new int GetHashCode() 
		{
			return this.getJMSMessageID() != null ? this.getJMSMessageID().hashCode() : super.hashCode();
		}

		/**
		 * Returns true if this instance is equivalent to obj
		 *
		 * @param obj the other instance to test
		 * @return true/false
		 */

		public bool equals(Object obj) 
		{
			bool result = obj == this;
			if (!result && obj != null && obj is ActiveMQMessage) 
			{
				ActiveMQMessage other = (ActiveMQMessage) obj;
				//the call getJMSMessageID() will initialize the messageID
				//if it hasn't already been set
				result = this.getJMSMessageID() == other.getJMSMessageID();
				if (!result)
				{
					if (this.jmsMessageID != null && this.jmsMessageID.length() > 0 ||
						other.jmsMessageID != null && other.jmsMessageID.length() > 0)
					{
						if (this.jmsMessageID != null && other.jmsMessageID != null)
						{
							result = this.jmsMessageID.equals(other.jmsMessageID);
						}
					}
					else
					{
						result = this.getId() == other.getId();
					}
				}
			}
			return result;
		}

  


		/**
		 * @return Returns a shallow copy of the message instance
		 * @
		 */

		public ActiveMQMessage shallowCopy() 
		{
			ActiveMQMessage other = new ActiveMQMessage();
			this.initializeOther(other);
			return other;
		}

		/**
		 * @return Returns a deep copy of the message - note the header fields are only shallow copied
		 * @
		 */

		public ActiveMQMessage deepCopy() 
		{
			return shallowCopy();
		}


		/**
		 * Indicates if the Message has expired
		 *
		 * @param currentTime -
		 *                    the current time in milliseconds
		 * @return true if the message can be expired
		 */
		public bool isExpired(long currentTime) 
		{
			bool result = false;
			long expiration = this.jmsExpiration;
			if (jmsExpiration > 0 && jmsExpiration < currentTime) 
			{
				result = true;
			}
			return result;
		}

		/**
		 * @return true if the message is expired
		 */
		public bool isExpired() 
		{
			return !dispatchedFromDLQ && jmsExpiration > 0 && isExpired(System.currentTimeMillis());
		}
    
		/**
		 * @return true if an advisory message
		 */
		public bool isAdvisory()
		{
			return jmsDestination != null && jmsDestination.isAdvisory();
		}

		/**
		 * Initializes another message with current values from this instance
		 *
		 * @param other the other ActiveMQMessage to initialize
		 */
		protected void initializeOther(ActiveMQMessage other) 
		{
			super.initializeOther(other);
			other.jmsMessageID = this.jmsMessageID;
			other.jmsClientID = this.jmsClientID;
			other.jmsCorrelationID = this.jmsCorrelationID;
			other.jmsDestination = this.jmsDestination;
			other.jmsReplyTo = this.jmsReplyTo;
			other.jmsDeliveryMode = this.jmsDeliveryMode;
			other.jmsRedelivered = this.jmsRedelivered;
			other.jmsType = this.jmsType;
			other.jmsExpiration = this.jmsExpiration;
			other.jmsPriority = this.jmsPriority;
			other.jmsTimestamp = this.jmsTimestamp;
			other.properties = this.properties != null ? new Hashtable(this.properties) : null;
			other.readOnlyProperties = this.readOnlyProperties;
			other.readOnlyMessage = this.readOnlyMessage;
			other.entryBrokerName = this.entryBrokerName;
			other.entryClusterName = this.entryClusterName;
			other.consumerNos = this.consumerNos;
			other.transactionId = this.transactionId;
			other.xaTransacted = this.xaTransacted;
			other.bodyAsBytes = this.bodyAsBytes;
			other.messageAcknowledge = this.messageAcknowledge;
			other.jmsMessageIdentity = this.jmsMessageIdentity;
			other.sequenceNumber = this.sequenceNumber;
			other.deliveryCount = this.deliveryCount;
			other.dispatchedFromDLQ = this.dispatchedFromDLQ;
			other.messsageHandle = this.messsageHandle;
			other.consumerIdentifier = this.consumerIdentifier;
			other.externalMessageId = this.externalMessageId;
			other.producerKey = this.producerKey;
			other.messagePart = this.messagePart;
			other.numberOfParts = this.numberOfParts;
			other.partNumber = this.partNumber;
			other.parentMessageID = this.parentMessageID;
		}
    

		/**
		 * Gets the message ID.
		 * <p/>
		 * <P>The <CODE>JMSMessageID</CODE> header field contains a value that
		 * uniquely identifies each message sent by a provider.
		 * <p/>
		 * <P>When a message is sent, <CODE>JMSMessageID</CODE> can be ignored.
		 * When the <CODE>send</CODE> or <CODE>publish</CODE> method returns, it
		 * contains a provider-assigned value.
		 * <p/>
		 * <P>A <CODE>JMSMessageID</CODE> is a <CODE>String</CODE> value that
		 * should function as a
		 * unique key for identifying messages in a historical repository.
		 * The exact scope of uniqueness is provider-defined. It should at
		 * least cover all messages for a specific installation of a
		 * provider, where an installation is some connected set of message
		 * routers.
		 * <p/>
		 * <P>All <CODE>JMSMessageID</CODE> values must start with the prefix
		 * <CODE>'ID:'</CODE>.
		 * Uniqueness of message ID values across different providers is
		 * not required.
		 * <p/>
		 * <P>Since message IDs take some effort to create and increase a
		 * message's size, some JMS providers may be able to optimize message
		 * overhead if they are given a hint that the message ID is not used by
		 * an application. By calling the
		 * <CODE>MessageProducer.setDisableMessageID</CODE> method, a JMS client
		 * enables this potential optimization for all messages sent by that
		 * message producer. If the JMS provider accepts this
		 * hint, these messages must have the message ID set to null; if the
		 * provider ignores the hint, the message ID must be set to its normal
		 * unique value.
		 *
		 * @return the message ID
		 * @see javax.jms.Message#setJMSMessageID(String)
		 * @see javax.jms.MessageProducer#setDisableMessageID(bool)
		 */

		public String getJMSMessageID() 
		{
			if (jmsMessageID == null && producerKey != null)
			{
				jmsMessageID = producerKey + sequenceNumber;
			}
			return jmsMessageID;
		}


		/**
		 * Sets the message ID.
		 * <p/>
		 * <P>JMS providers set this field when a message is sent. This method
		 * can be used to change the value for a message that has been received.
		 *
		 * @param id the ID of the message
		 * @see javax.jms.Message#getJMSMessageID()
		 */

		public void setJMSMessageID(String id) 
		{
			this.jmsMessageID = id;
			this.jmsMessageIdentity = null;
		}


		/**
		 * Gets the message timestamp.
		 * <p/>
		 * <P>The <CODE>JMSTimestamp</CODE> header field contains the time a
		 * message was
		 * handed off to a provider to be sent. It is not the time the
		 * message was actually transmitted, because the actual send may occur
		 * later due to transactions or other client-side queueing of messages.
		 * <p/>
		 * <P>When a message is sent, <CODE>JMSTimestamp</CODE> is ignored. When
		 * the <CODE>send</CODE> or <CODE>publish</CODE>
		 * method returns, it contains a time value somewhere in the interval
		 * between the call and the return. The value is in the format of a normal
		 * millis time value in the Java programming language.
		 * <p/>
		 * <P>Since timestamps take some effort to create and increase a
		 * message's size, some JMS providers may be able to optimize message
		 * overhead if they are given a hint that the timestamp is not used by an
		 * application. By calling the
		 * <CODE>MessageProducer.setDisableMessageTimestamp</CODE> method, a JMS
		 * client enables this potential optimization for all messages sent by
		 * that message producer. If the JMS provider accepts this
		 * hint, these messages must have the timestamp set to zero; if the
		 * provider ignores the hint, the timestamp must be set to its normal
		 * value.
		 *
		 * @return the message timestamp
		 * @see javax.jms.Message#setJMSTimestamp(long)
		 * @see javax.jms.MessageProducer#setDisableMessageTimestamp(bool)
		 */

		public long getJMSTimestamp() 
		{
			return jmsTimestamp;
		}


		/**
		 * Sets the message timestamp.
		 * <p/>
		 * <P>JMS providers set this field when a message is sent. This method
		 * can be used to change the value for a message that has been received.
		 *
		 * @param timestamp the timestamp for this message
		 * @see javax.jms.Message#getJMSTimestamp()
		 */

		public void setJMSTimestamp(long timestamp) 
		{
			this.jmsTimestamp = timestamp;
		}


		/**
		 * Gets the correlation ID as an array of bytes for the message.
		 * <p/>
		 * <P>The use of a <CODE>byte[]</CODE> value for
		 * <CODE>JMSCorrelationID</CODE> is non-portable.
		 *
		 * @return the correlation ID of a message as an array of bytes
		 * @see javax.jms.Message#setJMSCorrelationID(String)
		 * @see javax.jms.Message#getJMSCorrelationID()
		 * @see javax.jms.Message#setJMSCorrelationIDAsBytes(byte[])
		 */

		public byte[] getJMSCorrelationIDAsBytes() 
		{
			return this.jmsCorrelationID != null ? this.jmsCorrelationID.getBytes() : null;
		}


		/**
		 * Sets the correlation ID as an array of bytes for the message.
		 * <p/>
		 * <P>The array is copied before the method returns, so
		 * future modifications to the array will not alter this message header.
		 * <p/>
		 * <P>If a provider supports the native concept of correlation ID, a
		 * JMS client may need to assign specific <CODE>JMSCorrelationID</CODE>
		 * values to match those expected by native messaging clients.
		 * JMS providers without native correlation ID values are not required to
		 * support this method and its corresponding get method; their
		 * implementation may throw a
		 * <CODE>java.lang.UnsupportedOperationException</CODE>.
		 * <p/>
		 * <P>The use of a <CODE>byte[]</CODE> value for
		 * <CODE>JMSCorrelationID</CODE> is non-portable.
		 *
		 * @param correlationID the correlation ID value as an array of bytes
		 * @see javax.jms.Message#setJMSCorrelationID(String)
		 * @see javax.jms.Message#getJMSCorrelationID()
		 * @see javax.jms.Message#getJMSCorrelationIDAsBytes()
		 */

		public void setJMSCorrelationIDAsBytes(byte[] correlationID) 
		{
			if (correlationID == null) 
			{
				this.jmsCorrelationID = null;
			}
			else 
			{
				this.jmsCorrelationID = new String(correlationID);
			}
		}


		/**
		 * Sets the correlation ID for the message.
		 * <p/>
		 * <P>A client can use the <CODE>JMSCorrelationID</CODE> header field to
		 * link one message with another. A typical use is to link a response
		 * message with its request message.
		 * <p/>
		 * <P><CODE>JMSCorrelationID</CODE> can hold one of the following:
		 * <UL>
		 * <LI>A provider-specific message ID
		 * <LI>An application-specific <CODE>String</CODE>
		 * <LI>A provider-native <CODE>byte[]</CODE> value
		 * </UL>
		 * <p/>
		 * <P>Since each message sent by a JMS provider is assigned a message ID
		 * value, it is convenient to link messages via message ID. All message ID
		 * values must start with the <CODE>'ID:'</CODE> prefix.
		 * <p/>
		 * <P>In some cases, an application (made up of several clients) needs to
		 * use an application-specific value for linking messages. For instance,
		 * an application may use <CODE>JMSCorrelationID</CODE> to hold a value
		 * referencing some external information. Application-specified values
		 * must not start with the <CODE>'ID:'</CODE> prefix; this is reserved for
		 * provider-generated message ID values.
		 * <p/>
		 * <P>If a provider supports the native concept of correlation ID, a JMS
		 * client may need to assign specific <CODE>JMSCorrelationID</CODE> values
		 * to match those expected by clients that do not use the JMS API. A
		 * <CODE>byte[]</CODE> value is used for this
		 * purpose. JMS providers without native correlation ID values are not
		 * required to support <CODE>byte[]</CODE> values. The use of a
		 * <CODE>byte[]</CODE> value for <CODE>JMSCorrelationID</CODE> is
		 * non-portable.
		 *
		 * @param correlationID the message ID of a message being referred to
		 * @see javax.jms.Message#getJMSCorrelationID()
		 * @see javax.jms.Message#getJMSCorrelationIDAsBytes()
		 * @see javax.jms.Message#setJMSCorrelationIDAsBytes(byte[])
		 */

		public void setJMSCorrelationID(String correlationID) 
		{
			this.jmsCorrelationID = correlationID;
		}


		/**
		 * Gets the correlation ID for the message.
		 * <p/>
		 * <P>This method is used to return correlation ID values that are
		 * either provider-specific message IDs or application-specific
		 * <CODE>String</CODE> values.
		 *
		 * @return the correlation ID of a message as a <CODE>String</CODE>
		 * @see javax.jms.Message#setJMSCorrelationID(String)
		 * @see javax.jms.Message#getJMSCorrelationIDAsBytes()
		 * @see javax.jms.Message#setJMSCorrelationIDAsBytes(byte[])
		 */

		public String getJMSCorrelationID() 
		{
			return this.jmsCorrelationID;
		}


		/**
		 * Gets the <CODE>Destination</CODE> object to which a reply to this
		 * message should be sent.
		 *
		 * @return <CODE>Destination</CODE> to which to send a response to this
		 *         message
		 * @see javax.jms.Message#setJMSReplyTo(Destination)
		 */

		public ActiveMQDestination getJMSReplyTo() 
		{
			return this.jmsReplyTo;

		}


		/**
		 * Sets the <CODE>Destination</CODE> object to which a reply to this
		 * message should be sent.
		 * <p/>
		 * <P>The <CODE>JMSReplyTo</CODE> header field contains the destination
		 * where a reply
		 * to the current message should be sent. If it is null, no reply is
		 * expected. The destination may be either a <CODE>Queue</CODE> object or
		 * a <CODE>Topic</CODE> object.
		 * <p/>
		 * <P>Messages sent with a null <CODE>JMSReplyTo</CODE> value may be a
		 * notification of some event, or they may just be some data the sender
		 * thinks is of interest.
		 * <p/>
		 * <P>Messages with a <CODE>JMSReplyTo</CODE> value typically expect a
		 * response. A response is optional; it is up to the client to decide.
		 * These messages are called requests. A message sent in response to a
		 * request is called a reply.
		 * <p/>
		 * <P>In some cases a client may wish to match a request it sent earlier
		 * with a reply it has just received. The client can use the
		 * <CODE>JMSCorrelationID</CODE> header field for this purpose.
		 *
		 * @param replyTo <CODE>Destination</CODE> to which to send a response to
		 *                this message
		 * @see javax.jms.Message#getJMSReplyTo()
		 */

		public void setJMSReplyTo(ActiveMQDestination replyTo) 
		{
			this.jmsReplyTo = (ActiveMQDestination) replyTo;
		}


		/**
		 * Gets the <CODE>Destination</CODE> object for this message.
		 * <p/>
		 * <P>The <CODE>JMSDestination</CODE> header field contains the
		 * destination to which the message is being sent.
		 * <p/>
		 * <P>When a message is sent, this field is ignored. After completion
		 * of the <CODE>send</CODE> or <CODE>publish</CODE> method, the field
		 * holds the destination specified by the method.
		 * <p/>
		 * <P>When a message is received, its <CODE>JMSDestination</CODE> value
		 * must be equivalent to the value assigned when it was sent.
		 *
		 * @return the destination of this message
		 * @see javax.jms.Message#setJMSDestination(Destination)
		 */

		public ActiveMQDestination getJMSDestination() 
		{
			return this.jmsDestination;
		}


		/**
		 * Sets the <CODE>Destination</CODE> object for this message.
		 * <p/>
		 * <P>JMS providers set this field when a message is sent. This method
		 * can be used to change the value for a message that has been received.
		 *
		 * @param destination the destination for this message
		 * @see javax.jms.Message#getJMSDestination()
		 */

		public void setJMSDestination(ActiveMQDestination destination) 
		{
			this.jmsDestination = (ActiveMQDestination) destination;
		}


		/**
		 * Gets the <CODE>DeliveryMode</CODE> value specified for this message.
		 *
		 * @return the delivery mode for this message
		 * @see javax.jms.Message#setJMSDeliveryMode(int)
		 * @see javax.jms.DeliveryMode
		 */

		public int getJMSDeliveryMode() 
		{
			return this.jmsDeliveryMode;
		}


		/**
		 * Sets the <CODE>DeliveryMode</CODE> value for this message.
		 * <p/>
		 * <P>JMS providers set this field when a message is sent. This method
		 * can be used to change the value for a message that has been received.
		 *
		 * @param deliveryMode the delivery mode for this message
		 * @see javax.jms.Message#getJMSDeliveryMode()
		 * @see javax.jms.DeliveryMode
		 */

		public void setJMSDeliveryMode(int deliveryMode) 
		{
			this.jmsDeliveryMode = deliveryMode;
		}


		/**
		 * Gets an indication of whether this message is being redelivered.
		 * <p/>
		 * <P>If a client receives a message with the <CODE>JMSRedelivered</CODE>
		 * field set,
		 * it is likely, but not guaranteed, that this message was delivered
		 * earlier but that its receipt was not acknowledged
		 * at that time.
		 *
		 * @return true if this message is being redelivered
		 * @see javax.jms.Message#setJMSRedelivered(bool)
		 */

		public bool getJMSRedelivered() 
		{
			return this.jmsRedelivered;
		}


		/**
		 * Specifies whether this message is being redelivered.
		 * <p/>
		 * <P>This field is set at the time the message is delivered. This
		 * method can be used to change the value for a message that has
		 * been received.
		 *
		 * @param redelivered an indication of whether this message is being
		 *                    redelivered
		 * @see javax.jms.Message#getJMSRedelivered()
		 */

		public void setJMSRedelivered(bool redelivered) 
		{
			this.jmsRedelivered = redelivered;
		}


		/**
		 * Gets the message type identifier supplied by the client when the
		 * message was sent.
		 *
		 * @return the message type
		 * @see javax.jms.Message#setJMSType(String)
		 */

		public String getJMSType() 
		{
			return this.jmsType;
		}

		/**
		 * Sets the message type.
		 * <p/>
		 * <P>Some JMS providers use a message repository that contains the
		 * definitions of messages sent by applications. The <CODE>JMSType</CODE>
		 * header field may reference a message's definition in the provider's
		 * repository.
		 * <p/>
		 * <P>The JMS API does not define a standard message definition repository,
		 * nor does it define a naming policy for the definitions it contains.
		 * <p/>
		 * <P>Some messaging systems require that a message type definition for
		 * each application message be created and that each message specify its
		 * type. In order to work with such JMS providers, JMS clients should
		 * assign a value to <CODE>JMSType</CODE>, whether the application makes
		 * use of it or not. This ensures that the field is properly set for those
		 * providers that require it.
		 * <p/>
		 * <P>To ensure portability, JMS clients should use symbolic values for
		 * <CODE>JMSType</CODE> that can be configured at installation time to the
		 * values defined in the current provider's message repository. If string
		 * literals are used, they may not be valid type names for some JMS
		 * providers.
		 *
		 * @param type the message type
		 * @see javax.jms.Message#getJMSType()
		 */

		public void setJMSType(String type) 
		{
			this.jmsType = type;
		}


		/**
		 * Gets the message's expiration value.
		 * <p/>
		 * <P>When a message is sent, the <CODE>JMSExpiration</CODE> header field
		 * is left unassigned. After completion of the <CODE>send</CODE> or
		 * <CODE>publish</CODE> method, it holds the expiration time of the
		 * message. This is the sum of the time-to-live value specified by the
		 * client and the GMT at the time of the <CODE>send</CODE> or
		 * <CODE>publish</CODE>.
		 * <p/>
		 * <P>If the time-to-live is specified as zero, <CODE>JMSExpiration</CODE>
		 * is set to zero to indicate that the message does not expire.
		 * <p/>
		 * <P>When a message's expiration time is reached, a provider should
		 * discard it. The JMS API does not define any form of notification of
		 * message expiration.
		 * <p/>
		 * <P>Clients should not receive messages that have expired; however,
		 * the JMS API does not guarantee that this will not happen.
		 *
		 * @return the time the message expires, which is the sum of the
		 *         time-to-live value specified by the client and the GMT at the
		 *         time of the send
		 * @see javax.jms.Message#setJMSExpiration(long)
		 */

		public long getJMSExpiration() 
		{
			return this.jmsExpiration;
		}


		/**
		 * Sets the message's expiration value.
		 * <p/>
		 * <P>JMS providers set this field when a message is sent. This method
		 * can be used to change the value for a message that has been received.
		 *
		 * @param expiration the message's expiration time
		 * @see javax.jms.Message#getJMSExpiration()
		 */

		public void setJMSExpiration(long expiration) 
		{
			this.jmsExpiration = expiration;
		}

		/**
		 * Gets the message priority level.
		 * <p/>
		 * <P>The JMS API defines ten levels of priority value, with 0 as the
		 * lowest
		 * priority and 9 as the highest. In addition, clients should consider
		 * priorities 0-4 as gradations of normal priority and priorities 5-9
		 * as gradations of expedited priority.
		 * <p/>
		 * <P>The JMS API does not require that a provider strictly implement
		 * priority
		 * ordering of messages; however, it should do its best to deliver
		 * expedited messages ahead of normal messages.
		 *
		 * @return the default message priority
		 * @see javax.jms.Message#setJMSPriority(int)
		 */

		public int getJMSPriority() 
		{
			return this.jmsPriority;
		}


		/**
		 * Sets the priority level for this message.
		 * <p/>
		 * <P>JMS providers set this field when a message is sent. This method
		 * can be used to change the value for a message that has been received.
		 *
		 * @param priority the priority of this message
		 * @see javax.jms.Message#getJMSPriority()
		 */

		public void setJMSPriority(int priority) 
		{
			this.jmsPriority = priority;
		}

		/**
		 * Clears a message's properties.
		 * <p/>
		 * <P>The message's header fields and body are not cleared.
		 */

    
		/**
		 * Indicates whether a property value exists.
		 *
		 * @param name the name of the property to test
		 * @return true if the property exists
		 */

		public bool propertyExists(String name) 
		{
			return this.properties != null ? this.properties.containsKey(name) : false;
		}


		/**
		 * Returns the value of the <CODE>bool</CODE> property with the
		 * specified name.
		 *
		 * @param name the name of the <CODE>bool</CODE> property
		 * @return the <CODE>bool</CODE> property value for the specified name
		 * @           if the JMS provider fails to get the property
		 *                                value due to some internal error.
		 * @throws MessageFormatException if this type conversion is invalid.
		 */

		public bool getboolProperty(String name)  
		{
			return vanillaTobool(this.properties, name);
		}


		/**
		 * Returns the value of the <CODE>byte</CODE> property with the specified
		 * name.
		 *
		 * @param name the name of the <CODE>byte</CODE> property
		 * @return the <CODE>byte</CODE> property value for the specified name
		 * @           if the JMS provider fails to get the property
		 *                                value due to some internal error.
		 * @throws MessageFormatException if this type conversion is invalid.
		 */

		public byte getByteProperty(String name) 
		{
			return vanillaToByte(this.properties, name);
		}


		/**
		 * Returns the value of the <CODE>short</CODE> property with the specified
		 * name.
		 *
		 * @param name the name of the <CODE>short</CODE> property
		 * @return the <CODE>short</CODE> property value for the specified name
		 * @           if the JMS provider fails to get the property
		 *                                value due to some internal error.
		 * @throws MessageFormatException if this type conversion is invalid.
		 */

		public short getShortProperty(String name)  
		{
			return vanillaToShort(this.properties, name);
		}


		/**
		 * Returns the value of the <CODE>int</CODE> property with the specified
		 * name.
		 *
		 * @param name the name of the <CODE>int</CODE> property
		 * @return the <CODE>int</CODE> property value for the specified name
		 * @           if the JMS provider fails to get the property
		 *                                value due to some internal error.
		 * @throws MessageFormatException if this type conversion is invalid.
		 */

		public int getIntProperty(String name)  
		{
			return vanillaToInt(this.properties, name);
		}


		/**
		 * Returns the value of the <CODE>long</CODE> property with the specified
		 * name.
		 *
		 * @param name the name of the <CODE>long</CODE> property
		 * @return the <CODE>long</CODE> property value for the specified name
		 * @           if the JMS provider fails to get the property
		 *                                value due to some internal error.
		 * @throws MessageFormatException if this type conversion is invalid.
		 */

		public long getLongProperty(String name)  
		{
			return vanillaToLong(this.properties, name);
		}


		/**
		 * Returns the value of the <CODE>float</CODE> property with the specified
		 * name.
		 *
		 * @param name the name of the <CODE>float</CODE> property
		 * @return the <CODE>float</CODE> property value for the specified name
		 * @           if the JMS provider fails to get the property
		 *                                value due to some internal error.
		 * @throws MessageFormatException if this type conversion is invalid.
		 */

		public float getFloatProperty(String name)  
		{
			return vanillaToFloat(this.properties, name);
		}


		/**
		 * Returns the value of the <CODE>double</CODE> property with the specified
		 * name.
		 *
		 * @param name the name of the <CODE>double</CODE> property
		 * @return the <CODE>double</CODE> property value for the specified name
		 * @           if the JMS provider fails to get the property
		 *                                value due to some internal error.
		 * @throws MessageFormatException if this type conversion is invalid.
		 */

		public double getDoubleProperty(String name)  
		{
			return vanillaToDouble(this.properties, name);
		}


		/**
		 * Returns the value of the <CODE>String</CODE> property with the specified
		 * name.
		 *
		 * @param name the name of the <CODE>String</CODE> property
		 * @return the <CODE>String</CODE> property value for the specified name;
		 *         if there is no property by this name, a null value is returned
		 * @           if the JMS provider fails to get the property
		 *                                value due to some internal error.
		 * @throws MessageFormatException if this type conversion is invalid.
		 */

		public String getStringProperty(String name)  
		{
			return vanillaToString(this.properties, name);
		}


		/**
		 * Returns the value of the Java object property with the specified name.
		 * <p/>
		 * <P>This method can be used to return, in objectified format,
		 * an object that has been stored as a property in the message with the
		 * equivalent <CODE>setObjectProperty</CODE> method call, or its equivalent
		 * primitive <CODE>set<I>type</I>Property</CODE> method.
		 *
		 * @param name the name of the Java object property
		 * @return the Java object property value with the specified name, in
		 *         objectified format (for example, if the property was set as an
		 *         <CODE>int</CODE>, an <CODE>Integer</CODE> is
		 *         returned); if there is no property by this name, a null value
		 *         is returned
		 */

		public Object getObjectProperty(String name) 
		{
			return this.properties != null ? this.properties.get(name) : null;
		}


		/**
		 * Returns an <CODE>Enumeration</CODE> of all the property names.
		 * <p/>
		 * <P>Note that JMS standard header fields are not considered
		 * properties and are not returned in this enumeration.
		 *
		 * @return an enumeration of all the names of property values
		 */

		public IDictionaryEnumerator getPropertyNames() 
		{
			if (this.properties == null) 
			{
				this.properties = new Hashtable();
			}
			return this.properties.GetEnumerator();
		}

		/**
		 * Retrieve the message properties as a Hashtable
		 *
		 * @return the Hashtable representing the properties or null if not set or used
		 */

		public Hashtable getProperties() 
		{
			return this.properties;
		}

		/**
		 * Set the Message's properties from an external source
		 * No checking on correct types is done by this method
		 *
		 * @param newProperties
		 */

		public void setProperties(Hashtable newProperties) 
		{
			this.properties = newProperties;
		}


		/**
		 * Sets a <CODE>bool</CODE> property value with the specified name into
		 * the message.
		 *
		 * @param name  the name of the <CODE>bool</CODE> property
		 * @param value the <CODE>bool</CODE> property value to set
		 * @                 if the JMS provider fails to set the property
		 *                                      due to some internal error.
		 * @throws IllegalArgumentException     if the name is null or if the name is
		 *                                      an empty string.
		 * @throws MessageNotWriteableException if properties are read-only
		 */

		public void setboolProperty(String name, bool value)  
		{
			prepareProperty(name);
			this.properties.put(name, (value) ? bool.TRUE : bool.FALSE);
		}


		/**
		 * Sets a <CODE>byte</CODE> property value with the specified name into
		 * the message.
		 *
		 * @param name  the name of the <CODE>byte</CODE> property
		 * @param value the <CODE>byte</CODE> property value to set
		 * @                 if the JMS provider fails to set the property
		 *                                      due to some internal error.
		 * @throws IllegalArgumentException     if the name is null or if the name is
		 *                                      an empty string.
		 * @throws MessageNotWriteableException if properties are read-only
		 */

		public void setByteProperty(String name, byte value)  
		{
			prepareProperty(name);
			this.properties.put(name, new Byte(value));
		}


		/**
		 * Sets a <CODE>short</CODE> property value with the specified name into
		 * the message.
		 *
		 * @param name  the name of the <CODE>short</CODE> property
		 * @param value the <CODE>short</CODE> property value to set
		 * @                 if the JMS provider fails to set the property
		 *                                      due to some internal error.
		 * @throws IllegalArgumentException     if the name is null or if the name is
		 *                                      an empty string.
		 * @throws MessageNotWriteableException if properties are read-only
		 */

		public void setShortProperty(String name, short value)  
		{
			prepareProperty(name);
			this.properties.put(name, new Short(value));
		}


		/**
		 * Sets an <CODE>int</CODE> property value with the specified name into
		 * the message.
		 *
		 * @param name  the name of the <CODE>int</CODE> property
		 * @param value the <CODE>int</CODE> property value to set
		 * @                 if the JMS provider fails to set the property
		 *                                      due to some internal error.
		 * @throws IllegalArgumentException     if the name is null or if the name is
		 *                                      an empty string.
		 * @throws MessageNotWriteableException if properties are read-only
		 */

		public void setIntProperty(String name, int value)  
		{
			prepareProperty(name);
			this.properties.put(name, new Integer(value));
		}


		/**
		 * Sets a <CODE>long</CODE> property value with the specified name into
		 * the message.
		 *
		 * @param name  the name of the <CODE>long</CODE> property
		 * @param value the <CODE>long</CODE> property value to set
		 * @                 if the JMS provider fails to set the property
		 *                                      due to some internal error.
		 * @throws IllegalArgumentException     if the name is null or if the name is
		 *                                      an empty string.
		 * @throws MessageNotWriteableException if properties are read-only
		 */

		public void setLongProperty(String name, long value)  
		{
			prepareProperty(name);
			this.properties.put(name, new Long(value));
		}


		/**
		 * Sets a <CODE>float</CODE> property value with the specified name into
		 * the message.
		 *
		 * @param name  the name of the <CODE>float</CODE> property
		 * @param value the <CODE>float</CODE> property value to set
		 * @                 if the JMS provider fails to set the property
		 *                                      due to some internal error.
		 * @throws IllegalArgumentException     if the name is null or if the name is
		 *                                      an empty string.
		 * @throws MessageNotWriteableException if properties are read-only
		 */

		public void setFloatProperty(String name, float value)  
		{
			prepareProperty(name);
			this.properties.put(name, new Float(value));

		}


		/**
		 * Sets a <CODE>double</CODE> property value with the specified name into
		 * the message.
		 *
		 * @param name  the name of the <CODE>double</CODE> property
		 * @param value the <CODE>double</CODE> property value to set
		 * @                 if the JMS provider fails to set the property
		 *                                      due to some internal error.
		 * @throws IllegalArgumentException     if the name is null or if the name is
		 *                                      an empty string.
		 * @throws MessageNotWriteableException if properties are read-only
		 */

		public void setDoubleProperty(String name, double value)  
		{
			prepareProperty(name);
			this.properties.put(name, new Double(value));
		}


		/**
		 * Sets a <CODE>String</CODE> property value with the specified name into
		 * the message.
		 *
		 * @param name  the name of the <CODE>String</CODE> property
		 * @param value the <CODE>String</CODE> property value to set
		 * @                 if the JMS provider fails to set the property
		 *                                      due to some internal error.
		 * @throws IllegalArgumentException     if the name is null or if the name is
		 *                                      an empty string.
		 * @throws MessageNotWriteableException if properties are read-only
		 */

		public void setStringProperty(String name, String value)  
		{
			prepareProperty(name);
			if (value == null) 
			{
				this.properties.remove(name);
			}
			else 
			{
				this.properties.put(name, value);
			}
		}


		/**
		 * Sets a Java object property value with the specified name into the
		 * message.
		 * <p/>
		 * <P>Note that this method works only for the objectified primitive
		 * object types (<CODE>Integer</CODE>, <CODE>Double</CODE>,
		 * <CODE>Long</CODE> ...) and <CODE>String</CODE> objects.
		 *
		 * @param name  the name of the Java object property
		 * @param value the Java object property value to set
		 * @                 if the JMS provider fails to set the property
		 *                                      due to some internal error.
		 * @throws IllegalArgumentException     if the name is null or if the name is
		 *                                      an empty string.
		 * @throws MessageFormatException       if the object is invalid
		 * @throws MessageNotWriteableException if properties are read-only
		 */

		public void setObjectProperty(String name, Object value)  
		{
			prepareProperty(name);
			if (value == null) 
			{
				this.properties.remove(name);
			}
			else 
			{
				if (value is Number ||
					value is Character ||
					value is bool ||
					value is String) 
				{
					this.properties.put(name, value);
				}
				else 
				{
					throw new MessageFormatException("Cannot set property to type: " + value.getClass().getName());
				}
			}
		}


		/**
		 * Acknowledges all consumed messages of the session of this consumed
		 * message.
		 * <p/>
		 * <P>All consumed JMS messages support the <CODE>acknowledge</CODE>
		 * method for use when a client has specified that its JMS session's
		 * consumed messages are to be explicitly acknowledged.  By invoking
		 * <CODE>acknowledge</CODE> on a consumed message, a client acknowledges
		 * all messages consumed by the session that the message was delivered to.
		 * <p/>
		 * <P>Calls to <CODE>acknowledge</CODE> are ignored for both transacted
		 * sessions and sessions specified to use implicit acknowledgement modes.
		 * <p/>
		 * <P>A client may individually acknowledge each message as it is consumed,
		 * or it may choose to acknowledge messages as an application-defined group
		 * (which is done by calling acknowledge on the last received message of the group,
		 * thereby acknowledging all messages consumed by the session.)
		 * <p/>
		 * <P>Messages that have been received but not acknowledged may be
		 * redelivered.
		 *
		 * @ if the JMS provider fails to acknowledge the
		 *                      messages due to some internal error.
		 * @throws javax.jms.IllegalStateException
		 *                      if this method is called on a closed
		 *                      session.
		 * @see javax.jms.Session#CLIENT_ACKNOWLEDGE
		 */

		public void acknowledge()  
		{
			if (this.messageAcknowledge != null) 
			{
				this.messageAcknowledge.acknowledge(this);
			}
		}


		/**
		 * Clears out the message body. Clearing a message's body does not clear
		 * its header values or property entries.
		 * <p/>
		 * <P>If this message body was read-only, calling this method leaves
		 * the message body in the same state as an empty body in a newly
		 * created message.
		 *
		 * @ if the JMS provider fails to clear the message
		 *                      body due to some internal error.
		 */

		public void clearBody()  
		{
			this.readOnlyMessage = false;
			this.bodyAsBytes = null;
		}

		bool vanillaTobool(Hashtable table, String name)  
		{
			bool result = false;
			Object value = getVanillaProperty(table, name);
			if (value != null) 
			{
				if (value is bool) 
				{
					result = ((bool) value).boolValue();
				}
				else if (value is String) 
				{
					// will throw a runtime exception if cannot convert
					result = bool.valueOf((String) value).boolValue();
				}
				else 
				{
					throw new MessageFormatException(name + " not a bool type");
				}
			}
			return result;
		}

		byte vanillaToByte(Hashtable table, String name)  
		{
			byte result = 0;
			Object value = getVanillaProperty(table, name);
			if (value != null) 
			{
				if (value is Byte) 
				{
					result = ((Byte) value).byteValue();
				}
				else if (value is String) 
				{
					result = Byte.valueOf((String) value).byteValue();
				}
				else 
				{
					throw new MessageFormatException(name + " not a Byte type");
				}
			}
			else 
			{
				//object doesn't exist - so treat as a null ..
				throw new NumberFormatException("Cannot interpret null as a Byte");
			}
			return result;
		}

		short vanillaToShort(Hashtable table, String name)  
		{
			short result = 0;
			Object value = getVanillaProperty(table, name);
			if (value != null) 
			{
				if (value is Short) 
				{
					result = ((Short) value).shortValue();
				}
				else if (value is String) 
				{
					return Short.valueOf((String) value).shortValue();
				}
				else if (value is Byte) 
				{
					result = ((Byte) value).byteValue();
				}
				else 
				{
					throw new MessageFormatException(name + " not a Short type");
				}
			}
			else 
			{
				throw new NumberFormatException(name + " is null");
			}
			return result;
		}

		int vanillaToInt(Hashtable table, String name)  
		{
			int result = 0;
			Object value = getVanillaProperty(table, name);
			if (value != null) 
			{
				if (value is Integer) 
				{
					result = ((Integer) value).intValue();
				}
				else if (value is String) 
				{
					result = Integer.valueOf((String) value).intValue();
				}
				else if (value is Byte) 
				{
					result = ((Byte) value).intValue();
				}
				else if (value is Short) 
				{
					result = ((Short) value).intValue();
				}
				else 
				{
					throw new MessageFormatException(name + " not an Integer type");
				}
			}
			else 
			{
				throw new NumberFormatException(name + " is null");
			}
			return result;
		}

		long vanillaToLong(Hashtable table, String name)  
		{
			long result = 0;
			Object value = getVanillaProperty(table, name);
			if (value != null) 
			{
				if (value is Long) 
				{
					result = ((Long) value).longValue();
				}
				else if (value is String) 
				{
					// will throw a runtime exception if cannot convert
					result = Long.valueOf((String) value).longValue();
				}
				else if (value is Byte) 
				{
					result = ((Byte) value).byteValue();
				}
				else if (value is Short) 
				{
					result = ((Short) value).shortValue();
				}
				else if (value is Integer) 
				{
					result = ((Integer) value).intValue();
				}
				else 
				{
					throw new MessageFormatException(name + " not a Long type");
				}
			}
			else 
			{
				throw new NumberFormatException(name + " is null");
			}
			return result;
		}

		float vanillaToFloat(Hashtable table, String name)  
		{
			float result = 0.0f;
			Object value = getVanillaProperty(table, name);
			if (value != null) 
			{
				if (value is Float) 
				{
					result = ((Float) value).floatValue();
				}
				else if (value is String) 
				{
					result = Float.valueOf((String) value).floatValue();
				}
				else 
				{
					throw new MessageFormatException(name + " not a Float type: " + value.getClass());
				}
			}
			else 
			{
				throw new NullPointerException(name + " is null");
			}
			return result;
		}

		double vanillaToDouble(Hashtable table, String name)  
		{
			double result = 0.0d;
			Object value = getVanillaProperty(table, name);
			if (value != null) 
			{
				if (value is Double) 
				{
					result = ((Double) value).doubleValue();
				}
				else if (value is String) 
				{
					result = Double.valueOf((String) value).doubleValue();
				}
				else if (value is Float) 
				{
					result = ((Float) value).floatValue();
				}
				else 
				{
					throw new MessageFormatException(name + " not a Double type");
				}
			}
			else 
			{
				throw new NullPointerException(name + " is null");
			}
			return result;
		}
    
		Object getVanillaProperty(Hashtable table, String name) 
		{
			Object result = null;
			if (name == null) 
			{
				throw new NullPointerException("name supplied is null");
			}
			result = getReservedProperty(name);
			if (result == null && table != null) 
			{
				result = table.get(name);
			}
			return result;
		}
    
		Object getReservedProperty(String name)
		{
			Object result = null;
			if (name != null && name.equals(DELIVERY_COUNT_NAME))
			{
				result = new Integer(deliveryCount);
			}
			return result;  
		}


		String vanillaToString(Hashtable table, String name)  
		{
			String result = null;
			if (table != null) 
			{
				Object value = table.get(name);
				if (value != null) 
				{
					if (value is String || value is Number || value is bool) 
					{
						result = value.toString();
					}
					else 
					{
						throw new MessageFormatException(name + " not a String type");
					}
				}
			}
			return result;
		}

		private void prepareProperty(String name)  
		{
			if (name == null) 
			{
				throw new IllegalArgumentException("Invalid property name: cannot be null");
			}
			if (name.length() == 0) 
			{
				throw new IllegalArgumentException("Invalid property name: cannot be empty");
			}
			if (this.readOnlyProperties) 
			{
				throw new MessageNotWriteableException("Properties are read-only");
			}
			if (this.properties == null) 
			{
				this.properties = new Hashtable();
			}
		}

		/**
		 * @return Returns the entryBrokerName.
		 */
		public String getEntryBrokerName() 
		{
			return this.entryBrokerName;
		}

		/**
		 * @param newEntryBrokerName The entryBrokerName to set.
		 */
		public void setEntryBrokerName(String newEntryBrokerName) 
		{
			this.entryBrokerName = newEntryBrokerName;
		}

		/**
		 * @return Returns the entryClusterName.
		 */
		public String getEntryClusterName() 
		{
			return this.entryClusterName;
		}

		/**
		 * @param newEntryClusterName The entryClusterName to set.
		 */
		public void setEntryClusterName(String newEntryClusterName) 
		{
			this.entryClusterName = newEntryClusterName;
		}

		/**
		 * @return Returns the consumerNos.
		 */
		public int[] getConsumerNos() 
		{
			return this.consumerNos;
		}

		/**
		 * @param newConsumerNos The consumerIDs to set.
		 */
		public void setConsumerNos(int[] newConsumerNos) 
		{
			this.consumerNos = newConsumerNos;
		}

		/**
		 * @return Returns the jmsClientID.
		 */
		public String getJMSClientID() 
		{
			return this.jmsClientID;
		}

		/**
		 * @param newJmsClientID The jmsClientID to set.
		 */
		public void setJMSClientID(String newJmsClientID) 
		{
			this.jmsClientID = newJmsClientID;
		}



		/**
		 * @return Returns true if this message is part of a transaction
		 */

		public bool isPartOfTransaction() 
		{
			return this.transactionId != null;
		}

		/**
		 * @return Returns the transactionId.
		 */
		public Object getTransactionId() 
		{
			return this.transactionId;
		}

		/**
		 * @param newTransactionId The transactionId to set.
		 */
		public void setTransactionId(Object newTransactionId) 
		{
			this.transactionId = newTransactionId;
			//this.xaTransacted  = newTransactionId!=null && newTransactionId.getClass()==ActiveMQXid.class;
		}

		/**
		 * @return Returns the consumerId.
		 */
		public String getConsumerIdentifer() 
		{
			return consumerIdentifier;
		}

		/**
		 * @param consId The consumerId to set.
		 */
		public void setConsumerIdentifer(String consId) 
		{
			this.consumerIdentifier = consId;
		}

		/**
		 * @return Returns the messageConsumed.
		 */
		public bool isMessageConsumed() 
		{
			return messageConsumed;
		}

		/**
		 * @param messageConsumed The messageConsumed to set.
		 */
		public void setMessageConsumed(bool messageConsumed) 
		{
			this.messageConsumed = messageConsumed;
		}


		public void prepareMessageBody()  
		{
		}

		public void convertBodyToBytes() 
		{
			//TODO SERIALIZATION MECHANISM FUNKINESS HERE
		}

		public void buildBodyFromBytes() 
		{
			//TODO SERIALIZATION MECHANISM FUNKINESS HERE
		}

		/**
		
		public void writeBody(DataOutput dataOut) {

		}



		public void readBody(DataInput dataIn) throws IOException {

		}

		/**
		 * @return Returns the bodyAsBytes.
		 * @throws IOException
		 */
		public byte[] getBodyAsBytes() 
		{
			return this.bodyAsBytes;
		}
    
    
		/**
		 * @return true if the body is already stored as bytes
		 */
		public bool isBodyConvertedToBytes()
		{
			return bodyAsBytes != null;
		}
    

		public void setBodyAsBytes(byte[] data,int offset, int length) 
		{
			this.bodyAsBytes = new ByteArray(data);
		}
    
		/**
		 * set the body as bytes
		 * @param ba
		 */
		public void setBodyAsBytes(byte[] ba)
		{
			this.bodyAsBytes = ba;
		}

		/**
		 * write map properties to an output stream
		 *
		 * @param table
		 * @param dataOut
		 * @throws IOException
		 */

		public void writeMapProperties(Hashtable table, Object output) 
		{
			//TODO SERIALIZATION MECHANISM
		}

		/**
		 * @param dataIn
		 * @return
		 * @throws IOException
		 */
		public Hashtable readMapProperties(Object dataIn) 
		{
			//TODO serilization stuff
		}

		/**
		 * @return Returns the xaTransacted.
		 */
		public bool isXaTransacted() 
		{
			return xaTransacted;
		}

		/**
		 * @return the ActiveMQDestination
		 */
		public ActiveMQDestination getJMSActiveMQDestination() 
		{
			return jmsDestination;
		}


    
		/**
		 * Determine if the message originated in the network from the named broker
		 * @param brokerName
		 * @return true if entry point matches the brokerName
		 */
		public bool isEntryBroker(String brokerName)
		{
			bool result = entryBrokerName != null && brokerName != null && entryBrokerName.equals(brokerName);
			return result;
		}
    
		/**
		 * Determine if the message originated in the network from the named cluster
		 * @param clusterName
		 * @return true if the entry point matches the clusterName
		 */
		public bool isEntryCluster(String clusterName)
		{
			bool result = entryClusterName != null && clusterName != null && entryClusterName.equals(clusterName);
			return result;
		}
    
		/**
		 * @return Returns the transientConsumed.
		 */
		public bool isTransientConsumed() 
		{
			return transientConsumed;
		}
		/**
		 * @param transientConsumed The transientConsumed to set.
		 */
		public void setTransientConsumed(bool transientConsumed) 
		{
			this.transientConsumed = transientConsumed;
		}
    
		/**
		 * @return Returns the sequenceNumber.
		 */
		public long getSequenceNumber() 
		{
			return sequenceNumber;
		}
		/**
		 * @param sequenceNumber The sequenceNumber to set.
		 */
		public void setSequenceNumber(long sequenceNumber) 
		{
			this.sequenceNumber = sequenceNumber;
		}
		/**
		 * @return Returns the deliveryCount.
		 */
		public int getDeliveryCount() 
		{
			return deliveryCount;
		}
		/**
		 * @param deliveryCount The deliveredCount to set.
		 */
		public void setDeliveryCount(int deliveryCount) 
		{
			this.deliveryCount = deliveryCount;
		}
    
		/**
		 * Increment the delivery count
		 * @return the new value of the delivery count
		 */
		public int incrementDeliveryCount()
		{
			return ++this.deliveryCount;
		}
    
		/**
		 * @return true if the delivery mode is persistent
		 */
		public bool isPersistent()
		{
			return jmsDeliveryMode == DeliveryMode.PERSISTENT;
		}
    
		/**
		 * @return Returns the dispatchedFromDLQ.
		 */
		public bool isDispatchedFromDLQ() 
		{
			return dispatchedFromDLQ;
		}
		/**
		 * @param dispatchedFromDLQ The dispatchedFromDLQ to set.
		 */
		public void setDispatchedFromDLQ(bool dispatchedFromDLQ) 
		{
			this.dispatchedFromDLQ = dispatchedFromDLQ;
		}
    
		/**
		 * @return Returns the messsageHandle.
		 */
		public short getMesssageHandle() 
		{
			return messsageHandle;
		}
		/**
		 * @param messsageHandle The messsageHandle to set.
		 */
		public void setMesssageHandle(short messsageHandle) 
		{
			this.messsageHandle = messsageHandle;
		}
		/**
		 * @return Returns the externalMessageId.
		 */
		public bool isExternalMessageId() 
		{
			return externalMessageId;
		}
		/**
		 * @param externalMessageId The externalMessageId to set.
		 */
		public void setExternalMessageId(bool externalMessageId) 
		{
			this.externalMessageId = externalMessageId;
		}
		/**
		 * @return Returns the producerKey.
		 */
		public String getProducerKey() 
		{
			return producerKey;
		}
		/**
		 * @param producerKey The producerKey to set.
		 */
		public void setProducerKey(String producerKey) 
		{
			this.producerKey = producerKey;
		}
    
		/**
		 * reset message fragmentation infomation
		 * on this message
		 *
		 */
		public void resetMessagePart()
		{
			messagePart = false;
			partNumber = 0;
			parentMessageID = null;
		}
		/**
		 * @return Returns the messagePart.
		 */
		public bool isMessagePart() 
		{
			return messagePart;
		}
    
		/**
		 * @return true if this is the last part of a fragmented message
		 */
		public bool isLastMessagePart()
		{
			return numberOfParts -1 == partNumber;
		}
		/**
		 * @param messagePart The messagePart to set.
		 */
		public void setMessagePart(bool messagePart) 
		{
			this.messagePart = messagePart;
		}
		/**
		 * @return Returns the numberOfParts.
		 */
		public short getNumberOfParts() 
		{
			return numberOfParts;
		}
		/**
		 * @param numberOfParts The numberOfParts to set.
		 */
		public void setNumberOfParts(short numberOfParts) 
		{
			this.numberOfParts = numberOfParts;
		}
		/**
		 * @return Returns the partNumber.
		 */
		public short getPartNumber() 
		{
			return partNumber;
		}
		/**
		 * @param partNumber The partNumber to set.
		 */
		public void setPartNumber(short partNumber) 
		{
			this.partNumber = partNumber;
		}
		/**
		 * @return Returns the parentMessageId.
		 */
		public String getParentMessageID() 
		{
			return parentMessageID;
		}
		/**
		 * @param parentMessageId The parentMessageId to set.
		 */
		public void setParentMessageID(String parentMessageId) 
		{
			this.parentMessageID = parentMessageId;
		}
	}
}
