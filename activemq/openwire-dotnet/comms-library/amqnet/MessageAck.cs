using System;

namespace ActiveMQ
{
	/// <summary>
	/// Summary description for MessageAck.
	/// </summary>
	public class MessageAck : AbstractPacket {

		public const int MESSAGE_READ_INDEX = 2;
		public const int XA_TRANS_INDEX = 3;
		public const int PERSISTENT_INDEX = 4;
		public const int EXPIRED_INDEX = 5;
		public const int TRANSACTION_ID_INDEX = 6;
		public const int EXTERNAL_MESSAGE_ID_INDEX = 7;
		public const int CACHED_VALUES_INDEX = 8;
		public const int LONG_SEQUENCE_INDEX = 9;
    
		private String consumerId;
		private String messageID;
		private ActiveMQDestination  destination;
		private Object transactionId;
		private bool messageRead;
		private bool xaTransacted;
		private bool persistent;
		private bool expired;
		private short sessionId;
		private long sequenceNumber;
		private String producerKey;
		private bool externalMessageId;
		


		/**
		 * Return the type of Packet
		 *
		 * @return integer representation of the type of Packet
		 */

		public new int getPacketType() 
		{
			return ACTIVEMQ_MSG_ACK;
		}

		/**
		 * @return pretty print of this Packet
		 */
		public new String ToString() 
		{
			return super.toString() + " MessageAck{ " +
				"consumerId = '" + consumerId + "' " +
				", messageID = '" + messageID + "' " +
				", destination = " + destination +
				", transactionId = '" + transactionId + "' " +
				", messageRead = " + messageRead +
				", xaTransacted = " + xaTransacted +
				", persistent = " + persistent +
				", expired = " + expired +
				", messageIdentity = " + messageIdentity +
				" }";
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
		 * @return Returns true if this message is part of a transaction
		 */

		public bool isPartOfTransaction() 
		{
			return this.transactionId != null;
		}


		/**
		 * @return the messageId
		 */
    
		public String getMessageID() 
		{
			if (messageID == null && producerKey != null)
			{
				messageID = producerKey + sequenceNumber;
			}
			return messageID;
		}

		/**
		 * @param messageID The messageID to set.
		 */
		public void setMessageID(String messageID) 
		{
			this.messageID = messageID;
		}

		/**
		 * @return Returns the messageRead.
		 */
		public bool isMessageRead() 
		{
			return messageRead;
		}

		/**
		 * @param messageRead The messageRead to set.
		 */
		public void setMessageRead(bool messageRead) 
		{
			this.messageRead = messageRead;
		}

		/**
		 * @return Returns the consumerId.
		 */
		public String getConsumerId() 
		{
			return consumerId;
		}

		/**
		 * @param consumerId The consumerId to set.
		 */
		public void setConsumerId(String consumerId) 
		{
			this.consumerId = consumerId;
		}

		/**
		 * @return Returns the xaTransacted.
		 */
		public bool isXaTransacted() 
		{
			return xaTransacted;
		}


		/**
		 * @return Returns the destination.
		 */
		public ActiveMQDestination getDestination() 
		{
			return destination;
		}
		/**
		 * @param destination The destination to set.
		 */
		public void setDestination(ActiveMQDestination destination) 
		{
			this.destination = destination;
		}
		/**
		 * @return Returns the persistent.
		 */
		public bool isPersistent() 
		{
			return persistent;
		}
		/**
		 * @param persistent The persistent to set.
		 */
		public void setPersistent(bool persistent) 
		{
			this.persistent = persistent;
		}
    
		/**
		 * @return true the delivered message was to a non-persistent destination
		 */
		public bool isTemporary()
		{
			return persistent == false || (destination != null && destination.isTemporary());
		}
    
		/**
		 * @return Returns the expired.
		 */
		public bool isExpired() 
		{
			return expired;
		}
		/**
		 * @param expired The expired to set.
		 */
		public void setExpired(bool expired) 
		{
			this.expired = expired;
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
		 * @return Returns the messageSequence.
		 */
		public long getSequenceNumber() 
		{
			return sequenceNumber;
		}
		/**
		 * @param messageSequence The messageSequence to set.
		 */
		public void setSequenceNumber(long messageSequence) 
		{
			this.sequenceNumber = messageSequence;
		}
		/**
		 * @return Returns the sessionId.
		 */
		public short getSessionId() 
		{
			return sessionId;
		}
		/**
		 * @param sessionId The sessionId to set.
		 */
		public void setSessionId(short sessionId) 
		{
			this.sessionId = sessionId;
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
	}

}
