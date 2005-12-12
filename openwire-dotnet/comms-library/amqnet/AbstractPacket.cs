using System;
using ActiveMQ;

namespace ActiveMQ
{
	/// <summary>
	/// Summary description for AbstractPacket.
	/// </summary>
	public abstract class AbstractPacket {
		
		public const int 	NON_PERSISTENT = 	1;
		public const int 	PERSISTENT 	= 2;
    
											 /**
											  * Message flag indexes (used for writing/reading to/from a Stream
											  */
		public const int RECEIPT_REQUIRED_INDEX = 0;
		public const int BROKERS_VISITED_INDEX =1;
		private short id = 0;
		protected byte[] data = new byte[100];
		private  bool receiptRequired;

		protected AbstractPacket()
		{
			
		}

		public short getId() 
		{
			return this.id;
		}

		public virtual void setId(short newId) 
		{
			this.id = newId;
		}

		public virtual bool isReceiptRequired() 
		{
			return this.receiptRequired;
		}

		
		public virtual bool isReceipt() 
		{
			return false;
		}

		public void setReceiptRequired(bool value) 
		{
			this.receiptRequired = value;
		}

		public virtual bool isJMSMessage() 
		{
			return false;
		}

		public int hashCode() 
		{
			return this.id;
		}

		public virtual short getPacketType() 
		{
			return id;
		}

		public String toString() 
		{
			return getPacketTypeAsString(getPacketType()) + ": id = " + getId();
		}


		public static String getPacketTypeAsString(int type) 
		{
			String packetTypeStr = "";
			switch (type) 
			{
				case PacketConstants.ACTIVEMQ_MESSAGE:
					packetTypeStr = "ACTIVEMQ_MESSAGE";
					break;
				case PacketConstants.ACTIVEMQ_TEXT_MESSAGE:
					packetTypeStr = "ACTIVEMQ_TEXT_MESSAGE";
					break;
				case PacketConstants.ACTIVEMQ_OBJECT_MESSAGE:
					packetTypeStr = "ACTIVEMQ_OBJECT_MESSAGE";
					break;
				case PacketConstants.ACTIVEMQ_BYTES_MESSAGE:
					packetTypeStr = "ACTIVEMQ_BYTES_MESSAGE";
					break;
				case PacketConstants.ACTIVEMQ_STREAM_MESSAGE:
					packetTypeStr = "ACTIVEMQ_STREAM_MESSAGE";
					break;
				case PacketConstants.ACTIVEMQ_MAP_MESSAGE:
					packetTypeStr = "ACTIVEMQ_MAP_MESSAGE";
					break;
				case PacketConstants.ACTIVEMQ_MSG_ACK:
					packetTypeStr = "ACTIVEMQ_MSG_ACK";
					break;
				case PacketConstants.RECEIPT_INFO:
					packetTypeStr = "RECEIPT_INFO";
					break;
				case PacketConstants.CONSUMER_INFO:
					packetTypeStr = "CONSUMER_INFO";
					break;
				case PacketConstants.PRODUCER_INFO:
					packetTypeStr = "PRODUCER_INFO";
					break;
				case PacketConstants.TRANSACTION_INFO:
					packetTypeStr = "TRANSACTION_INFO";
					break;
				case PacketConstants.XA_TRANSACTION_INFO:
					packetTypeStr = "XA_TRANSACTION_INFO";
					break;
				case PacketConstants.ACTIVEMQ_BROKER_INFO:
					packetTypeStr = "ACTIVEMQ_BROKER_INFO";
					break;
				case PacketConstants.ACTIVEMQ_CONNECTION_INFO:
					packetTypeStr = "ACTIVEMQ_CONNECTION_INFO";
					break;
				case PacketConstants.SESSION_INFO:
					packetTypeStr = "SESSION_INFO";
					break;
				case PacketConstants.DURABLE_UNSUBSCRIBE:
					packetTypeStr = "DURABLE_UNSUBSCRIBE";
					break;
				case PacketConstants.RESPONSE_RECEIPT_INFO:
					packetTypeStr = "RESPONSE_RECEIPT_INFO";
					break;
				case PacketConstants.INT_RESPONSE_RECEIPT_INFO:
					packetTypeStr = "INT_RESPONSE_RECEIPT_INFO";
					break;
				case PacketConstants.CAPACITY_INFO:
					packetTypeStr = "CAPACITY_INFO";
					break;
				case PacketConstants.CAPACITY_INFO_REQUEST:
					packetTypeStr = "CAPACITY_INFO_REQUEST";
					break;
				case PacketConstants.WIRE_FORMAT_INFO:
					packetTypeStr = "WIRE_FORMAT_INFO";
					break;
				case PacketConstants.KEEP_ALIVE:
					packetTypeStr = "KEEP_ALIVE";
					break;
				case PacketConstants.CACHED_VALUE_COMMAND:
					packetTypeStr = "CachedValue";
					break;
				default :
					packetTypeStr = "UNKNOWN PACKET TYPE: " + type;
					break;
			}
			return packetTypeStr;
		}

		protected virtual bool equals(Object left, Object right) 
		{	
			return left == right || (left != null && left.Equals(right));
		}

		public byte[] getData() 
		{
			return data;
		}

		public void setBitArray(byte[] data) 
		{
			this.data = data;
		}
	}
	}

