using System;

namespace OpenWire.Core
{
	/// <summary>
	/// Summary description for AbstractCommand.
	/// </summary>
	public abstract class AbstractCommand : Command {
		
		//public const int 	NON_PERSISTENT = 	1;
		//public const int 	PERSISTENT 	= 2;
    
		 /**
		  * Message flag indexes (used for writing/reading to/from a Stream
		  */
		public const int RECEIPT_REQUIRED_INDEX = 0;
		public const int BROKERS_VISITED_INDEX =1;
		private short id = 0;
		private  bool receiptRequired;

		protected AbstractCommand()
		{
			
		}
		
         public virtual int GetCommandType()
         {
         	    return 0;
 	    }

		public short GetId() 
		{
			return this.id;
		}

		public virtual void SetId(short newId) 
		{
			this.id = newId;
		}

		public virtual bool IsReceiptRequired() 
		{
			return this.receiptRequired;
		}

		
		public virtual bool IsReceipt() 
		{
			return false;
		}

		public void SetReceiptRequired(bool value) 
		{
			this.receiptRequired = value;
		}

		public virtual bool IsJMSMessage() 
		{
			return false;
		}

		public override int GetHashCode() 
		{
			return this.id;
		}

		public override String ToString() 
		{
			return GetCommandTypeAsString(GetCommandType()) + ": id = " + GetId();
		}


		public static String GetCommandTypeAsString(int type) 
		{
			String packetTypeStr = "";
			switch (type) 
			{
				case CommandConstants.ACTIVEMQ_MESSAGE:
					packetTypeStr = "ACTIVEMQ_MESSAGE";
					break;
				case CommandConstants.ACTIVEMQ_TEXT_MESSAGE:
					packetTypeStr = "ACTIVEMQ_TEXT_MESSAGE";
					break;
				case CommandConstants.ACTIVEMQ_OBJECT_MESSAGE:
					packetTypeStr = "ACTIVEMQ_OBJECT_MESSAGE";
					break;
				case CommandConstants.ACTIVEMQ_BYTES_MESSAGE:
					packetTypeStr = "ACTIVEMQ_BYTES_MESSAGE";
					break;
				case CommandConstants.ACTIVEMQ_STREAM_MESSAGE:
					packetTypeStr = "ACTIVEMQ_STREAM_MESSAGE";
					break;
				case CommandConstants.ACTIVEMQ_MAP_MESSAGE:
					packetTypeStr = "ACTIVEMQ_MAP_MESSAGE";
					break;
				case CommandConstants.ACTIVEMQ_MSG_ACK:
					packetTypeStr = "ACTIVEMQ_MSG_ACK";
					break;
				case CommandConstants.RECEIPT_INFO:
					packetTypeStr = "RECEIPT_INFO";
					break;
				case CommandConstants.CONSUMER_INFO:
					packetTypeStr = "CONSUMER_INFO";
					break;
				case CommandConstants.PRODUCER_INFO:
					packetTypeStr = "PRODUCER_INFO";
					break;
				case CommandConstants.TRANSACTION_INFO:
					packetTypeStr = "TRANSACTION_INFO";
					break;
				case CommandConstants.XA_TRANSACTION_INFO:
					packetTypeStr = "XA_TRANSACTION_INFO";
					break;
				case CommandConstants.ACTIVEMQ_BROKER_INFO:
					packetTypeStr = "ACTIVEMQ_BROKER_INFO";
					break;
				case CommandConstants.ACTIVEMQ_CONNECTION_INFO:
					packetTypeStr = "ACTIVEMQ_CONNECTION_INFO";
					break;
				case CommandConstants.SESSION_INFO:
					packetTypeStr = "SESSION_INFO";
					break;
				case CommandConstants.DURABLE_UNSUBSCRIBE:
					packetTypeStr = "DURABLE_UNSUBSCRIBE";
					break;
				case CommandConstants.RESPONSE_RECEIPT_INFO:
					packetTypeStr = "RESPONSE_RECEIPT_INFO";
					break;
				case CommandConstants.INT_RESPONSE_RECEIPT_INFO:
					packetTypeStr = "INT_RESPONSE_RECEIPT_INFO";
					break;
				case CommandConstants.CAPACITY_INFO:
					packetTypeStr = "CAPACITY_INFO";
					break;
				case CommandConstants.CAPACITY_INFO_REQUEST:
					packetTypeStr = "CAPACITY_INFO_REQUEST";
					break;
				case CommandConstants.WIRE_FORMAT_INFO:
					packetTypeStr = "WIRE_FORMAT_INFO";
					break;
				case CommandConstants.KEEP_ALIVE:
					packetTypeStr = "KEEP_ALIVE";
					break;
				case CommandConstants.CACHED_VALUE_COMMAND:
					packetTypeStr = "CachedValue";
					break;
				default :
					packetTypeStr = "UNKNOWN PACKET TYPE: " + type;
					break;
			}
			return packetTypeStr;
		}
	}
}

