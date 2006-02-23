using System;
using OpenWire.Client;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client.Core
{
    /// <summary>
    /// Summary description for AbstractCommand.
    /// </summary>
    public abstract class AbstractCommand : Command
    {
        private short commandId;
        private bool responseRequired;
        
        
        protected AbstractCommand()
        {
        }
        
        public virtual byte GetDataStructureType()
        {
            return 0;
        }
        
        public virtual bool IsMarshallAware()
        {
            return false;
        }
        
        
        
        // Properties
        
        public short CommandId
        {
            get { return commandId; }
            set { this.commandId = value; }
        }
        
        public bool ResponseRequired
        {
            get { return responseRequired; }
            set { this.responseRequired = value; }
        }
        
        public static String GetDataStructureTypeAsString(int type)
        {
            String packetTypeStr = "";
            switch (type)
            {
                case ActiveMQMessage.ID_ActiveMQMessage :
                    packetTypeStr = "ACTIVEMQ_MESSAGE";
                    break;
                case ActiveMQTextMessage.ID_ActiveMQTextMessage :
                    packetTypeStr = "ACTIVEMQ_TEXT_MESSAGE";
                    break;
                case ActiveMQObjectMessage.ID_ActiveMQObjectMessage:
                    packetTypeStr = "ACTIVEMQ_OBJECT_MESSAGE";
                    break;
                case ActiveMQBytesMessage.ID_ActiveMQBytesMessage :
                    packetTypeStr = "ACTIVEMQ_BYTES_MESSAGE";
                    break;
                case ActiveMQStreamMessage.ID_ActiveMQStreamMessage :
                    packetTypeStr = "ACTIVEMQ_STREAM_MESSAGE";
                    break;
                case ActiveMQMapMessage.ID_ActiveMQMapMessage :
                    packetTypeStr = "ACTIVEMQ_MAP_MESSAGE";
                    break;
                case MessageAck.ID_MessageAck :
                    packetTypeStr = "ACTIVEMQ_MSG_ACK";
                    break;
                case Response.ID_Response :
                    packetTypeStr = "RESPONSE";
                    break;
                case ConsumerInfo.ID_ConsumerInfo :
                    packetTypeStr = "CONSUMER_INFO";
                    break;
                case ProducerInfo.ID_ProducerInfo :
                    packetTypeStr = "PRODUCER_INFO";
                    break;
                case TransactionInfo.ID_TransactionInfo :
                    packetTypeStr = "TRANSACTION_INFO";
                    break;
                case BrokerInfo.ID_BrokerInfo :
                    packetTypeStr = "BROKER_INFO";
                    break;
                case ConnectionInfo.ID_ConnectionInfo :
                    packetTypeStr = "CONNECTION_INFO";
                    break;
                case SessionInfo.ID_SessionInfo :
                    packetTypeStr = "SESSION_INFO";
                    break;
                case RemoveSubscriptionInfo.ID_RemoveSubscriptionInfo :
                    packetTypeStr = "DURABLE_UNSUBSCRIBE";
                    break;
                case IntegerResponse.ID_IntegerResponse :
                    packetTypeStr = "INT_RESPONSE_RECEIPT_INFO";
                    break;
                case WireFormatInfo.ID_WireFormatInfo :
                    packetTypeStr = "WIRE_FORMAT_INFO";
                    break;
                case RemoveInfo.ID_RemoveInfo :
                    packetTypeStr = "REMOVE_INFO";
                    break;
                case KeepAliveInfo.ID_KeepAliveInfo :
                    packetTypeStr = "KEEP_ALIVE";
                    break;
            }
            return packetTypeStr;
        }
        
        // Helper methods
        public int HashCode(object value)
        {
            if (value != null)
            {
                return value.GetHashCode();
            }
            else
            {
                return -1;
            }
        }
    }
}
