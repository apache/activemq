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
using ActiveMQ.Commands;
using System;
using ActiveMQ.OpenWire;

namespace ActiveMQ.Commands
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
		
		public virtual void BeforeMarshall(OpenWireFormat wireFormat)
		{
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

