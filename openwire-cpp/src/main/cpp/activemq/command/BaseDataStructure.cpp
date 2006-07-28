/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <string.h>
#include "activemq/command/BaseDataStructure.hpp"

#include "activemq/command/ActiveMQMessage.hpp"
#include "activemq/command/ActiveMQBytesMessage.hpp"
#include "activemq/command/ActiveMQMapMessage.hpp"
#include "activemq/command/ActiveMQObjectMessage.hpp"
#include "activemq/command/ActiveMQStreamMessage.hpp"
#include "activemq/command/ActiveMQTextMessage.hpp"
#include "activemq/command/ActiveMQQueue.hpp"
#include "activemq/command/ActiveMQTopic.hpp"
#include "activemq/command/ActiveMQTempQueue.hpp"
#include "activemq/command/ActiveMQTempTopic.hpp"
#include "activemq/command/ExceptionResponse.hpp"
#include "activemq/command/ConnectionId.hpp"
#include "activemq/command/ConsumerId.hpp"
#include "activemq/command/ProducerId.hpp"
#include "activemq/command/MessageId.hpp"
#include "activemq/command/LocalTransactionId.hpp"
#include "activemq/command/MessageAck.hpp"
#include "activemq/command/MessageDispatch.hpp"
#include "activemq/command/Response.hpp"
#include "activemq/command/ConsumerInfo.hpp"
#include "activemq/command/IntegerResponse.hpp"
#include "activemq/command/ProducerInfo.hpp"
#include "activemq/command/BrokerInfo.hpp"
#include "activemq/command/KeepAliveInfo.hpp"
#include "activemq/command/ConnectionInfo.hpp"
#include "activemq/command/RemoveInfo.hpp"
#include "activemq/command/RemoveSubscriptionInfo.hpp"
#include "activemq/command/SessionInfo.hpp"
#include "activemq/command/TransactionInfo.hpp"
#include "activemq/command/WireFormatInfo.hpp"
#include "activemq/command/BrokerId.hpp"
#include "activemq/command/ShutdownInfo.hpp"

using namespace apache::activemq::command;

/*
 * 
 */
unsigned char BaseDataStructure::getDataStructureType()
{
    return 0 ;
}

/*
 * 
 */
int BaseDataStructure::marshal(p<IMarshaller> marshaller, int mode, p<IOutputStream> ostream) throw(IOException)
{
    return 0 ;
}

/*
 * 
 */
void BaseDataStructure::unmarshal(p<IMarshaller> marshaller, int mode, p<IInputStream> istream) throw(IOException)
{
}

/*
 * 
 */
p<IDataStructure> BaseDataStructure::createObject(unsigned char type)
{
    switch( type )
    {
        case ActiveMQMessage::TYPE:
            return new ActiveMQMessage() ;
        case ActiveMQTextMessage::TYPE:
            return new ActiveMQTextMessage() ;
        case ActiveMQObjectMessage::TYPE:
            return new ActiveMQObjectMessage() ;
        case ActiveMQBytesMessage::TYPE:
            return new ActiveMQBytesMessage() ;
        case ActiveMQStreamMessage::TYPE:
            return new ActiveMQStreamMessage() ;
        case ActiveMQMapMessage::TYPE:
            return new ActiveMQMapMessage() ;
        case ActiveMQQueue::TYPE:
            return new ActiveMQQueue() ;
        case ActiveMQTopic::TYPE:
            return new ActiveMQTopic() ;
        case ActiveMQTempQueue::TYPE:
            return new ActiveMQTempQueue() ;
        case ActiveMQTempTopic::TYPE:
            return new ActiveMQTempTopic() ;
        case ExceptionResponse::TYPE:
            return new ExceptionResponse() ;
        case ConnectionId::TYPE:
            return new ConnectionId() ;
        case ConsumerId::TYPE:
            return new ConsumerId() ;
        case ProducerId::TYPE:
            return new ProducerId() ;
        case MessageId::TYPE:
            return new MessageId() ;
        case LocalTransactionId::TYPE:
            return new LocalTransactionId() ;
        case MessageAck::TYPE:
            return new MessageAck() ;
        case MessageDispatch::TYPE:
            return new MessageDispatch() ;
        case Response::TYPE:
            return new Response() ;
        case ConsumerInfo::TYPE:
            return new ConsumerInfo() ;
        case ProducerInfo::TYPE:
            return new ProducerInfo() ;
        case TransactionInfo::TYPE:
            return new TransactionInfo() ;
        case BrokerInfo::TYPE:
            return new BrokerInfo() ;
        case BrokerId::TYPE:
            return new BrokerId() ;
        case ConnectionInfo::TYPE:
            return new ConnectionInfo() ;
        case SessionInfo::TYPE:
            return new SessionInfo() ;
        case RemoveSubscriptionInfo::TYPE:
            return new RemoveSubscriptionInfo() ;
        case IntegerResponse::TYPE:
            return new IntegerResponse() ;
        case WireFormatInfo::TYPE:
            return new WireFormatInfo() ;
        case RemoveInfo::TYPE:
            return new RemoveInfo() ;
        case KeepAliveInfo::TYPE:
            return new KeepAliveInfo() ;
        case ShutdownInfo::TYPE:
            return new ShutdownInfo() ;
        default:
            return NULL ;
    }
 }

/*
 * 
 */
p<string> BaseDataStructure::getDataStructureTypeAsString(unsigned char type)
{
    p<string> packetType = new string() ;

    switch( type )
    {
        case ActiveMQMessage::TYPE:
            packetType->assign("ACTIVEMQ_MESSAGE") ;
            break ;
        case ActiveMQTextMessage::TYPE:
            packetType->assign("ACTIVEMQ_TEXT_MESSAGE") ;
            break ;
        case ActiveMQObjectMessage::TYPE:
            packetType->assign("ACTIVEMQ_OBJECT_MESSAGE") ;
            break ;
        case ActiveMQBytesMessage::TYPE:
            packetType->assign("ACTIVEMQ_BYTES_MESSAGE") ;
            break ;
        case ActiveMQStreamMessage::TYPE:
            packetType->assign("ACTIVEMQ_STREAM_MESSAGE") ;
            break ;
        case ActiveMQMapMessage::TYPE:
            packetType->assign("ACTIVEMQ_MAP_MESSAGE") ;
            break ;
        case ActiveMQQueue::TYPE:
            packetType->assign("ACTIVEMQ_QUEUE") ;
            break ;
        case ActiveMQTopic::TYPE:
            packetType->assign("ACTIVEMQ_TOPIC") ;
            break ;
        case ConnectionId::TYPE:
            packetType->assign("CONNECTION_ID") ;
            break ;
        case ConsumerId::TYPE:
            packetType->assign("CONSUMER_ID") ;
            break ;
        case ProducerId::TYPE:
            packetType->assign("PRODUCER_ID") ;
            break ;
        case MessageId::TYPE:
            packetType->assign("MESSAGE_ID") ;
            break ;
        case LocalTransactionId::TYPE:
            packetType->assign("LOCAL_TRANSACTION_ID") ;
            break ;
        case MessageAck::TYPE:
            packetType->assign("ACTIVEMQ_MSG_ACK") ;
            break ;
        case MessageDispatch::TYPE:
            packetType->assign("ACTIVEMQ_MSG_DISPATCH") ;
            break ;
        case Response::TYPE:
            packetType->assign("RESPONSE") ;
            break ;
        case ExceptionResponse::TYPE:
            packetType->assign("EXCEPTION_RESPONSE") ;
            break ;
        case ConsumerInfo::TYPE:
            packetType->assign("CONSUMER_INFO") ;
            break ;
        case ProducerInfo::TYPE:
            packetType->assign("PRODUCER_INFO") ;
            break;
        case TransactionInfo::TYPE:
            packetType->assign("TRANSACTION_INFO") ;
            break ;
        case BrokerInfo::TYPE:
            packetType->assign("BROKER_INFO") ;
            break ;
        case ConnectionInfo::TYPE:
            packetType->assign("CONNECTION_INFO") ;
            break ;
        case SessionInfo::TYPE:
            packetType->assign("SESSION_INFO") ;
            break ;
        case RemoveSubscriptionInfo::TYPE:
            packetType->assign("DURABLE_UNSUBSCRIBE") ;
            break ;
        case IntegerResponse::TYPE:
            packetType->assign("INT_RESPONSE_RECEIPT_INFO") ;
            break ;
        case WireFormatInfo::TYPE:
            packetType->assign("WIRE_FORMAT_INFO") ;
            break ;
        case RemoveInfo::TYPE:
            packetType->assign("REMOVE_INFO") ;
            break ;
        case KeepAliveInfo::TYPE:
            packetType->assign("KEEP_ALIVE") ;
            break ;
        case ShutdownInfo::TYPE:
            packetType->assign("SHUTDOWN") ;
            break ;
        default:
            packetType->assign("UNDEFINED") ;
            break ;
    }
    return packetType ;
}
