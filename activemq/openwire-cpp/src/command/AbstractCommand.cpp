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
#include <string.h>
#include "command/AbstractCommand.hpp"
#include "command/ActiveMQMessage.hpp"
#include "command/ActiveMQTextMessage.hpp"
#include "command/MessageAck.hpp"
#include "command/Response.hpp"
#include "command/ConsumerInfo.hpp"
#include "command/ProducerInfo.hpp"
#include "command/BrokerInfo.hpp"
#include "command/ConnectionInfo.hpp"
#include "command/RemoveInfo.hpp"
#include "command/SessionInfo.hpp"

using namespace apache::activemq::client::command;

/*
 * 
 */
AbstractCommand::AbstractCommand()
{
}

AbstractCommand::~AbstractCommand()
{
}

int AbstractCommand::getCommandType()
{
    return 0 ; 
}

p<string> AbstractCommand::getCommandTypeAsString(int type)
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
//        case ActiveMQObjectMessage::TYPE:
//            packetType->assign("ACTIVEMQ_OBJECT_MESSAGE") ;
//            break ;
//        case ActiveMQBytesMessage::TYPE:
//            packetType->assign("ACTIVEMQ_BYTES_MESSAGE") ;
//            break ;
//        case ActiveMQStreamMessage::TYPE:
//            packetType->assign("ACTIVEMQ_STREAM_MESSAGE") ;
//            break ;
//        case ActiveMQMapMessage::TYPE:
//            packetType->assign("ACTIVEMQ_MAP_MESSAGE") ;
//            break ;
        case MessageAck::TYPE:
            packetType->assign("ACTIVEMQ_MSG_ACK") ;
            break ;
        case Response::TYPE:
            packetType->assign("RESPONSE") ;
            break ;
        case ConsumerInfo::TYPE:
            packetType->assign("CONSUMER_INFO") ;
            break ;
        case ProducerInfo::TYPE:
            packetType->assign("PRODUCER_INFO") ;
            break;
//        case TransactionInfo::TYPE:
//            packetType->assign("TRANSACTION_INFO") ;
//            break ;
        case BrokerInfo::TYPE:
            packetType->assign("BROKER_INFO") ;
            break ;
        case ConnectionInfo::TYPE:
            packetType->assign("CONNECTION_INFO") ;
            break ;
        case SessionInfo::TYPE:
            packetType->assign("SESSION_INFO") ;
            break ;
//        case RemoveSubscriptionInfo::TYPE:
//            packetType->assign("DURABLE_UNSUBSCRIBE") ;
//            break ;
//        case IntegerResponse::TYPE:
//            packetType->assign("INT_RESPONSE_RECEIPT_INFO") ;
//            break ;
//        case WireFormatInfo::TYPE:
//            packetType->assign("WIRE_FORMAT_INFO") ;
//            break ;
        case RemoveInfo::TYPE:
            packetType->assign("REMOVE_INFO") ;
            break ;
//        case KeepAliveInfo::TYPE:
//            packetType->assign("KEEP_ALIVE") ;
//            break ;
        default:
            packetType->assign("UNDEFINED") ;
            break ;
    }
    return packetType ;
}
