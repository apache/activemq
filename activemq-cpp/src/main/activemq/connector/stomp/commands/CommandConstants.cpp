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
 
#include "CommandConstants.h"
#include <stdio.h>

#include <activemq/connector/stomp/StompTopic.h>
#include <activemq/connector/stomp/StompQueue.h>

using namespace std;
using namespace activemq;
using namespace activemq::exceptions;
using namespace activemq::connector::stomp;
using namespace activemq::connector::stomp::commands;

////////////////////////////////////////////////////////////////////////////////
const char* CommandConstants::queuePrefix = "/queue/";
const char* CommandConstants::topicPrefix = "/topic/";

////////////////////////////////////////////////////////////////////////////////
string CommandConstants::StaticInitializer::stompHeaders[NUM_STOMP_HEADERS];
string CommandConstants::StaticInitializer::commands[NUM_COMMANDS];
string CommandConstants::StaticInitializer::ackModes[NUM_ACK_MODES];
string CommandConstants::StaticInitializer::msgTypes[NUM_MSG_TYPES];
map<std::string, CommandConstants::StompHeader> CommandConstants::StaticInitializer::stompHeaderMap;
map<std::string, CommandConstants::CommandId> CommandConstants::StaticInitializer::commandMap;
map<std::string, CommandConstants::AckMode> CommandConstants::StaticInitializer::ackModeMap;
map<std::string, CommandConstants::MessageType> CommandConstants::StaticInitializer::msgTypeMap;
CommandConstants::StaticInitializer CommandConstants::staticInits;

////////////////////////////////////////////////////////////////////////////////
CommandConstants::StaticInitializer::StaticInitializer(){
    
    stompHeaders[HEADER_DESTINATION] = "destination";
    stompHeaders[HEADER_TRANSACTIONID] = "transaction";
    stompHeaders[HEADER_CONTENTLENGTH] = "content-length";
    stompHeaders[HEADER_SESSIONID] = "session";
    stompHeaders[HEADER_RECEIPTID] = "receipt-id";
    stompHeaders[HEADER_RECEIPT_REQUIRED] = "receipt";
    stompHeaders[HEADER_MESSAGEID] = "message-id";
    stompHeaders[HEADER_ACK] = "ack";
    stompHeaders[HEADER_LOGIN] = "login";
    stompHeaders[HEADER_PASSWORD] = "passcode";
    stompHeaders[HEADER_CLIENT_ID] = "client-id";
    stompHeaders[HEADER_MESSAGE] = "message";
    stompHeaders[HEADER_CORRELATIONID] = "correlation-id";
    stompHeaders[HEADER_REQUESTID] = "request-id";
    stompHeaders[HEADER_RESPONSEID] = "response-id";
    stompHeaders[HEADER_EXPIRES] = "expires";
    stompHeaders[HEADER_PERSISTANT] = "persistent";
    stompHeaders[HEADER_PRIORITY] = "priority";
    stompHeaders[HEADER_REPLYTO] = "reply-to";
    stompHeaders[HEADER_TYPE] = "type";
    stompHeaders[HEADER_AMQMSGTYPE] = "amq-msg-type";
    stompHeaders[HEADER_JMSXGROUPID] = "JMSXGroupID";
    stompHeaders[HEADER_JMSXGROUPSEQNO] = "JMSXGroupSeq";
    stompHeaders[HEADER_SELECTOR] = "selector";
    stompHeaders[HEADER_DISPATCH_ASYNC] = "activemq.dispatchAsync";
    stompHeaders[HEADER_EXCLUSIVE] = "activemq.exclusive";
    stompHeaders[HEADER_MAXPENDINGMSGLIMIT] = "activemq.maximumPendingMessageLimit";
    stompHeaders[HEADER_NOLOCAL] = "activemq.noLocal";
    stompHeaders[HEADER_PREFETCHSIZE] = "activemq.prefetchSize";
    stompHeaders[HEADER_PRIORITY] = "activemq.priority";
    stompHeaders[HEADER_RETROACTIVE] = "activemq.retroactive";
    stompHeaders[HEADER_SUBSCRIPTIONNAME] = "activemq.subscriptionName";
    stompHeaders[HEADER_TIMESTAMP] = "timestamp";
    stompHeaders[HEADER_REDELIVERED] = "redelivered";
    stompHeaders[HEADER_REDELIVERYCOUNT] = "redelivery_count";
    stompHeaders[HEADER_SELECTOR] = "selector";
    stompHeaders[HEADER_ID] = "id";
    stompHeaders[HEADER_SUBSCRIPTION] = "subscription";
    commands[CONNECT] = "CONNECT";
    commands[CONNECTED] = "CONNECTED";
    commands[DISCONNECT] = "DISCONNECT";
    commands[SUBSCRIBE] = "SUBSCRIBE";
    commands[UNSUBSCRIBE] = "UNSUBSCRIBE";
    commands[MESSAGE] = "MESSAGE";
    commands[SEND] = "SEND";
    commands[BEGIN] = "BEGIN";
    commands[COMMIT] = "COMMIT";
    commands[ABORT] = "ABORT";
    commands[ACK] = "ACK";
    commands[ERROR_CMD] = "ERROR";
    commands[RECEIPT] = "RECEIPT";
    ackModes[ACK_CLIENT] = "client";
    ackModes[ACK_AUTO] = "auto";
    msgTypes[TEXT] = "text";
    msgTypes[BYTES] = "bytes";

    for( int ix=0; ix<NUM_STOMP_HEADERS; ++ix ){
        stompHeaderMap[stompHeaders[ix]] = (StompHeader)ix;
    }
    
    for( int ix=0; ix<NUM_COMMANDS; ++ix ){
        commandMap[commands[ix]] = (CommandId)ix;
    }
    
    for( int ix=0; ix<NUM_ACK_MODES; ++ix ){
        ackModeMap[ackModes[ix]] = (AckMode)ix;
    }

    for( int ix=0; ix<NUM_MSG_TYPES; ++ix ){
        msgTypeMap[msgTypes[ix]] = (MessageType)ix;
    }
}

////////////////////////////////////////////////////////////////////////////////
cms::Destination* CommandConstants::toDestination( const std::string& dest )
    throw ( exceptions::IllegalArgumentException )
{
    int qpos = dest.find(queuePrefix);
    int tpos = dest.find(topicPrefix);
    
    if(tpos == 0)
    {
        return new StompTopic(dest.substr(strlen(topicPrefix)));
    }
    else if(qpos == 0)
    {
        return new StompQueue(dest.substr(strlen(queuePrefix)));
    }
    else
    {
        throw IllegalArgumentException(
            __FILE__, __LINE__,
            "CommandConstants::toDestionation - Not a valid Stomp Dest");
    }
}  


