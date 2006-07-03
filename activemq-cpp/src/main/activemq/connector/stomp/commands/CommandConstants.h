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
#ifndef ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_COMMANDCONSTANTS_H_
#define ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_COMMANDCONSTANTS_H_

#include <cms/Destination.h>
#include <activemq/exceptions/IllegalArgumentException.h>

#include <string>
#include <map>

namespace activemq{
namespace connector{
namespace stomp{
namespace commands{
    
    class CommandConstants{    
    public:
    
        enum CommandId{
            CONNECT,
            CONNECTED,
            DISCONNECT,
            SUBSCRIBE,
            UNSUBSCRIBE,
            MESSAGE,
            SEND,
            BEGIN,
            COMMIT,
            ABORT,
            ACK,
            ERROR_CMD,
            RECEIPT,
            NUM_COMMANDS
        };
        
        enum StompHeader{
            HEADER_DESTINATION,
            HEADER_TRANSACTIONID,
            HEADER_CONTENTLENGTH,
            HEADER_SESSIONID,
            HEADER_RECEIPT_REQUIRED,
            HEADER_RECEIPTID,
            HEADER_MESSAGEID,
            HEADER_ACK,
            HEADER_LOGIN,
            HEADER_PASSWORD,
            HEADER_CLIENT_ID,
            HEADER_MESSAGE,
            HEADER_CORRELATIONID,
            HEADER_REQUESTID,
            HEADER_RESPONSEID,
            HEADER_EXPIRES,
            HEADER_PERSISTANT,
            HEADER_REPLYTO,
            HEADER_TYPE,
            HEADER_AMQMSGTYPE,
            HEADER_JMSXGROUPID,
            HEADER_JMSXGROUPSEQNO,
            HEADER_DISPATCH_ASYNC,
            HEADER_EXCLUSIVE,
            HEADER_MAXPENDINGMSGLIMIT,
            HEADER_NOLOCAL,
            HEADER_PREFETCHSIZE,
            HEADER_PRIORITY,
            HEADER_RETROACTIVE,
            HEADER_SUBSCRIPTIONNAME,
            HEADER_TIMESTAMP,
            HEADER_REDELIVERED,
            HEADER_REDELIVERYCOUNT,
            HEADER_SELECTOR,
            HEADER_ID,
            HEADER_SUBSCRIPTION,
            NUM_STOMP_HEADERS
        }; 
        
        enum AckMode{
            ACK_CLIENT,
            ACK_AUTO,
            NUM_ACK_MODES
        };
        
        enum MessageType
        {
            TEXT,
            BYTES,
            NUM_MSG_TYPES
        };
        
        static const char* queuePrefix;
        static const char* topicPrefix;
        
        static const std::string& toString( const CommandId cmd ){
            return StaticInitializer::commands[cmd];
        }
        
        static CommandId toCommandId( const std::string& cmd ){     
            std::map<std::string, CommandId>::iterator iter = 
                StaticInitializer::commandMap.find(cmd);

            if( iter == StaticInitializer::commandMap.end() ){
                return NUM_COMMANDS;
            }
                    
            return iter->second;
        }             
        
        static std::string toString( const StompHeader header ){
            return StaticInitializer::stompHeaders[header];
        }
        
        static StompHeader toStompHeader( const std::string& header ){  
            
            std::map<std::string, StompHeader>::iterator iter = 
                StaticInitializer::stompHeaderMap.find(header);

            if( iter == StaticInitializer::stompHeaderMap.end() ){
                return NUM_STOMP_HEADERS;
            }
                    
            return iter->second;            
        }        
        
        static std::string toString( const AckMode mode ){
            return StaticInitializer::ackModes[mode];
        }
        
        static AckMode toAckMode( const std::string& mode ){
            std::map<std::string, AckMode>::iterator iter = 
                StaticInitializer::ackModeMap.find(mode);

            if( iter == StaticInitializer::ackModeMap.end() ){
                return NUM_ACK_MODES;
            }
                    
            return iter->second;
        }  
         
        static std::string toString( const MessageType type ){
            return StaticInitializer::msgTypes[type];
        }
        
        static MessageType toMessageType( const std::string& type ){
            std::map<std::string, MessageType>::iterator iter = 
                StaticInitializer::msgTypeMap.find(type);

            if( iter == StaticInitializer::msgTypeMap.end() ){
                return NUM_MSG_TYPES;
            }
                    
            return iter->second;
        }  

        static cms::Destination* toDestination( const std::string& dest )
            throw ( exceptions::IllegalArgumentException );

        class StaticInitializer{
        public:
            StaticInitializer();
            virtual ~StaticInitializer(){}
            
            static std::string stompHeaders[NUM_STOMP_HEADERS];
            static std::string commands[NUM_COMMANDS];
            static std::string ackModes[NUM_ACK_MODES];
            static std::string msgTypes[NUM_MSG_TYPES];
            static std::map<std::string, StompHeader> stompHeaderMap;
            static std::map<std::string, CommandId> commandMap;
            static std::map<std::string, AckMode> ackModeMap;
            static std::map<std::string, MessageType> msgTypeMap;
        };
        
    private:
    
        static StaticInitializer staticInits;        
    };
    
}}}}

#endif /*ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_COMMANDCONSTANTS_H_*/
