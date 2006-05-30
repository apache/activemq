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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_STOMPTEXTMESSAGE_H_
#define ACTIVEMQ_TRANSPORT_STOMP_STOMPTEXTMESSAGE_H_

#include <activemq/transport/stomp/DestinationMessage.h>
#include <activemq/transport/stomp/TransactionMessage.h>
#include <cms/TextMessage.h>

namespace activemq{
namespace transport{
namespace stomp{
		
    /**
     * A stomp text message implementation
     * @author Nathan Mittler
     */
	class StompTextMessage 
	: 
		public DestinationMessage,
		public TransactionMessage,
		public cms::TextMessage
	{
	public:
	
		StompTextMessage(){
			transactionId = NULL;
            own = false;
            message = NULL;
		}
        
		virtual ~StompTextMessage(){
			if( transactionId != NULL ){
				delete transactionId;
			}
            
            clear();
		};        
        
		virtual MessageType getMessageType() const{
			return MSG_TEXT;
		}
		virtual const cms::Message* getCMSMessage() const{
			return this;
		}
		virtual cms::Message* getCMSMessage(){
			return this;
		}
		
		virtual void setDestination( const char* destination ){
			this->destination = destination;
		}
		
		virtual const char* getDestination() const{
			return destination.c_str();
		}
		
		virtual bool isTransaction() const{
			return transactionId != NULL;
		}
		
		virtual const char* getTransactionId() const{
			if( isTransaction() ){
				return transactionId->c_str();
			}
			return  NULL;
		}
		
		virtual void setTransactionId( const char* id ){
			if( transactionId != NULL ){
				delete transactionId;
				transactionId = NULL;
			}
			
			transactionId = new std::string( id );
		}
		
		virtual const char* getText() const throw( cms::CMSException ){
			return message;
		}
		
		virtual void setText( const char* msg ) throw( cms::CMSException ){
            
            clear();
            
            int len = strlen( msg );                     
            
            own = true;
			message = new char[len+1];
            memcpy( message, msg, len + 1 );
		}
        
        virtual void setTextNoCopy( const char* msg ) throw( cms::CMSException ){
            
            clear();
            
            own = false;
            message = const_cast<char*>(msg);
        }
		
		virtual void acknowledge() throw( cms::CMSException ){			
		}
		
		virtual cms::Message* clone() const{
			StompTextMessage* msg = new StompTextMessage();
			msg->destination = destination;
			if( transactionId != NULL ){
				msg->setTransactionId( transactionId->c_str() );
			}
			msg->message = message;
			return msg->getCMSMessage();
		}
		
    private:
    
        void clear(){
            
            if( message != NULL && own ){
                delete [] message;
            }
            message = NULL;
        }
        
	private:
	
		std::string destination;
		std::string* transactionId;
		//std::string message;
        char* message;
        bool own;
	};

}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_STOMPTEXTMESSAGE_H_*/
