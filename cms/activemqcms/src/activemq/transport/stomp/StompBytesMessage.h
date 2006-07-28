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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_STOMPBYTESMESSAGE_H_
#define ACTIVEMQ_TRANSPORT_STOMP_STOMPBYTESMESSAGE_H_

#include <activemq/transport/stomp/DestinationMessage.h>
#include <activemq/transport/stomp/TransactionMessage.h>
#include <cms/BytesMessage.h>

namespace activemq{
namespace transport{
namespace stomp{
		
    /**
     * A binary data message.
     * @author Nathan Mittler
     */
	class StompBytesMessage 
	: 
		public DestinationMessage,
		public TransactionMessage,
		public cms::BytesMessage
	{
	public:
	
		StompBytesMessage(){
			transactionId = NULL;
			data = NULL;
			numBytes = 0;
            own = false;
		}
        
		virtual ~StompBytesMessage(){
			if( transactionId != NULL ){
				delete transactionId;
			}
			
			clearData();
		};
		
		virtual MessageType getMessageType() const{
			return MSG_BYTES;
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
		
		virtual void setData( const char* data, const int numBytes ) throw(cms::CMSException){
			clearData();
			
            own = true;
            
			char* buf = new char[numBytes];
			memcpy( buf, data, numBytes );
			this->data = buf;
			this->numBytes = numBytes;			
		}
        
        virtual void setDataNoCopy( const char* data, const int numBytes ) throw(cms::CMSException){
            clearData();
            
            own = false;
            
            this->data = const_cast<char*>(data);
            this->numBytes = numBytes;          
        }
		
		virtual int getNumBytes() const{
			return numBytes;
		}
		
		virtual const char* getData() const{
			return data;
		}
		
		virtual void acknowledge() throw( cms::CMSException ){			
		}
		
		virtual cms::Message* clone() const{
			StompBytesMessage* msg = new StompBytesMessage();
			msg->destination = destination;
			if( transactionId != NULL ){
				msg->setTransactionId( transactionId->c_str() );
			}
			msg->setData( data, numBytes );
			return msg;
		}
		
	protected:
	
		void clearData(){
			if( data != NULL && own ){
				delete [] data;				
			}
            data = NULL;
		}
		
	private:
	
		std::string destination;
		std::string* transactionId;
		int numBytes;
		char* data;
        bool own;
	};

}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_STOMPBYTESMESSAGE_H_*/
