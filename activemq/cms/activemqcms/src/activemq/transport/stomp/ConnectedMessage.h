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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_CONNECTEDMESSAGE_H_
#define ACTIVEMQ_TRANSPORT_STOMP_CONNECTEDMESSAGE_H_
 
#include <activemq/transport/stomp/StompMessage.h>

namespace activemq{
namespace transport{
namespace stomp{
	
    /**
     * The stomp message returned from the broker indicating
     * a connection has been established.
     * @author Nathan Mittler
     */
	class ConnectedMessage : public StompMessage
	{
	public:
		virtual ~ConnectedMessage(){};
		
		virtual MessageType getMessageType() const{
			return MSG_CONNECTED;
		}
		
		virtual const cms::Message* getCMSMessage() const{
			return NULL;
		}
		
		virtual cms::Message* getCMSMessage(){
			return NULL;
		}
		
		virtual void setSessionId( const char* sessionId ){
			this->sessionId = sessionId;
		}
		virtual const std::string& getSessionId() const{
			return sessionId;
		}
		
		virtual cms::Message* clone() const{
			ConnectedMessage* msg = new ConnectedMessage();
			msg->sessionId = sessionId;
			return msg->getCMSMessage();
		}
		
	private:
	
		std::string sessionId;
		
	};

}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_CONNECTEDMESSAGE_H_*/
