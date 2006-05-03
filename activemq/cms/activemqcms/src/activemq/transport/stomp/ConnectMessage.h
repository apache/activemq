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
 
#ifndef ACTIVEMQ_TRANSPORT_STOMP_CONNECTMESSAGE_H_
#define ACTIVEMQ_TRANSPORT_STOMP_CONNECTMESSAGE_H_

#include <activemq/transport/stomp/StompMessage.h>
#include <string>

namespace activemq{
namespace transport{
namespace stomp{
	
    /**
     * Message sent to the broker to connect.
     * @author Nathan Mittler
     */
	class ConnectMessage : public StompMessage
	{
	public:
		virtual ~ConnectMessage(){};
		
		virtual MessageType getMessageType() const{
			return MSG_CONNECT;
		}
		
		virtual const cms::Message* getCMSMessage() const{
			return NULL;
		}
		
		virtual cms::Message* getCMSMessage(){
			return NULL;
		}
		
		virtual const std::string& getLogin() const{
			return login;
		}
		
		virtual void setLogin( const std::string& login ){
			this->login = login;
		}
		
		virtual const std::string& getPassword() const{
			return password;
		}
		
		virtual void setPassword( const std::string& password ){
			this->password = password;
		}		
		
		virtual cms::Message* clone() const{
			ConnectMessage* msg = new ConnectMessage();
			msg->login = login;
			msg->password = password;
			return msg->getCMSMessage();
		}
		
	private:
	
		std::string login;
		std::string password;
	};
	
}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_CONNECTMESSAGE_H_*/
