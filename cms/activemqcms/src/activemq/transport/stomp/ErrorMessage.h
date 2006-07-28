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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_ERRORMESSAGE_H_
#define ACTIVEMQ_TRANSPORT_STOMP_ERRORMESSAGE_H_

#include <activemq/transport/stomp/StompMessage.h>

namespace activemq{
namespace transport{
namespace stomp{
	
    /**
     * Message sent from the broker when an error
     * occurs.
     * @author Nathan Mittler
     */
	class ErrorMessage : public StompMessage
	{
	public:
		virtual ~ErrorMessage(){};
		
		virtual MessageType getMessageType() const{
			return MSG_ERROR;
		}
		virtual const cms::Message* getCMSMessage() const{
			return NULL;
		}
		virtual cms::Message* getCMSMessage(){
			return NULL;
		}
		
		virtual void setErrorTitle( const char* title ){
			errorTitle = title;
		}
		
		virtual const std::string& getErrorTitle() const{
			return errorTitle;
		}
		
		virtual void setErrorText( const char* text ){
			errorText = text;
		}
		
		virtual const std::string& getErrorText() const{
			return errorText;
		}
		
		virtual cms::Message* clone() const{
			ErrorMessage* msg = new ErrorMessage();
			msg->errorTitle = errorTitle;
			msg->errorText = errorText;
			return msg->getCMSMessage();
		}
		
	private:
	
		std::string errorTitle;
		std::string errorText;
	};
	
}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_ERRORMESSAGE_H_*/
