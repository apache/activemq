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

#ifndef ACTIVEMQ_ACTIVEMQTEXTMESSAGE_H_
#define ACTIVEMQ_ACTIVEMQTEXTMESSAGE_H_

#include <cms/TextMessage.h>

namespace activemq{
		
	class ActiveMQTextMessage : public cms::TextMessage
	{
	public:
	
		ActiveMQTextMessage(){
		}
		virtual ~ActiveMQTextMessage(){
		}
		
		virtual const char* getText() const throw( cms::CMSException ){
			return message.c_str();
		}
		
		virtual void setText( const char* msg ) throw( cms::CMSException ){
			message = msg;
		}
		
		virtual void acknowledge() throw( cms::CMSException ){			
		}
		
        /**
         * Clones this message.
         * @return a copy of this message.  The caller is responsible
         * for freeing this memory.
         */
        virtual cms::Message* clone() const{
            
            ActiveMQTextMessage* newMsg = new ActiveMQTextMessage();
            newMsg->setText( message.c_str() );
            return newMsg;
        }
        
	private:
	
		std::string message;
	};

}

#endif /*ACTIVEMQ_ACTIVEMQTEXTMESSAGE_H_*/
