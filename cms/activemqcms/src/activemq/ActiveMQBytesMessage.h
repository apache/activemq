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

#ifndef ACTIVEMQ_ACTIVEMQBYTESMESSAGE_H_
#define ACTIVEMQ_ACTIVEMQBYTESMESSAGE_H_

#include <cms/BytesMessage.h>

namespace activemq{
		
    /**
     * Simple implementation of the bytes message interface.
     * @author Nathan Mittler
     */
	class ActiveMQBytesMessage : public cms::BytesMessage
	{
	public:
	
		ActiveMQBytesMessage(){
			data = NULL;
			numBytes = 0;
		}
		virtual ~ActiveMQBytesMessage(){			
			clearData();
		};
		
		virtual void setData( const char* data, const int numBytes ) throw(cms::CMSException){
			clearData();
			
			char* buf = new char[numBytes];
			memcpy( buf, data, numBytes );
			this->data = buf;
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
        
        /**
         * Clones this message.
         * @return a copy of this message.  The caller is responsible
         * for freeing this memory.
         */
        virtual cms::Message* clone() const{
            
            ActiveMQBytesMessage* newMsg = new ActiveMQBytesMessage();
            newMsg->setData( data, numBytes );
            return newMsg;
        }
		
	protected:
	
		void clearData(){
			if( data != NULL ){
				delete [] data;
				data = NULL;
			}
		}
		
	private:

		int numBytes;
		char* data;
	};

}

#endif /*ACTIVEMQ_ACTIVEMQBYTESMESSAGE_H_*/
