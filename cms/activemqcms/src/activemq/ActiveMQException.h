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

#ifndef ACTIVEMQ_STOMPEXCEPTION_H_
#define ACTIVEMQ_STOMPEXCEPTION_H_

#include <cms/CMSException.h>
#include <string>

namespace activemq{
	
	class ActiveMQException : public cms::CMSException{
		
	public:
	
		ActiveMQException( const ActiveMQException& ex ){
			(*this)=ex;
		}
		
		ActiveMQException( const char* msg ){
			this->msg = msg;
		}
		
		ActiveMQException( const std::string msg ){
			this->msg = msg;
		}
		
		virtual ~ActiveMQException(){}
		
		/**
		 * @return The error message.
		 */
		virtual const char* getMessage() const{
			
			return msg.c_str();
		}
		
	private:
		
		std::string msg;
	};
}

#endif /*ACTIVEMQ_STOMPEXCEPTION_H_*/
