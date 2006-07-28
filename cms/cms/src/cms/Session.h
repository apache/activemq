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

#ifndef _CMS_SESSION_H_
#define _CMS_SESSION_H_
 
#include <cms/CMSException.h>
#include <cms/Closeable.h>
#include <cms/TransactionController.h>

namespace cms{
	
	// Forward declarations.
	class TextMessage;
	class BytesMessage;
		
	class Session 
	: 
		public Closeable,
		public TransactionController{	
		
	public:
	
		enum AcknowledgeMode{
			
			/**
			 * With this acknowledgment mode, the session automatically 
			 * acknowledges a client's receipt of a message either when 
			 * the session has successfully returned from a call to receive 
			 * or when the message listener the session has called to 
			 * process the message successfully returns.
			 */
			AUTO_ACKNOWLEDGE,
			
			/**
			 * With this acknowledgment mode, the client acknowledges a 
			 * consumed message by calling the message's acknowledge method.
			 */
			CLIENT_ACKNOWLEDGE,

		};
		
	public:
	
		virtual ~Session(){}
		
		/**
		 * Indicates whether the session is in transacted mode.
		 * @return true if the session is in transacted mode.
		 */
		virtual bool getTransacted() const = 0;
		
		/**
		 * Creates a TextMessage object.
		 * @return a new text message object.
		 */
		virtual TextMessage* createTextMessage() throw( CMSException ) = 0; 
		
		/**
		 * Creates an initialized TextMessage object.
		 * @param msg The buffer to be set in the message.
		 * @return A new text message object.
		 */
		virtual TextMessage* createTextMessage( const char* msg ) throw( CMSException ) = 0;
		
		/**
		 * Creates a BytesMessage object.
		 * @return A new byte message object.
		 */
		virtual BytesMessage* createBytesMessage() throw( CMSException ) = 0;
	};

}

#endif /*_CMS_SESSION_H_*/
