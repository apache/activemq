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

#ifndef _CMS_TEXTMESSAGE_H_
#define _CMS_TEXTMESSAGE_H_
 
#include <cms/Message.h>
#include <cms/CMSException.h>

namespace cms{
	
	/**
	 * Interface for a text message.
	 */
	class TextMessage : public Message{
		
	public:
	
		virtual ~TextMessage(){}
		
		/**
		 * Gets the message character buffer.
		 * @return The message character buffer.
		 */
		virtual const char* getText() const throw( CMSException ) = 0;
		
		/**
		 * Sets the message contents.
		 * @param msg The message buffer.
		 */
		virtual void setText( const char* msg ) throw( CMSException ) = 0;
	};
}

#endif /*_CMS_TEXTMESSAGE_H_*/
