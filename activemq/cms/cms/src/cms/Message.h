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

#ifndef _CMS_MESSAGE_H_
#define _CMS_MESSAGE_H_
 
#include <cms/CMSException.h>

namespace cms{
	
	/**
	 * Root of all messages.
	 */
	class Message{	
		
	public:
	
		virtual ~Message(){}
		
		/**
		 * Acknowledges all consumed messages of the session 
		 * of this consumed message.
		 */
		virtual void acknowledge() throw( CMSException ) = 0;
        
        /**
         * Clones this message.
         * @return a copy of this message.  The caller is responsible
         * for freeing this memory.
         */
        virtual Message* clone() const = 0;
	};
}

#endif /*_CMS_MESSAGE_H_*/
