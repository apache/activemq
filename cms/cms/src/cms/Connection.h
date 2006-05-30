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

#ifndef _CMS_CONNECTION_H_
#define _CMS_CONNECTION_H_
 
#include <cms/CMSException.h>
#include <cms/Closeable.h>
#include <cms/Service.h>
#include <cms/Session.h>

namespace cms{
	
	// Forward declarations.
	class ExceptionListener;
	
	class Connection
	:
		public Closeable,
		public Service
	{
		
	public:
	
		virtual ~Connection(){}
		
		virtual Session* createSession( const bool transacted,
			const Session::AcknowledgeMode = Session::AUTO_ACKNOWLEDGE ) throw( CMSException ) = 0;
		
		virtual void setExceptionListener( ExceptionListener* listener ) 
			throw( CMSException ) = 0;
		
		virtual ExceptionListener* getExceptionListener() const 
			throw( CMSException ) = 0;		
	};

}

#endif /*_CMS_CONNECTION_H_*/
