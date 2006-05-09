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

#ifndef _CMS_TRANSACTIONCONTROLLER_
#define _CMS_TRANSACTIONCONTROLLER_
 
#include <cms/CMSException.h>

namespace cms{
	
	class TransactionController{
		
	public:
	
		virtual ~TransactionController(){}
		
		/**
		 * Commits the current transaction.
		 */
		virtual void commit() throw( CMSException ) = 0;
		
		/**
		 * Cancels the current transaction and rolls the system state
		 * back to before the transaction occurred.
		 */
		virtual void rollback() throw( CMSException ) = 0;
	};
}

#endif /*_CMS_TRANSACTIONCONTROLLER_*/
