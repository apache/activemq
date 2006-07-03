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
#ifndef _CMS_CONNECTIONFACTORY_H_
#define _CMS_CONNECTIONFACTORY_H_

#include <cms/Connection.h>
#include <cms/CMSException.h>

#include <string>

namespace cms
{

   class ConnectionFactory
   {
   public:

      /**
       * Destructor
       */
   	virtual ~ConnectionFactory(void) {}

      /**
       * Creates a connection with the default user identity. The 
       * connection is created in stopped mode. No messages will be 
       * delivered until the Connection.start method is explicitly 
       * called. 
       * @throws CMSException
       */
      virtual Connection* createConnection(void) throw ( CMSException ) = 0;

      /**
       * Creates a connection with the specified user identity. The 
       * connection is created in stopped mode. No messages will be 
       * delivered until the Connection.start method is explicitly called.
       * @throw CMSException.
       */
      virtual Connection* createConnection(const std::string& username,
                                           const std::string& password,
                                           const std::string& clientId) 
                                              throw ( CMSException ) = 0;

   };

}

#endif /*_CMS_CONNECTIONFACTORY_H_*/
