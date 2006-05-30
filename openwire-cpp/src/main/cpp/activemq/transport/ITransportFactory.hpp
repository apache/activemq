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
#ifndef ActiveMQ_ITransportFactory_hpp_
#define ActiveMQ_ITransportFactory_hpp_

#include "activemq/transport/ITransport.hpp"
#include "ppr/IllegalArgumentException.hpp"
#include "ppr/net/SocketException.hpp"
#include "ppr/net/Uri.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace transport
    {
      using namespace ifr ;
      using namespace apache::ppr;
      using namespace apache::ppr::net;

/*
 * 
 */
struct ITransportFactory : Interface
{
	virtual p<ITransport> createTransport(p<Uri> location) throw (SocketException, IllegalArgumentException) = 0 ;
} ;

/* namespace */
    }
  }
}

#endif /*ActiveMQ_ITransportFactory_hpp_*/
