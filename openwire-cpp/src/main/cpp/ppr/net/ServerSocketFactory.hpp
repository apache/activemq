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
#ifndef Ppr_ServerSocketFactory_hpp_
#define Ppr_ServerSocketFactory_hpp_

#include "ppr/net/IServerSocketFactory.hpp"
#include "ppr/net/ServerSocket.hpp"

namespace apache
{
  namespace ppr
  {
    namespace net
    {
      using namespace std;

/*
 *
 */
class ServerSocketFactory : public IServerSocketFactory
{
public:
    /** Creates an unbound TCP/IP server socket, ready to call bind() on.
    */
    virtual p<IServerSocket> createSocket() {
        return new ServerSocket();
    }
};

/* namespace */
    }
  }
}

#endif /*Ppr_ServerSocketFactory_hpp_*/

