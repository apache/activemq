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
#ifndef ConnectionInfo_hpp_
#define ConnectionInfo_hpp_

#include <string>
#include "command/BaseCommand.hpp"
#include "command/BrokerId.hpp"
#include "command/ConnectionId.hpp"
#include "util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      namespace command
      {
        using namespace ifr;
        using namespace std;

/*
 * Dummy, should be auto-generated.
 */
class ConnectionInfo : public BaseCommand
{
private:
    p<ConnectionId> connectionId ;
    p<BrokerId*>    brokerPath ;
    p<string>       username,
                    password,
                    clientId ;
public:
    const static int TYPE = 3 ;

public:
    ConnectionInfo() ;
    virtual ~ConnectionInfo() ;

    virtual p<ConnectionId> getConnectionId() ;
    virtual void setConnectionId(p<ConnectionId> connectionId) ;
    virtual p<string> getUsername() ;
    virtual void setUsername(const char* username) ;
    virtual p<string> getPassword() ;
    virtual void setPassword(const char* password) ;
    virtual p<string> getClientId() ;
    virtual void setClientId(const char* clientId) ;
    virtual p<BrokerId*> getBrokerPath() ;
    virtual void setBrokerPath(p<BrokerId*> brokerPath) ;
} ;

/* namespace */
      }
    }
  }
}

#endif /*ConnectionInfo_hpp_*/
