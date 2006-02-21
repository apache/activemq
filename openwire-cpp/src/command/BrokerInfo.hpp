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
#ifndef BrokerInfo_hpp_
#define BrokerInfo_hpp_

#include <string>
#include "command/BaseCommand.hpp"
#include "command/BrokerId.hpp"
#include "util/ifr/ap"
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
 *
 */
class BrokerInfo : public BaseCommand
{
private:
    p<BrokerId>    brokerId ;
    p<string>      brokerURL,
                   brokerName ;
    ap<BrokerInfo> peerInfos ;

public:
    const static char TYPE = 2 ;

public:
    BrokerInfo() ;
    virtual ~BrokerInfo() ;

    virtual int getCommandType() ;
    virtual p<string> getBrokerName() ;
    virtual void setBrokerName(const char* name) ;
    virtual p<BrokerId> getBrokerId() ;
    virtual void setBrokerId(p<BrokerId> id) ;
    virtual p<string> getBrokerURL() ;
    virtual void setBrokerURL(const char* url) ;
    virtual ap<BrokerInfo> getPeerBrokerInfo() ;
    virtual void setPeerBrokerInfo(ap<BrokerInfo> info) ;
} ;

/* namespace */
      }
    }
  }
}

#endif /*BrokerInfo_hpp_*/