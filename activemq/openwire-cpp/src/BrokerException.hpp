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
#ifndef BrokerException_hpp_
#define BrokerException_hpp_

#include <string>
#include "BrokerError.hpp"
#include "OpenWireException.hpp"
#include "util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      using namespace std;
      using namespace ifr ;

/*
 * 
 */
class BrokerException : public OpenWireException
{
private:
    p<BrokerError> brokerError ;
    
public:
    BrokerException(p<BrokerError> cause) ;
    virtual ~BrokerException() ;

    virtual p<BrokerError> getCause() ;
};

/* namespace */
    }
  }
}

#endif /*BrokerException_hpp_*/
