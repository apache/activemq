/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef ActiveMQ_BrokerException_hpp_
#define ActiveMQ_BrokerException_hpp_

#include <string>
#include "activemq/command/BrokerError.hpp"
#include "cms/CmsException.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    using namespace std;
    using namespace ifr;
    using namespace apache::cms;
    using namespace apache::activemq::command;

/*
 * 
 */
class BrokerException : public CmsException
{
private:
    p<BrokerError> brokerError ;
    
public:
    BrokerException(p<BrokerError> brokerError) ;
    virtual ~BrokerException() throw() {}

    virtual p<BrokerError> getBrokerError() ;
    virtual p<string> getJavaStackTrace() ;
};

/* namespace */
  }
}

#endif /*ActiveMQ_BrokerException_hpp_*/
