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
#ifndef ActiveMQ_ActiveMQDestination_hpp_
#define ActiveMQ_ActiveMQDestination_hpp_

#include <typeinfo>
#include "cms/IDestination.hpp"
#include "cms/ITopic.hpp"
#include "cms/IQueue.hpp"
#include "cms/ITemporaryTopic.hpp"
#include "cms/ITemporaryQueue.hpp"
#include "activemq/command/AbstractCommand.hpp"
#include "activemq/protocol/IMarshaller.hpp"
#include "ppr/io/IOutputStream.hpp"
#include "ppr/io/IInputStream.hpp"
#include "ppr/io/IOException.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace command
    {
      using namespace ifr;
      using namespace apache::activemq;
      using namespace apache::activemq::protocol;
      using namespace apache::cms;
      using namespace apache::ppr::io;
      class ActiveMQQueue ;
      class ActiveMQTempQueue ;
      class ActiveMQTopic ;
      class ActiveMQTempTopic ;

/*
 * 
 */
class ActiveMQDestination : public AbstractCommand, public IDestination
{
private:
    p<string> orderedTarget,
              physicalName ;
    bool      exclusive,
              ordered,
              advisory ;

    // Prefix/postfix for queue/topic names
    static const char* TEMP_PREFIX ;
    static const char* TEMP_POSTFIX ;
    static const char* COMPOSITE_SEPARATOR ;
    static const char* QUEUE_PREFIX ;
    static const char* TOPIC_PREFIX ;

public:
    // Destination type constants
    static const int ACTIVEMQ_TOPIC           = 1 ;
    static const int ACTIVEMQ_TEMPORARY_TOPIC = 2 ;
    static const int ACTIVEMQ_QUEUE           = 3 ;
    static const int ACTIVEMQ_TEMPORARY_QUEUE = 4 ;

    // Prefixes for Advisory message destinations
    static const char* ADVISORY_PREFIX ;
    static const char* CONSUMER_ADVISORY_PREFIX ;
    static const char* PRODUCER_ADVISORY_PREFIX ;
    static const char* CONNECTION_ADVISORY_PREFIX ;

    // The default target for ordered destinations
    static const char* DEFAULT_ORDERED_TARGET ;

protected:
    ActiveMQDestination() ;
    ActiveMQDestination(const char* name) ;

public:
    virtual ~ActiveMQDestination() ;

    // Attributes methods
    virtual bool isAdvisory() ;
    virtual void setAdvisory(bool advisory) ;
    virtual bool isConsumerAdvisory() ;
    virtual bool isProducerAdvisory() ;
    virtual bool isConnectionAdvisory() ;
    virtual bool isExclusive() ;
    virtual void setExclusive(bool exclusive) ;
    virtual bool isOrdered() ;
    virtual void setOrdered(bool ordered) ;
    virtual p<string> getOrderedTarget() ;
    virtual void setOrderedTarget(const char* target) ;
    virtual p<string> getPhysicalName() ;
    virtual void setPhysicalName(const char* name) ;
    virtual bool isTopic() ;
    virtual bool isQueue() ;
    virtual bool isTemporary() ;
    virtual bool isComposite() ;
    virtual bool isWildcard() ;
    virtual p<string> toString() ;

    virtual int marshal(p<IMarshaller> marshaller, int mode, p<IOutputStream> writer) throw (IOException) ;
    virtual void unmarshal(p<IMarshaller> marshaller, int mode, p<IInputStream> reader) throw (IOException) ;

    //
    // Abstract methods

    // Factory method to create a child destination if this destination is a composite.
    virtual p<ActiveMQDestination> createDestination(const char* name) = 0 ;
    virtual int getDestinationType() = 0 ;

    //
    // Static methods

    static p<string> inspect(p<ActiveMQDestination> destination) ;
    //static p<ActiveMQDestination> transform(p<IDestination> destination) ;
    static p<ActiveMQDestination> createDestination(int type, const char* physicalName) ;
    static p<string> createTemporaryName(const char* clientId) ;
    static p<string> getClientId(p<ActiveMQDestination> destination) ;
} ;

/* namespace */
    }
  }
}

#endif /*ActiveMQ_ActiveMQDestination_hpp_*/
