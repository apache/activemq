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
#ifndef ActiveMQ_MessageProducer_hpp_
#define ActiveMQ_MessageProducer_hpp_

#include <string>
#include "cms/IDestination.hpp"
#include "cms/IMessage.hpp"
#include "cms/IMessageProducer.hpp"
#include "activemq/command/ProducerInfo.hpp"
#include "ppr/thread/SimpleMutex.hpp"
#include "ppr/util/Time.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    using namespace ifr;
    using namespace apache::cms;
    using namespace apache::activemq::command;
    using namespace apache::ppr;
    using namespace apache::ppr::thread;
    class Session ;

/*
 * 
 */
class MessageProducer : public IMessageProducer
{
private:
    p<Session>      session ;
    p<ProducerInfo> producerInfo ;
    SimpleMutex     mutex ;
    int             priority ;
    long long       messageCounter,
                    timeToLive ;
    bool            closed,
                    persistent,
                    disableMessageID,
                    disableMessageTimestamp ;

    // Default message values
    const static char      DEFAULT_PRIORITY   = 4 ;
    const static long long DEFAULT_TIMETOLIVE = 0 ;

public:
    MessageProducer(p<Session> session, p<ProducerInfo> producerInfo) ;
    virtual ~MessageProducer() ;

    // Attribute methods
	virtual bool getPersistent() ;
	virtual void setPersistent(bool persistent) ;
    virtual long long getTimeToLive() ;
    virtual void getTimeToLive(long long ttl) ;
    virtual int getPriority() ;
    virtual void getPriority(int priority) ;
    virtual bool getDisableMessageID() ;
    virtual void getDisableMessageID(bool disable) ;
    virtual bool getDisableMessageTimestamp() ;
    virtual void getDisableMessageTimestamp(bool disable) ;

    // Operation methods
    virtual void send(p<IMessage> message) ;
    virtual void send(p<IDestination> destination, p<IMessage> message) ;
    virtual void send(p<IDestination> destination, p<IMessage> message, char priority, long long timeToLive) ;
    virtual void close() ;

private:
    long long getCurrentTimeMillis() ;
} ;

/* namespace */
  }
}

#endif /*ActiveMQ_MessageProducer_hpp_*/
