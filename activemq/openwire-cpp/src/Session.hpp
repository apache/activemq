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
#ifndef Session_hpp_
#define Session_hpp_

#include <string>
#include "IMessage.hpp"
#include "ISession.hpp"
#include "IConnection.hpp"
#include "command/IDataStructure.hpp"
#include "command/ConsumerInfo.hpp"
#include "command/ProducerInfo.hpp"
#include "command/RemoveInfo.hpp"
#include "command/SessionInfo.hpp"
#include "util/SimpleMutex.hpp"
#include "util/ifr/p.hpp"

namespace apache
{
  namespace activemq
  {
    namespace client
    {
        using namespace ifr;
        using namespace apache::activemq::client::util;
        class Connection;

/*
 * 
 */
class Session : public ISession
{
private:
    p<Connection>       connection ;
    p<SessionInfo>      sessionInfo ;
    AcknowledgementMode ackMode ;
    long                consumerCounter ;
    SimpleMutex         mutex ;

public:
    Session(p<Connection> connection, p<SessionInfo> sessionInfo) ;
    virtual ~Session() ;
    
    virtual p<IMessageProducer> createProducer() ;
    virtual p<IMessageProducer> createProducer(p<IDestination> destination) ;
    virtual p<IMessageConsumer> createConsumer(p<IDestination> destination) ;
    virtual p<IMessageConsumer> createConsumer(p<IDestination> destination, const char* selector) ;
    virtual p<IQueue> getQueue(const char* name) ;
    virtual p<ITopic> getTopic(const char* name) ;
    virtual p<IMessage> createMessage() ;
    virtual p<ITextMessage> createTextMessage() ;
    virtual p<ITextMessage> createTextMessage(const char* text) ;
    virtual void acknowledge(p<IMessage> message) ;
    virtual void doSend(p<IDestination> destination, p<IMessage> message) ;
    virtual void disposeOf(p<IDataStructure> objectId) ;

protected:
    p<ConsumerInfo> createConsumerInfo(p<IDestination> destination, const char* selector) ;
    p<ProducerInfo> createProducerInfo(p<IDestination> destination) ;
} ;

/* namespace */
    }
  }
}

#endif /*Session_hpp_*/
