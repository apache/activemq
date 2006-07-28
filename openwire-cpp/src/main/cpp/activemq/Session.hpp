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
#ifndef ActiveMQ_Session_hpp_
#define ActiveMQ_Session_hpp_

#include <string>
#include <map>
#include "activemq/AcknowledgementMode.hpp"
#include "cms/IMessage.hpp"
#include "cms/IBytesMessage.hpp"
#include "cms/IMapMessage.hpp"
#include "cms/ITextMessage.hpp"
#include "cms/ISession.hpp"
#include "cms/CmsException.hpp"
#include "activemq/IDataStructure.hpp"
#include "activemq/MessageConsumer.hpp"
#include "activemq/MessageProducer.hpp"
#include "activemq/TransactionContext.hpp"
#include "activemq/command/ConsumerInfo.hpp"
#include "activemq/command/ProducerInfo.hpp"
#include "activemq/command/RemoveInfo.hpp"
#include "activemq/command/SessionInfo.hpp"
#include "activemq/command/SessionId.hpp"
#include "ppr/thread/SimpleMutex.hpp"
#include "ppr/thread/Semaphore.hpp"
#include "ppr/thread/Thread.hpp"
#include "ppr/util/ifr/array"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
      using namespace ifr;
      using namespace apache::cms;
      using namespace apache::ppr::thread;
      using namespace apache::ppr::util;
      class Connection;
      class DispatchThread;

/*
 * 
 */
class Session : public ISession
{
private:
    p<Connection>                 connection ;
    p<SessionInfo>                sessionInfo ;
    AcknowledgementMode           ackMode ;
    p<TransactionContext>         transactionContext ;
    p<DispatchThread>             dispatchThread ;
    map<long long, p<MessageConsumer> > consumers ;
    map<long long, p<MessageProducer> > producers ;
    SimpleMutex                   mutex ;
    long                          consumerCounter,
                                  producerCounter ;
    int                           prefetchSize ;
    bool                          closed ;

public:
    Session(p<Connection> connection, p<SessionInfo> sessionInfo, AcknowledgementMode ackMode) ;
    virtual ~Session() ;

    // Attribute methods
    virtual bool isTransacted() ;
    virtual p<Connection> getConnection() ;
    virtual p<SessionId> getSessionId() ;
    virtual p<TransactionContext> getTransactionContext() ;
    virtual p<MessageConsumer> getConsumer(p<ConsumerId> consumerId) ;
    virtual p<MessageProducer> getProducer(p<ProducerId> producerId) ;

    // Operation methods
    virtual void commit() throw(CmsException) ;
    virtual void rollback() throw(CmsException) ;
    virtual p<IMessageProducer> createProducer() ;
    virtual p<IMessageProducer> createProducer(p<IDestination> destination) ;
    virtual p<IMessageConsumer> createConsumer(p<IDestination> destination) ;
    virtual p<IMessageConsumer> createConsumer(p<IDestination> destination, const char* selector) ;
    virtual p<IMessageConsumer> createDurableConsumer(p<ITopic> destination, const char* name, const char* selector, bool noLocal) ;
    virtual p<IQueue> getQueue(const char* name) ;
    virtual p<ITopic> getTopic(const char* name) ;
    virtual p<ITemporaryQueue> createTemporaryQueue() ;
    virtual p<ITemporaryTopic> createTemporaryTopic() ;
    virtual p<IMessage> createMessage() ;
    virtual p<IBytesMessage> createBytesMessage() ;
    virtual p<IBytesMessage> createBytesMessage(char* body, int size) ;
    virtual p<IMapMessage> createMapMessage() ;
    virtual p<ITextMessage> createTextMessage() ;
    virtual p<ITextMessage> createTextMessage(const char* text) ;

    virtual void doSend(p<IDestination> destination, p<IMessage> message) ;
    virtual void doStartTransaction() ;
    virtual void dispatch(int delay = 0) ;
    virtual void dispatchAsyncMessages() ;
    virtual void close() ;

protected:
    // Implementation methods
    p<ConsumerInfo> createConsumerInfo(p<IDestination> destination, const char* selector) ;
    p<ProducerInfo> createProducerInfo(p<IDestination> destination) ;
    void configure(p<IMessage> message) ;
} ;

/*
 * 
 */
class DispatchThread : public Thread
{
public:
    p<Session> session ;
    Semaphore  semaphore ;
    bool       interrupted ;

    DispatchThread(p<Session> session)
    {
        this->session = session ;
        this->interrupted = false ;
    }

    void interrupt()
    {
        interrupted = true ;
    }

    void wakeup()
    {
        semaphore.notify() ;
    }

protected:
    virtual void run () throw (p<exception>) 
    {
        while( !interrupted )
        {
            // Wait for wake-up call
            semaphore.wait() ;

            session->dispatchAsyncMessages() ;
        }
    }
} ;

/* namespace */
  }
}

#endif /*ActiveMQ_Session_hpp_*/
