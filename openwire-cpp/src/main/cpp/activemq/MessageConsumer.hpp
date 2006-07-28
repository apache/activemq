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
#ifndef ActiveMQ_MessageConsumer_hpp_
#define ActiveMQ_MessageConsumer_hpp_

#include <string>
#include "cms/IDestination.hpp"
#include "cms/IMessage.hpp"
#include "cms/IMessageConsumer.hpp"
#include "cms/IMessageListener.hpp"
#include "cms/CmsException.hpp"
#include "activemq/AcknowledgementMode.hpp"
#include "activemq/MessageAckType.hpp"
#include "activemq/Dispatcher.hpp"
#include "activemq/IAcknowledger.hpp"
#include "activemq/MessageConsumerSynchronization.hpp"
#include "activemq/ConnectionClosedException.hpp"
#include "activemq/command/ConsumerInfo.hpp"
#include "activemq/command/Message.hpp"
#include "activemq/command/ActiveMQMessage.hpp"
#include "activemq/command/MessageAck.hpp"
#include "ppr/util/ifr/p"
#include "ppr/thread/Thread.hpp"

// Turn off warning message for ignored exception specification
#ifdef _MSC_VER
#pragma warning( disable : 4290 )
#endif

namespace apache
{
  namespace activemq
  {
      using namespace ifr;
      using namespace apache::cms;
      using namespace apache::ppr::thread;
      class Session ;

/*
 * 
 */
class MessageConsumer : public IMessageConsumer, public IAcknowledger
{
private:
    p<Session>          session ;
    p<ConsumerInfo>     consumerInfo ;
    p<Dispatcher>       dispatcher ;
    p<IMessageListener> listener ;
    AcknowledgementMode acknowledgementMode ;
    bool                closed ;
    int                 maximumRedeliveryCount,
                        redeliveryTimeout ;

public:
    MessageConsumer(p<Session> session, p<ConsumerInfo> consumerInfo, AcknowledgementMode acknowledgementMode) ;
    virtual ~MessageConsumer() ;

    virtual void setMessageListener(p<IMessageListener> listener) ;
    virtual p<IMessageListener> getMessageListener() ;
    virtual p<ConsumerId> getConsumerId() ;
    virtual void setMaximumRedeliveryCount(int count) ;
    virtual int getMaximumRedeliveryCount() ;
    virtual void setRedeliveryTimeout(int timeout) ;
    virtual int getRedeliveryTimeout() ;

    virtual p<IMessage> receive() ;
    virtual p<IMessage> receive(int timeout) ;
    virtual p<IMessage> receiveNoWait() ;
    virtual void redeliverRolledBackMessages() ;
    virtual void dispatch(p<IMessage> message) ;
    virtual void dispatchAsyncMessages() ;
    virtual void afterRollback(p<ActiveMQMessage> message) ;
    virtual void acknowledge(p<ActiveMQMessage> message) ;
    virtual void close() ;

protected:
    void checkClosed() throw(CmsException) ;
    p<IMessage> autoAcknowledge(p<IMessage> message) ;
    void doClientAcknowledge(p<ActiveMQMessage> message) ;
    void doAcknowledge(p<Message> message) ;
    p<MessageAck> createMessageAck(p<Message> message) ;
} ;

/* namespace */
  }
}

#endif /*ActiveMQ_IMessageConsumer_hpp_*/
