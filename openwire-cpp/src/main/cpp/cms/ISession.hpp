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
#ifndef Cms_ISession_hpp_
#define Cms_ISession_hpp_

#include "cms/IDestination.hpp"
#include "cms/IMessageProducer.hpp"
#include "cms/IMessageConsumer.hpp"
#include "cms/IQueue.hpp"
#include "cms/ITopic.hpp"
#include "cms/ITemporaryQueue.hpp"
#include "cms/ITemporaryTopic.hpp"
#include "cms/ITextMessage.hpp"
#include "cms/IBytesMessage.hpp"
#include "cms/IMapMessage.hpp"
#include "cms/CmsException.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace cms
  {
    using namespace ifr;

/*
 * 
 */
struct ISession : Interface
{
    virtual void commit() throw(CmsException) = 0 ;
    virtual void rollback() throw(CmsException) = 0 ;
    virtual p<IQueue> getQueue(const char* name) = 0 ;
    virtual p<ITopic> getTopic(const char* name) = 0 ;
    virtual p<IMessageProducer> createProducer() = 0 ;
    virtual p<IMessageProducer> createProducer(p<IDestination> destination) = 0 ;
    virtual p<IMessageConsumer> createConsumer(p<IDestination> destination) = 0 ;
    virtual p<IMessageConsumer> createConsumer(p<IDestination> destination, const char* selector) = 0 ;
    virtual p<IMessageConsumer> createDurableConsumer(p<ITopic> destination, const char* name, const char* selector, bool noLocal) = 0 ;
    virtual p<ITemporaryQueue> createTemporaryQueue() = 0 ;
    virtual p<ITemporaryTopic> createTemporaryTopic() = 0 ;
    virtual p<IMessage> createMessage() = 0 ;
    virtual p<IBytesMessage> createBytesMessage() = 0 ;
    virtual p<IBytesMessage> createBytesMessage(char* body, int size) = 0 ;
    virtual p<IMapMessage> createMapMessage() = 0 ;
    virtual p<ITextMessage> createTextMessage() = 0 ;
    virtual p<ITextMessage> createTextMessage(const char* text) = 0 ;
    virtual void close() = 0 ;
} ;

/* namespace */
  }
}

#endif /*Cms_ISession_hpp_*/
