/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
  
  http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
*/

#ifndef ACTIVEMQ_BLOCKINGMSGCONSUMER_REF_H
#define ACTIVEMQ_BLOCKINGMSGCONSUMER_REF_H

#include <semaphore.h>
#include <pthread.h>
#include <memory>
#include <deque>

#include "MessageConsumerRef.h"
#include "BlockingMessageConsumer.h"

namespace ActiveMQ {
    class CoreLib;

    /// handle on message vendor with blocking semantics.
    /**
       This class is a handle on a BlockingMessageConsumer, which is
       an object that holds messages that have been received by a
       BrokerSession object but not yet received by the application.  It
       contains an internal queue and a semaphore (this class is
       thread safe).

       When a Message is enqueued, it is stored internally and the
       semaphore is posted.

       The receive() call will block until a message is ready, then
       return that message.

       BlockingMessageConsumers are owned by the library, so this weak
       reference class is provided to allow flexible sharing of the
       handle (like a pointer) while at the same time providing
       protection from pointer invalidation.  When the owning library
       is destructed, it will nullify existing weak reference objects
       so that calls will throw instead of crashing.

       @version $Id$
    */
    class BlockingMessageConsumerRef : public MessageConsumerRef {
    public:
        BlockingMessageConsumerRef() : MessageConsumerRef(), cons_(NULL) {}
        BlockingMessageConsumerRef(const BlockingMessageConsumerRef &);
        BlockingMessageConsumerRef& operator=(const BlockingMessageConsumerRef& oth);
        virtual ~BlockingMessageConsumerRef();

        /// gets the number of messages that are ready
        /**
           @returns the number of waiting messages
        */
        int getNumReadyMessages() const;

        /// blocking receive
        /**
           Receives a Message.  If none are ready, this will block
           until one is.  It is returned as a std::auto_ptr to make
           the ownership policy clear.

           Note that a variant of receive() that returns a regular
           pointer is defined on ActiveMQ::MessageConsumerRef.

           @returns the new Message
        */
        std::auto_ptr<Message> receive();

        /// checks validity
        /**
           Since this class is a weak reference, it could be
           invalidated.  If it's invalid, calling these functions will
           throw.

           @returns true if the reference is invalid
        */
        virtual bool isValid() const;

    private:
        friend class CoreLibImpl;
        BlockingMessageConsumerRef(CoreLib *a, BlockingMessageConsumer *q);
        
        BlockingMessageConsumer *cons_;
        MessageConsumer *getConsumer() const { return cons_; }
    };
};

#endif // ACTIVEMQ_BLOCKINGMSGCONSUMER_REF_H
