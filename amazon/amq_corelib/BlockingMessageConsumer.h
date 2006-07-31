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

#ifndef ACTIVEMQ_BLOCKINGMSGCONSUMER_H
#define ACTIVEMQ_BLOCKINGMSGCONSUMER_H

#include <pthread.h>
#include <memory>
#include <deque>

#include "MessageConsumer.h"
#include "Sem.h"

extern const char* const BlockingMessageConsumer_RCSID;

namespace ActiveMQ {
    class CoreLib;

    /// Message vendor with blocking semantics. (private)
    /**
       This class holds messages that have been received by a
       BrokerSession object but not delivered to the application.  It
       contains an internal queue and a semaphore.

       When a Message is enqueued, it is stored internally and one of
       the threads waiting on the blocking receive() call is woken
       with the new message.

       <b>The user of the API never actually owns one of these.</b>
       The library vends instances of BlockingMessageConsumerRef.
       The BlockingMessageConsumer object is destroyed when all of
       its references have been destroyed.

       See BlockingMessageConsumerRef for usage details.

       @version $Id$
    */
    class BlockingMessageConsumer : public MessageConsumer {
    private:
        std::deque<Message *> messages_;
        Semaphore ready_;
        mutable pthread_mutex_t messages_lock_;

        BlockingMessageConsumer(const BlockingMessageConsumer& oth);
        const BlockingMessageConsumer& operator=(const BlockingMessageConsumer& oth);

        friend class CoreLibImpl;
        BlockingMessageConsumer();
        virtual ~BlockingMessageConsumer();
        void enqueue(Message *msg);
        void removeQueued(const Destination &d);

        friend class BlockingMessageConsumerRef;
        unsigned int getNumReadyMessages() const;
        std::auto_ptr<Message> receive();
    };
};

#endif // ACTIVEMQ_BLOCKINGMSGCONSUMER_H
