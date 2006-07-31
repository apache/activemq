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

#include "RCSID.h"
#include "BlockingMessageConsumer.h"
#include "Message.h"
#include "Lock.h"

#include <algorithm>

#include <stdio.h>
#include <errno.h>
#include <assert.h>

using namespace ActiveMQ;
using std::auto_ptr;

RCSID(BlockingMessageConsumer, "$Id$");

BlockingMessageConsumer::BlockingMessageConsumer() {
    pthread_mutex_init(&messages_lock_, NULL);
}

void
BlockingMessageConsumer::enqueue(Message *msg) {
    {
        Lock l(&messages_lock_);
        messages_.push_back(msg);
    }
    ready_.post();
}

unsigned int
BlockingMessageConsumer::getNumReadyMessages() const {
    Lock l(&messages_lock_);
    return messages_.size();
}

auto_ptr<Message>
BlockingMessageConsumer::receive() {
    ready_.wait();
    Message *ret;
    {
        Lock l(&messages_lock_);
        assert(!messages_.empty());
        ret = messages_.front();
        messages_.pop_front();
    }
    return auto_ptr<Message>(ret);
}

BlockingMessageConsumer::~BlockingMessageConsumer() {
    pthread_mutex_destroy(&messages_lock_);
}

void
BlockingMessageConsumer::removeQueued(const Destination &d) {
    Lock l(&messages_lock_);
    messages_.erase(std::remove_if(messages_.begin(),
                                   messages_.end(),
                                   HasDest(d)),
                    messages_.end());
}
