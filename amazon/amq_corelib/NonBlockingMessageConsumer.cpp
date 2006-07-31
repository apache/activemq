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

#include "NonBlockingMessageConsumer.h"
#include "RCSID.h"
#include "Message.h"

#include <algorithm>

#include <unistd.h>

using namespace ActiveMQ;
using std::auto_ptr;

RCSID(NonBlockingMessageConsumer, "$Id$")

NonBlockingMessageConsumer::NonBlockingMessageConsumer() {
    pipe(eventpipe_);
}

NonBlockingMessageConsumer::~NonBlockingMessageConsumer() {
    close(eventpipe_[0]);
    close(eventpipe_[1]);
}

void
NonBlockingMessageConsumer::enqueue(Message *msg) {
    messages_.push_back(msg);
    if (messages_.size() == 1)
        write(eventpipe_[1], "0", 1);
}

auto_ptr<Message>
NonBlockingMessageConsumer::receive() {
    if (messages_.empty())
        return auto_ptr<Message>(NULL);
    Message *ret = messages_.front();
    messages_.pop_front();
    if (messages_.empty()) {
        unsigned char buf;
        read(eventpipe_[0], &buf, 1);
    }
    return auto_ptr<Message>(ret);
}

unsigned int 
NonBlockingMessageConsumer::getNumReadyMessages() const {
    return messages_.size();
}

void
NonBlockingMessageConsumer::removeQueued(const Destination &d) {
    messages_.erase(std::remove_if(messages_.begin(),
                                   messages_.end(),
                                   HasDest(d)),
                    messages_.end());
}
