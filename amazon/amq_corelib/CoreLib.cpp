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

#include <string>
#include <memory>

#include "MessageConsumer.h"
#include "NonBlockingMessageConsumerRef.h"
#include "BlockingMessageConsumerRef.h"
#include "RCSID.h"

#include "CoreLib.h"
#include "CoreLibImpl_.h"

using namespace ActiveMQ;
using std::string;
using std::auto_ptr;

RCSID(CoreLib, "$Id$");

CoreLib::CoreLib(const string& user,
                 const string& password) :
    pimpl_(new CoreLibImpl(this,user,password))
  {}

void
CoreLib::initialize(Buffer& b) {
    pimpl_->initialize(b);
}

void
CoreLib::publish(const Message& m, Buffer& b) {
    pimpl_->publish(m,b);
}

void
CoreLib::publish(const Destination& d,
                 const Message& m,
                 Buffer& b) {
    pimpl_->publish(d,m,b);
}

void
CoreLib::disconnect(Buffer& b) {
    return pimpl_->disconnect(b);
}

void
CoreLib::subscribe(const Destination& d, MessageConsumerRef& q, Buffer& b) {
    return pimpl_->subscribe(d,q,b);
}

void
CoreLib::unsubscribe(const Destination& d, Buffer& b) {
    return pimpl_->unsubscribe(d,b);
}

void
CoreLib::handleData(const Buffer& incoming, Buffer& b) {
    pimpl_->handleData(incoming, b);
}

void
CoreLib::handleData(const unsigned char *buf, size_t len, Buffer& b) {
    pimpl_->handleData(buf,len, b);
}

BlockingMessageConsumerRef
CoreLib::newBlockingMessageConsumer(void) {
    return pimpl_->newBlockingMessageConsumer();
}

NonBlockingMessageConsumerRef
CoreLib::newNonBlockingMessageConsumer(void) {
    return pimpl_->newNonBlockingMessageConsumer();
}

void
CoreLib::registerRef(MessageConsumerRef *q) {
    pimpl_->registerRef(q);
}

void
CoreLib::deregisterRef(MessageConsumerRef *q) {
    pimpl_->deregisterRef(q);
}

void
CoreLib::setLogger(auto_ptr<Logger> lgr) {
    pimpl_->setLogger(lgr);
}

void
CoreLib::setLogger(Logger *c) {
    setLogger(auto_ptr<Logger>(c));
}

Logger&
CoreLib::getLogger() {
    return pimpl_->getLogger();
}

Destination
CoreLib::createTemporaryTopic() {
    return pimpl_->createTemporaryTopic();
}

Destination
CoreLib::createTemporaryQueue() {
    return pimpl_->createTemporaryQueue();
}

Destination
CoreLib::createTopic(const string& name) {
    return pimpl_->createTopic(name);
}

Destination
CoreLib::createQueue(const string& name) {
    return pimpl_->createQueue(name);
}

void
CoreLib::registerDest(const Destination& d) {
    pimpl_->registerDest(d);
}

void
CoreLib::unregisterDest(const Destination& d) {
    pimpl_->unregisterDest(d);
}
