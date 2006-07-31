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

#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include <sys/poll.h>

#include <memory>
#include <map>
#include <string>
#include <utility>
#include <iostream>

#include "BrokerSessionImpl_.h"

#include "amq_corelib/BlockingMessageConsumer.h"
#include "amq_corelib/NonBlockingMessageConsumer.h"
#include "amq_corelib/BlockingMessageConsumerRef.h"
#include "amq_corelib/NonBlockingMessageConsumerRef.h"
#include "amq_corelib/Buffer.h"
#include "amq_corelib/Lock.h"
#include "amq_corelib/CoreLib.h"
#include "amq_corelib/RCSID.h"

#include "amq_transport/TCPTransport.h"
#include "amq_transport/TransportFactory.h"

using namespace ActiveMQ;
using namespace std;

RCSID(BrokerSessionImpl, "$Id$");

#define READ_BUFFER_SIZE 4096

void *
BrokerSessionImpl::backgroundThreadTask_(void *arg) {
    // If we're passed a bad argument there's nothing we can do -
    // we're in a separate thread and can't get at the brokersession's
    // exception callback
    assert(arg != NULL);

    BrokerSessionImpl *bsi(static_cast<BrokerSessionImpl *>(arg));
    int listenfd = bsi->transport_->getFD();
    unsigned char readbuf[READ_BUFFER_SIZE];

    // The background thread just waits for data in the transport's
    // socket, then hands it to the core library.

    while (1) {
        struct pollfd pfd;
        pfd.fd = listenfd;
        pfd.events = POLLIN;
    
        int rc = poll(&pfd,1,-1);
        if (rc == 1) {
            try {
                int numread = bsi->transport_->recv(readbuf, READ_BUFFER_SIZE);
                if (numread == 0)
                    return NULL;
                Buffer b;
                bsi->corelib_.handleData(readbuf, numread, b);
                if (!b.empty())
                    bsi->transport_->send(b);
            } catch (const Exception& e) {
                (bsi->getExceptionCallback())(e);
                {
                    Lock l(&bsi->brokersessionMutex_);
                    bsi->connected_ = false;
                }
                return NULL;
            }
        }
        else if (rc == -1 && errno != EINTR)
        {
           (bsi->getExceptionCallback())(Exception(errno >= sys_nerr
                                                        ? "Unknown error" :
                                                        sys_errlist[errno]));
            return NULL;
        }
    }
    return NULL;
}


static
void
defaultErrorHandler_(const Exception& msg) {
    cerr << "Exception: " << msg.what() << endl;
}

BrokerSessionImpl::BrokerSessionImpl(const string& uri,
                             const string& user,
                             const string& password)
  : corelib_(user, password),
    transport_(TransportFactory::instance().getFromURI(uri)),
    connected_(false),
    exceptionCB_(defaultErrorHandler_)
{
    pthread_mutex_init(&brokersessionMutex_,NULL);
}

void
BrokerSessionImpl::disconnect() {
    Lock l(&brokersessionMutex_);
    if (connected_) {
        l.unlock();
        connected_ = false;
        Buffer b;
        corelib_.disconnect(b);
        transport_->send(b);
        pthread_join(backgroundThread_, NULL);
    }
}

BrokerSessionImpl::~BrokerSessionImpl() {
    try {
        disconnect();
        pthread_mutex_destroy(&brokersessionMutex_);
    }
    catch (...) {}
}

void
BrokerSessionImpl::connect() {
    Lock l(&brokersessionMutex_);
    if (!connected_) {
        Buffer b;
        transport_->connect();
        corelib_.initialize(b);
        transport_->send(b);
        connected_ = true;

        pthread_create(&backgroundThread_,
                       NULL,
                       BrokerSessionImpl::backgroundThreadTask_, this);
    }
}

bool
BrokerSessionImpl::isConnected() const {
    Lock l(&brokersessionMutex_);
    return connected_;
}

void
BrokerSessionImpl::publish(const Message& m) {
    publish(m.getDestination(), m);
}

void
BrokerSessionImpl::publish(const Destination &dest,
                       const Message& m) {
    Lock l(&brokersessionMutex_);
    if (!connected_)
        throw Exception("BrokerSession is not connected");
    Buffer b;
    corelib_.publish(dest, m, b);
    transport_->send(b);
}

void
BrokerSessionImpl::subscribe(const Destination& dest,
                         MessageConsumerRef& q) {
    Lock l(&brokersessionMutex_);
    if (!connected_)
        throw Exception("BrokerSession is not connected");
    Buffer b;
    corelib_.subscribe(dest, q, b);
    transport_->send(b);
}

void
BrokerSessionImpl::unsubscribe(const Destination& dest) {
    Lock l(&brokersessionMutex_);
    if (!connected_)
        throw Exception("BrokerSession is not connected");
    Buffer b;
    corelib_.unsubscribe(dest, b);
    transport_->send(b);
}

BlockingMessageConsumerRef
BrokerSessionImpl::newBlockingMessageConsumer() {
    Lock l(&brokersessionMutex_);
    return corelib_.newBlockingMessageConsumer();
}

NonBlockingMessageConsumerRef
BrokerSessionImpl::newNonBlockingMessageConsumer() {
    Lock l(&brokersessionMutex_);
    return corelib_.newNonBlockingMessageConsumer();
}

ExceptionCallback
BrokerSessionImpl::setExceptionCallback(ExceptionCallback c) {
    Lock l(&brokersessionMutex_);
    ExceptionCallback old;
    old = exceptionCB_;
    exceptionCB_ = c;
    return old;
}

ExceptionCallback&
BrokerSessionImpl::getExceptionCallback() {
    Lock l(&brokersessionMutex_);
    return exceptionCB_;
}

void
BrokerSessionImpl::setLogger(auto_ptr<Logger> lgr) {
    Lock l(&brokersessionMutex_);
    corelib_.setLogger(lgr);
}

void
BrokerSessionImpl::fatal_(const string& msg) {
    Lock l(&brokersessionMutex_);
    corelib_.getLogger().logFatal(msg);
}

void
BrokerSessionImpl::error_(const string& msg) {
    Lock l(&brokersessionMutex_);
    corelib_.getLogger().logError(msg);
}

void
BrokerSessionImpl::warning_(const string& msg) {
    Lock l(&brokersessionMutex_);
    corelib_.getLogger().logWarning(msg);
}

void
BrokerSessionImpl::inform_(const string& msg) {
    Lock l(&brokersessionMutex_);
    corelib_.getLogger().logInform(msg);
}

void
BrokerSessionImpl::debug_(const string& msg) {
    Lock l(&brokersessionMutex_);
    corelib_.getLogger().logDebug(msg);
}


Destination
BrokerSessionImpl::createTopic(const std::string& name) {
    return corelib_.createTopic(name);
}

Destination
BrokerSessionImpl::createQueue(const std::string& name) {
    return corelib_.createQueue(name);
}

Destination
BrokerSessionImpl::createTemporaryTopic() {
    return corelib_.createTemporaryTopic();
}

Destination
BrokerSessionImpl::createTemporaryQueue() {
    return corelib_.createTemporaryQueue();
}
