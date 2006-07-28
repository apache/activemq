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
#ifndef ActiveMQ_Dispatcher_hpp_
#define ActiveMQ_Dispatcher_hpp_

#include <string>
#include <list>
#include <queue>
#include "cms/IMessage.hpp"
#include "activemq/command/Response.hpp"
#include "ppr/thread/SimpleMutex.hpp"
#include "ppr/thread/Semaphore.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    using namespace ifr;
    using namespace apache::activemq::command;
    using namespace apache::cms;
    using namespace apache::ppr::thread;

/*
 * Handles the multi-threaded dispatching between transport and consumers.
 */
class Dispatcher
{
private:
    p<queue< p<IMessage> > > dispatchQueue ;
    p<list< p<IMessage> > >  redeliverList ;
    SimpleMutex              mutex ;
    Semaphore                semaphore ;

public:
    Dispatcher() ;
    virtual ~Dispatcher() {}

    virtual void redeliverRolledBackMessages() ;
    virtual void redeliver(p<IMessage> message) ;
    virtual void enqueue(p<IMessage> message) ;
    virtual p<IMessage> dequeueNoWait() ;
    virtual p<IMessage> dequeue(int timeout) ;
    virtual p<IMessage> dequeue() ;
} ;

/* namespace */
  }
}

#endif /*ActiveMQ_Dispatcher_hpp_*/
