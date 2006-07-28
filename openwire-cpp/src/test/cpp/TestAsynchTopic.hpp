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
#ifndef TestAsynchTopic_hpp_
#define TestAsynchTopic_hpp_

#include <exception>
#include <string>

#include "cms/IConnection.hpp"
#include "ppr/thread/Semaphore.hpp"
#include "ppr/util/ifr/p"

#include "ppr/TraceException.hpp"
#include "IUnitTest.hpp"

using namespace apache::cms;
using namespace apache::ppr;
using namespace apache::ppr::thread;
using namespace ifr;
using namespace std;

/*
 * Tests sending/receiving a message to two asynchronous listeners.
 */
class TestAsynchTopic : public IUnitTest, public IMessageListener
{
private:
    p<IConnection> connection ;
    p<ISession>    session ;
    Semaphore      semaphore ; 
    char*          error ;

public:
    TestAsynchTopic(p<IConnection> connection) ;
    virtual ~TestAsynchTopic() ;

    virtual void setUp() throw (exception) ;
    virtual void execute() throw (exception) ;
    virtual void tearDown() throw (exception) ;
    virtual p<string> toString() ;

    virtual void onMessage(p<IMessage> message) ;
} ;

#endif /*TestAsynchTopic_hpp_*/
